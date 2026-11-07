// demo-app: 이전 대상이 되는 데모 채팅 앱.
//
// 메시지마다 "어느 환경의 앱이 처리했고 어느 DB에 저장했는지"를 함께 기록해서,
// 이전이 진행되는 모습이 채팅 기록 자체에 남도록 한다.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"
)

//go:embed static
var staticFS embed.FS

type server struct {
	envName  string
	store    *Store
	hub      *Hub
	drain    context.Context // 종료 신호를 받으면 취소된다. SSE 연결을 닫아 다른 인스턴스로 재접속시킨다
	draining atomic.Bool
}

func main() {
	envName := getenv("ENV_NAME", "local")
	addr := getenv("ADDR", ":8080")
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	pollMS, _ := strconv.Atoi(getenv("POLL_INTERVAL_MS", "150"))
	drainDelay, err := time.ParseDuration(getenv("DRAIN_DELAY", "3s"))
	if err != nil {
		log.Fatalf("DRAIN_DELAY: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := newStore(ctx, dbURL)
	if err != nil {
		log.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		log.Fatal(err)
	}
	hub := newHub(store, time.Duration(pollMS)*time.Millisecond)
	hubCtx, stopHub := context.WithCancel(context.Background())
	go hub.Run(hubCtx)

	s := &server{envName: envName, store: store, hub: hub, drain: ctx}
	fault := newFaults(os.Getenv("FAULT_TOKEN"))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("POST /api/messages", s.postMessage)
	mux.HandleFunc("GET /api/messages", s.listMessages)
	mux.HandleFunc("GET /api/search", s.searchMessages)
	mux.HandleFunc("GET /api/stream", s.stream)
	mux.HandleFunc("POST /api/verify", s.verify)
	mux.HandleFunc("GET /api/stats", s.stats)
	for _, method := range []string{"GET", "POST", "DELETE"} {
		mux.HandleFunc(method+" "+faultPath, fault.handle)
	}
	static, _ := fs.Sub(staticFS, "static")
	mux.Handle("GET /", http.FileServerFS(static))

	srv := &http.Server{
		Addr:              addr,
		Handler:           s.withServedBy(fault.middleware(mux)),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		// 1) 헬스 체크를 실패시켜 로드 밸런서가 새 요청을 보내지 않게 하고
		// 2) SSE 연결은 drain 컨텍스트로 이미 닫히며 (브라우저가 다른 인스턴스로 재접속)
		// 3) 진행 중인 POST가 끝날 때까지 기다린 뒤 종료한다
		s.draining.Store(true)
		log.Printf("draining for %s", drainDelay)
		time.Sleep(drainDelay) // 로드 밸런서가 헬스 체크 실패를 알아챌 시간
		sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil {
			log.Printf("shutdown: %v", err)
		}
		stopHub()
	}()

	log.Printf("demo-app env=%s listening on %s", envName, addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	log.Printf("bye")
}

func (s *server) withServedBy(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Served-By", s.envName)
		next.ServeHTTP(w, r)
	})
}

// healthz는 DB를 확인하지 않는다. DB 전환 중 PgBouncer PAUSE 동안 DB 조회가 멈추는데,
// 그때 헬스 체크까지 실패하면 로드 밸런서가 모든 인스턴스를 내려 버린다.
func (s *server) healthz(w http.ResponseWriter, r *http.Request) {
	status := http.StatusOK
	state := "ok"
	if s.draining.Load() {
		status, state = http.StatusServiceUnavailable, "draining"
	}
	writeJSON(w, status, map[string]string{"status": state, "app_env": s.envName, "db_node": s.hub.Node()})
}

type postReq struct {
	ClientMsgID string `json:"client_msg_id"`
	Author      string `json:"author"`
	Body        string `json:"body"`
	Translation string `json:"translation"`
}

func (s *server) postMessage(w http.ResponseWriter, r *http.Request) {
	var req postReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid json")
		return
	}
	req.Author = strings.TrimSpace(req.Author)
	req.Body = strings.TrimSpace(req.Body)
	req.Translation = strings.TrimSpace(req.Translation)
	switch {
	case len(req.ClientMsgID) < 8 || len(req.ClientMsgID) > 64:
		httpError(w, http.StatusBadRequest, "client_msg_id must be 8-64 chars")
		return
	case req.Author == "" || utf8.RuneCountInString(req.Author) > 40:
		httpError(w, http.StatusBadRequest, "author must be 1-40 chars")
		return
	case req.Body == "" || utf8.RuneCountInString(req.Body) > 500:
		httpError(w, http.StatusBadRequest, "body must be 1-500 chars")
		return
	case utf8.RuneCountInString(req.Translation) > 1000:
		httpError(w, http.StatusBadRequest, "translation must be at most 1000 chars")
		return
	}
	var translation *string
	if req.Translation != "" {
		translation = &req.Translation
	}

	// DB 전환 중에는 PgBouncer가 쿼리를 잠시 대기시키므로 넉넉하게 기다린다
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	m, created, err := s.store.Insert(ctx, req.ClientMsgID, req.Author, req.Body, translation, s.envName)
	if err != nil {
		log.Printf("insert: %v", err)
		httpError(w, http.StatusServiceUnavailable, "temporarily unavailable, retry with the same client_msg_id")
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, m)
}

func (s *server) listMessages(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	msgs, err := s.store.After(r.Context(), after, limit)
	if err != nil {
		httpError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, msgs)
}

// searchMessages는 작성자나 본문 일부로 메시지를 찾는다. 두 조건 모두 선택이며,
// sort로 정렬 기준(author, created_at)을 바꿀 수 있다.
func (s *server) searchMessages(w http.ResponseWriter, r *http.Request) {
	author := r.URL.Query().Get("author")
	q := r.URL.Query().Get("q")
	sort := r.URL.Query().Get("sort")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	msgs, err := s.store.Search(r.Context(), author, q, sort, limit)
	if err != nil {
		httpError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, msgs)
}

// stream은 SSE로 메시지를 보낸다. 브라우저의 EventSource는 연결이 끊기면 자동으로 재접속하면서
// 마지막으로 받은 id를 Last-Event-ID 헤더로 보내므로, 그 이후부터 이어서 보내면 빠지는 구간이 없다.
func (s *server) stream(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")

	if s.draining.Load() {
		// 503을 받은 로드 밸런서는 다른 인스턴스로 다시 보낸다 (haproxy.cfg의 retry-on)
		httpError(w, http.StatusServiceUnavailable, "draining")
		return
	}
	lastID, resume := parseLastID(r)

	// 백로그를 조회하기 전에 구독해야 그 사이에 들어온 메시지를 놓치지 않는다. 중복은 id로 거른다
	ch := s.hub.Subscribe()
	defer s.hub.Unsubscribe(ch)

	var backlog []Message
	var err error
	if resume {
		backlog, err = s.store.After(r.Context(), lastID, 1000)
	} else {
		backlog, err = s.store.Recent(r.Context(), 50)
	}
	if err != nil {
		httpError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	fmt.Fprint(w, "retry: 1000\n\n")
	s.writeStatus(w, "hello")
	for _, m := range backlog {
		writeMsg(w, m)
		lastID = m.ID
	}
	if err := rc.Flush(); err != nil {
		return
	}

	status := time.NewTicker(2 * time.Second)
	defer status.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.drain.Done():
			// 이 인스턴스가 내려간다. 브라우저는 retry 간격 뒤에 로드 밸런서를 거쳐 다른 인스턴스로 재접속한다
			s.writeStatus(w, "bye")
			_ = rc.Flush()
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			if m.ID <= lastID {
				continue
			}
			writeMsg(w, m)
			lastID = m.ID
		case <-status.C:
			// 상태 이벤트는 연결 유지(heartbeat) 역할도 한다
			s.writeStatus(w, "status")
		}
		if err := rc.Flush(); err != nil {
			return
		}
	}
}

func parseLastID(r *http.Request) (int64, bool) {
	v := r.Header.Get("Last-Event-ID")
	if v == "" {
		v = r.URL.Query().Get("after")
	}
	if v == "" {
		return 0, false
	}
	id, err := strconv.ParseInt(v, 10, 64)
	return id, err == nil
}

func writeMsg(w http.ResponseWriter, m Message) {
	b, _ := json.Marshal(m)
	fmt.Fprintf(w, "id: %d\nevent: message\ndata: %s\n\n", m.ID, b)
}

func (s *server) writeStatus(w http.ResponseWriter, event string) {
	b, _ := json.Marshal(map[string]string{"app_env": s.envName, "db_node": s.hub.Node()})
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
}

type verifyReq struct {
	ClientMsgIDs []string `json:"client_msg_ids"`
}

// verify는 클라이언트가 보냈다고 기록한 메시지 중 DB에 없는 것을 돌려준다 (유실 검증).
func (s *server) verify(w http.ResponseWriter, r *http.Request) {
	var req verifyReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if len(req.ClientMsgIDs) > 10000 {
		httpError(w, http.StatusBadRequest, "too many ids")
		return
	}
	missing, err := s.store.Missing(r.Context(), req.ClientMsgIDs)
	if err != nil {
		httpError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"checked": len(req.ClientMsgIDs), "missing": missing})
}

// stats는 전체 통계를 돌려준다. env 쿼리 파라미터를 주면 그 앱 환경으로만 좁힌다.
func (s *server) stats(w http.ResponseWriter, r *http.Request) {
	env := r.URL.Query().Get("env")
	st, err := s.store.StatsByEnv(r.Context(), env)
	if err != nil {
		httpError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
