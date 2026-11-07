package main

// 장애 스위치: 이전 시연에서 "새 환경의 앱이 오류를 낸다"는 상황을 일부러 만든다.
//
// 켜면 /api/ 요청의 일부에 지연과 500을 낸다. /healthz는 그대로 정상이다.
// 헬스 체크는 통과하는데 사용자는 오류를 받는 상황이라, 지표(오류율·p99)로만 알아챌 수 있다.
//
// FAULT_TOKEN이 비어 있으면 스위치 자체가 없다(404). 공개 경로로 켤 수 없도록 토큰이 맞아야 하고,
// 플랫폼 중계 서버(Envoy)는 공개 경로의 /__cmp/를 막는다. 켠 장애는 정한 시간이 지나면 저절로 꺼진다.

import (
	"crypto/subtle"
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	faultPath        = "/__cmp/fault"
	faultDefaultTTL  = 300 * time.Second
	faultMaxTTL      = 900 * time.Second
	faultMaxDelay    = 10 * time.Second
	faultTokenHeader = "X-Fault-Token"
)

type faultState struct {
	ErrorRate float64   `json:"error_rate"` // 0~1, 이 비율의 요청에 500을 낸다
	DelayMS   int       `json:"delay_ms"`   // 모든 대상 요청에 더하는 지연
	Until     time.Time `json:"until"`
}

type faultRequest struct {
	ErrorRate float64 `json:"error_rate"`
	DelayMS   int     `json:"delay_ms"`
	DurationS int     `json:"duration_s"`
}

type faults struct {
	token string
	mu    sync.Mutex
	state faultState
	now   func() time.Time
	roll  func() float64
	sleep func(time.Duration)
}

func newFaults(token string) *faults {
	return &faults{token: token, now: time.Now, roll: rand.Float64, sleep: time.Sleep}
}

// current는 지금 켜져 있는 장애를 돌려준다. 시간이 지났으면 끄고 false를 돌려준다.
func (f *faults) current() (faultState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state.Until.IsZero() {
		return faultState{}, false
	}
	if !f.now().Before(f.state.Until) {
		log.Printf("fault expired")
		f.state = faultState{}
		return faultState{}, false
	}
	return f.state, true
}

// applies는 장애를 적용할 요청인지 본다. 실시간 스트림은 오래 열린 연결이라 제외한다.
func applies(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, "/api/") && r.URL.Path != "/api/stream"
}

func (f *faults) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if applies(r) {
			if st, on := f.current(); on {
				if st.DelayMS > 0 {
					f.sleep(time.Duration(st.DelayMS) * time.Millisecond)
				}
				if st.ErrorRate > 0 && f.roll() < st.ErrorRate {
					httpError(w, http.StatusInternalServerError, "internal error")
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

// handle은 GET(상태), POST(켜기), DELETE(끄기)를 받는다.
func (f *faults) handle(w http.ResponseWriter, r *http.Request) {
	if f.token == "" {
		http.NotFound(w, r)
		return
	}
	given := r.Header.Get(faultTokenHeader)
	if subtle.ConstantTimeCompare([]byte(given), []byte(f.token)) != 1 {
		httpError(w, http.StatusForbidden, "invalid fault token")
		return
	}
	switch r.Method {
	case http.MethodGet:
	case http.MethodDelete:
		f.mu.Lock()
		f.state = faultState{}
		f.mu.Unlock()
		log.Printf("fault cleared")
	case http.MethodPost:
		var req faultRequest
		r.Body = http.MaxBytesReader(w, r.Body, 1024)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpError(w, http.StatusBadRequest, "invalid json")
			return
		}
		ttl := time.Duration(req.DurationS) * time.Second
		if req.DurationS == 0 {
			ttl = faultDefaultTTL
		}
		delay := time.Duration(req.DelayMS) * time.Millisecond
		if req.ErrorRate < 0 || req.ErrorRate > 1 || delay < 0 || delay > faultMaxDelay ||
			ttl <= 0 || ttl > faultMaxTTL || (req.ErrorRate == 0 && req.DelayMS == 0) {
			httpError(w, http.StatusBadRequest,
				"error_rate 0~1, delay_ms 0~10000, duration_s 1~900, and at least one of error_rate or delay_ms")
			return
		}
		f.mu.Lock()
		f.state = faultState{ErrorRate: req.ErrorRate, DelayMS: req.DelayMS, Until: f.now().Add(ttl)}
		f.mu.Unlock()
		log.Printf("fault on: error_rate=%.2f delay_ms=%d for %s", req.ErrorRate, req.DelayMS, ttl)
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		httpError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	st, on := f.current()
	writeJSON(w, http.StatusOK, map[string]any{"active": on, "fault": st})
}
