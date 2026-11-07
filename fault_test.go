package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func testFaults(token string, roll float64) (*faults, *fakeClock, *[]time.Duration) {
	clock := &fakeClock{t: time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC)}
	slept := &[]time.Duration{}
	f := newFaults(token)
	f.now = clock.now
	f.roll = func() float64 { return roll }
	f.sleep = func(d time.Duration) { *slept = append(*slept, d) }
	return f, clock, slept
}

func call(h http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set(faultTokenHeader, token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func app(f *faults) http.Handler {
	mux := http.NewServeMux()
	ok := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }
	mux.HandleFunc("GET /healthz", ok)
	mux.HandleFunc("POST /api/messages", ok)
	mux.HandleFunc("GET /api/stream", ok)
	mux.HandleFunc("GET /", ok) // main.go처럼 정적 파일 경로가 있어도 충돌하지 않아야 한다
	for _, method := range []string{"GET", "POST", "DELETE"} {
		mux.HandleFunc(method+" "+faultPath, f.handle)
	}
	return f.middleware(mux)
}

func TestSwitchDoesNotExistWithoutToken(t *testing.T) {
	f, _, _ := testFaults("", 0)
	if rec := call(app(f), "POST", faultPath, "anything", `{"error_rate":1}`); rec.Code != 404 {
		t.Fatalf("want 404, got %d", rec.Code)
	}
}

func TestSwitchNeedsTheRightToken(t *testing.T) {
	f, _, _ := testFaults("secret", 0)
	for _, token := range []string{"", "wrong"} {
		if rec := call(app(f), "POST", faultPath, token, `{"error_rate":1}`); rec.Code != 403 {
			t.Fatalf("token %q: want 403, got %d", token, rec.Code)
		}
	}
}

func TestFaultFailsApiButKeepsHealthAndStream(t *testing.T) {
	f, _, slept := testFaults("secret", 0.1)
	h := app(f)
	if rec := call(h, "POST", faultPath, "secret", `{"error_rate":0.5,"delay_ms":800}`); rec.Code != 200 {
		t.Fatalf("turn on: %d %s", rec.Code, rec.Body)
	}
	if rec := call(h, "POST", "/api/messages", "", `{}`); rec.Code != 500 {
		t.Fatalf("api: want 500, got %d", rec.Code)
	}
	if len(*slept) != 1 || (*slept)[0] != 800*time.Millisecond {
		t.Fatalf("want one 800ms delay, got %v", *slept)
	}
	if rec := call(h, "GET", "/healthz", "", ""); rec.Code != 200 {
		t.Fatalf("healthz must stay 200, got %d", rec.Code)
	}
	if rec := call(h, "GET", "/api/stream", "", ""); rec.Code != 200 {
		t.Fatalf("stream must not be faulted, got %d", rec.Code)
	}
}

func TestOnlyTheConfiguredShareFails(t *testing.T) {
	f, _, _ := testFaults("secret", 0.7) // 굴린 값이 비율보다 크면 통과
	h := app(f)
	call(h, "POST", faultPath, "secret", `{"error_rate":0.5}`)
	if rec := call(h, "POST", "/api/messages", "", `{}`); rec.Code != 200 {
		t.Fatalf("want 200, got %d", rec.Code)
	}
}

func TestFaultTurnsItselfOffAfterItsDuration(t *testing.T) {
	f, clock, _ := testFaults("secret", 0)
	h := app(f)
	call(h, "POST", faultPath, "secret", `{"error_rate":1,"duration_s":30}`)
	clock.t = clock.t.Add(29 * time.Second)
	if rec := call(h, "POST", "/api/messages", "", `{}`); rec.Code != 500 {
		t.Fatalf("before expiry: want 500, got %d", rec.Code)
	}
	clock.t = clock.t.Add(2 * time.Second)
	if rec := call(h, "POST", "/api/messages", "", `{}`); rec.Code != 200 {
		t.Fatalf("after expiry: want 200, got %d", rec.Code)
	}
}

func TestDeleteClearsAndGetReportsState(t *testing.T) {
	f, _, _ := testFaults("secret", 0)
	h := app(f)
	call(h, "POST", faultPath, "secret", `{"error_rate":1}`)
	var got struct{ Active bool }
	_ = json.Unmarshal(call(h, "GET", faultPath, "secret", "").Body.Bytes(), &got)
	if !got.Active {
		t.Fatal("want active")
	}
	call(h, "DELETE", faultPath, "secret", "")
	if rec := call(h, "POST", "/api/messages", "", `{}`); rec.Code != 200 {
		t.Fatalf("after clear: want 200, got %d", rec.Code)
	}
}

func TestInvalidSettingsAreRejected(t *testing.T) {
	f, _, _ := testFaults("secret", 0)
	for _, body := range []string{
		`{"error_rate":1.5}`, `{"error_rate":-0.1}`, `{"delay_ms":20000}`,
		`{"error_rate":1,"duration_s":3600}`, `{}`, `not json`,
	} {
		if rec := call(app(f), "POST", faultPath, "secret", body); rec.Code != 400 {
			t.Fatalf("%s: want 400, got %d", body, rec.Code)
		}
	}
}
