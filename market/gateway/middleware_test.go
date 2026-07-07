package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Integration tests for middleware ordering: auth before rate limiting, context propagation.
// Bounty: jackjin1997/TentOfTrials#2 — $50

func TestAuthMiddleware_RejectsMissingToken(t *testing.T) {
	h := AuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not run without token")
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAuthMiddleware_AcceptsBearerAndPropagatesContext(t *testing.T) {
	var gotUser, gotSession string
	h := AuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, _ = r.Context().Value(ContextKeyUserID).(string)
		gotSession, _ = r.Context().Value(ContextKeySessionID).(string)
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusOK)
	}
	if gotUser != "user_stub" || gotSession != "session_stub" {
		t.Fatalf("context user=%q session=%q", gotUser, gotSession)
	}
}

func TestRateLimitMiddleware_UsesAPIKeyOverIP(t *testing.T) {
	var hits int
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	})
	chain := RateLimitMiddleware(0.5, 1)(inner)

	req1 := httptest.NewRequest(http.MethodGet, "/", nil)
	req1.Header.Set("X-API-Key", "key-a")
	req1.RemoteAddr = "1.2.3.4:1234"
	rec1 := httptest.NewRecorder()
	chain.ServeHTTP(rec1, req1)

	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("X-API-Key", "key-a")
	req2.RemoteAddr = "9.9.9.9:5678"
	rec2 := httptest.NewRecorder()
	chain.ServeHTTP(rec2, req2)

	if hits != 1 {
		t.Fatalf("same API key should share bucket: hits=%d want 1", hits)
	}
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status=%d want 429", rec2.Code)
	}
}

func TestMiddlewareChain_AuthBeforeRateLimit(t *testing.T) {
	// Unauthenticated requests must fail auth and NOT consume rate-limit budget for the IP.
	rate := RateLimitMiddleware(100, 1)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	chain := AuthMiddleware(rate)

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "10.0.0.1:1111"
		rec := httptest.NewRecorder()
		chain.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("unauth request %d status=%d want 401", i, rec.Code)
		}
	}

	// Authenticated request should still pass rate limit (budget not burned by 401s).
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer ok")
	req.RemoteAddr = "10.0.0.1:1111"
	rec := httptest.NewRecorder()
	chain.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("auth request status=%d want 200", rec.Code)
	}
}

func TestRequestIDMiddleware_GeneratesAndPreserves(t *testing.T) {
	var ctxID string
	h := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctxID, _ = r.Context().Value(ContextKeyRequestID).(string)
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("missing response request id")
	}
	if ctxID != rec.Header().Get("X-Request-ID") {
		t.Fatalf("context id=%q header=%q", ctxID, rec.Header().Get("X-Request-ID"))
	}

	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("X-Request-ID", "client-fixed-id")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Header().Get("X-Request-ID") != "client-fixed-id" {
		t.Fatalf("preserved id=%q", rec2.Header().Get("X-Request-ID"))
	}
}

func TestCORSMiddleware_PreflightAndAllowedOrigin(t *testing.T) {
	h := CORSMiddleware([]string{"https://app.example"}, time.Hour)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)
	req := httptest.NewRequest(http.MethodOptions, "/api", nil)
	req.Header.Set("Origin", "https://app.example")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight status=%d", rec.Code)
	}
	if !strings.Contains(rec.Header().Get("Access-Control-Allow-Origin"), "app.example") {
		t.Fatal("missing CORS allow-origin")
	}
}

func TestRecoveryMiddleware_CatchesPanic(t *testing.T) {
	h := RecoveryMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", rec.Code)
	}
}

func TestTimeoutMiddleware_CancelsContext(t *testing.T) {
	done := make(chan struct{})
	h := TimeoutMiddleware(10*time.Millisecond)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			close(done)
		case <-time.After(50 * time.Millisecond):
			t.Fatal("handler not cancelled")
		}
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("context not cancelled in time")
	}
	_ = context.Background() // ensure context import used
}