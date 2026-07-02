package check

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDo(t *testing.T) {
	// Handlers we can point monitors at.
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer fail.Close()

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	// A server we immediately close, so its address refuses connections.
	refused := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	refusedURL := refused.URL
	refused.Close()

	tests := []struct {
		name       string
		url        string
		expected   int
		timeout    time.Duration
		wantUp     bool
		wantStatus int
	}{
		{"200 matches expected", ok.URL, 200, time.Second, true, 200},
		{"500 not expected", fail.URL, 200, time.Second, false, 500},
		{"500 is expected", fail.URL, 500, time.Second, true, 500},
		{"timeout", slow.URL, 200, 10 * time.Millisecond, false, 0},
		{"connection refused", refusedURL, 200, time.Second, false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Do(context.Background(), Monitor{
				URL:            tt.url,
				Method:         "GET",
				Timeout:        tt.timeout,
				ExpectedStatus: tt.expected,
			})
			if got.Up != tt.wantUp {
				t.Errorf("Up = %v, want %v (err=%q)", got.Up, tt.wantUp, got.Err)
			}
			if got.StatusCode != tt.wantStatus {
				t.Errorf("StatusCode = %d, want %d", got.StatusCode, tt.wantStatus)
			}
		})
	}
}
