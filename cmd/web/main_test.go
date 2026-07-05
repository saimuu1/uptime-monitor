package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBasicAuth(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := basicAuth(ok, "admin", "secret")

	tests := []struct {
		name     string
		path     string
		user     string
		pass     string
		withAuth bool
		want     int
	}{
		{"no creds -> 401", "/", "", "", false, http.StatusUnauthorized},
		{"wrong pass -> 401", "/", "admin", "nope", true, http.StatusUnauthorized},
		{"wrong user -> 401", "/", "root", "secret", true, http.StatusUnauthorized},
		{"right creds -> 200", "/", "admin", "secret", true, http.StatusOK},
		{"metrics exempt", "/metrics", "", "", false, http.StatusOK},
		{"healthz exempt", "/healthz", "", "", false, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.withAuth {
				req.SetBasicAuth(tt.user, tt.pass)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.want {
				t.Errorf("got %d, want %d", rec.Code, tt.want)
			}
		})
	}
}
