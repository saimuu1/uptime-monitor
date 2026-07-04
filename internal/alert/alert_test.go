package alert

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRecipients(t *testing.T) {
	tests := []struct {
		name     string
		emails   []string
		fallback string
		want     []string
	}{
		{"per-monitor wins", []string{"a@x.com"}, "me@x.com", []string{"a@x.com"}},
		{"fallback when empty", nil, "me@x.com", []string{"me@x.com"}},
		{"none when both empty", nil, "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Recipients(tt.emails, tt.fallback)
			if len(got) != len(tt.want) || (len(got) > 0 && got[0] != tt.want[0]) {
				t.Errorf("Recipients(%v, %q) = %v, want %v", tt.emails, tt.fallback, got, tt.want)
			}
		})
	}
}

func TestWebhookSend(t *testing.T) {
	var gotBody map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	err := NewWebhook(srv.URL).Send(context.Background(), Event{
		Monitor: "My API",
		Kind:    Down,
		Region:  "east",
		Cause:   "connection refused",
		At:      time.Now(),
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	// Both Discord (content) and Slack (text) fields present, with the message.
	if !strings.Contains(gotBody["content"], "My API") || !strings.Contains(gotBody["content"], "DOWN") {
		t.Errorf("content missing details: %q", gotBody["content"])
	}
	if gotBody["text"] != gotBody["content"] {
		t.Errorf("text/content mismatch: %q vs %q", gotBody["text"], gotBody["content"])
	}
}

func TestWebhookNon2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	if err := NewWebhook(srv.URL).Send(context.Background(), Event{Monitor: "x", Kind: Down}); err == nil {
		t.Error("expected error on 500 response")
	}
}
