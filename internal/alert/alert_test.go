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
