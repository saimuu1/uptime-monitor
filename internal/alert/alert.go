// Package alert delivers DOWN/RECOVERED notifications. v3 ships a webhook
// notifier (Discord/Slack) and a no-op used when no webhook is configured.
package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Kind is the notification type.
type Kind string

const (
	Down      Kind = "DOWN"
	Recovered Kind = "RECOVERED"
)

// Event is a notification about a monitor's state change.
type Event struct {
	Monitor string
	Kind    Kind
	Region  string
	Cause   string
	At      time.Time
}

// Message renders the human-readable line sent to the webhook.
func (e Event) Message() string {
	if e.Kind == Down {
		return fmt.Sprintf("🔴 DOWN — %s (agreed by regions incl. %s): %s", e.Monitor, e.Region, e.Cause)
	}
	return fmt.Sprintf("🟢 RECOVERED — %s (region %s)", e.Monitor, e.Region)
}

// Notifier delivers an event somewhere.
type Notifier interface {
	Send(ctx context.Context, e Event) error
}

// Noop drops events (used when no webhook URL is configured).
type Noop struct{}

func (Noop) Send(context.Context, Event) error { return nil }

// Webhook posts events to a Discord/Slack-compatible incoming webhook.
type Webhook struct {
	URL    string
	Client *http.Client
}

// NewWebhook builds a Webhook with a sane timeout.
func NewWebhook(url string) Webhook {
	return Webhook{URL: url, Client: &http.Client{Timeout: 10 * time.Second}}
}

// Send POSTs the event. Discord expects {"content"}, Slack expects {"text"};
// we send both so one payload works with either service.
func (w Webhook) Send(ctx context.Context, e Event) error {
	msg := e.Message()
	body, err := json.Marshal(map[string]string{"content": msg, "text": msg})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}
