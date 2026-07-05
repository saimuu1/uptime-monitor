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
	"net/smtp"
	"strings"
	"time"
)

// Kind is the notification type.
type Kind string

const (
	Down         Kind = "DOWN"
	Recovered    Kind = "RECOVERED"
	CertExpiring Kind = "CERT EXPIRING"
)

// Event is a notification about a monitor's state change.
type Event struct {
	Monitor string
	Kind    Kind
	Region  string
	Cause   string
	At      time.Time
	To      []string // recipient emails (used by the Email notifier)
}

// Subject renders the email subject line.
func (e Event) Subject() string {
	return fmt.Sprintf("[%s] %s", e.Kind, e.Monitor)
}

// Message renders the human-readable line sent to the webhook.
func (e Event) Message() string {
	switch e.Kind {
	case Down:
		return fmt.Sprintf("🔴 DOWN — %s (agreed by regions incl. %s): %s", e.Monitor, e.Region, e.Cause)
	case CertExpiring:
		return fmt.Sprintf("⚠️ SSL CERT EXPIRING — %s: %s", e.Monitor, e.Cause)
	default:
		return fmt.Sprintf("🟢 RECOVERED — %s (region %s)", e.Monitor, e.Region)
	}
}

// Recipients returns the monitor's own recipient list, or the fallback address
// (e.g. the operator's own inbox) when the monitor has none. An empty fallback
// means "no recipients". This is what lets a solo user set one email in config
// and get alerted about every site without listing it per-monitor.
func Recipients(monitorEmails []string, fallback string) []string {
	if len(monitorEmails) > 0 {
		return monitorEmails
	}
	if fallback != "" {
		return []string{fallback}
	}
	return nil
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

// Email sends alerts over SMTP to each event's recipient list. One sending
// account (the operator's) delivers to any number of per-monitor recipients —
// so people are notified just by having their address on a monitor, no
// per-user accounts or chat apps required.
type Email struct {
	Host string
	Port string
	User string
	Pass string
	From string
}

// NewEmail builds an Email notifier.
func NewEmail(host, port, user, pass, from string) Email {
	return Email{Host: host, Port: port, User: user, Pass: pass, From: from}
}

// Send emails the event to e's recipients. No recipients = nothing to do.
func (m Email) Send(_ context.Context, ev Event) error {
	if len(ev.To) == 0 {
		return nil
	}
	auth := smtp.PlainAuth("", m.User, m.Pass, m.Host)
	addr := m.Host + ":" + m.Port
	return smtp.SendMail(addr, auth, m.From, ev.To, buildMessage(m.From, ev))
}

// buildMessage renders the raw RFC 5322 message (headers + body).
func buildMessage(from string, ev Event) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", strings.Join(ev.To, ", "))
	fmt.Fprintf(&b, "Subject: %s\r\n", ev.Subject())
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	b.WriteString("\r\n")
	b.WriteString(ev.Message())
	b.WriteString("\r\n")
	return b.Bytes()
}
