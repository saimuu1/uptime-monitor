// Package alert delivers DOWN/RECOVERED notifications. v3 ships a webhook
// notifier (Discord/Slack) and a no-op used when no webhook is configured.
package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
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
	Slow         Kind = "SLOW"
)

// Event is a notification about a monitor's state change.
type Event struct {
	Monitor  string
	URL      string // the monitored URL (shown in the email so it's clear which site)
	Kind     Kind
	Region   string
	Cause    string
	At       time.Time
	Duration time.Duration // for Recovered: how long the site was down (0 if unknown)
	To       []string      // recipient emails (used by the Email notifier)
}

// Subject renders the email subject line: a short tag plus, at a glance, exactly
// what's wrong. Kept ASCII so it needs no MIME encoding.
func (e Event) Subject() string {
	switch e.Kind {
	case Down:
		return "[DOWN] " + e.Monitor + dash(e.Cause)
	case Recovered:
		s := "[RESOLVED] " + e.Monitor + " is back up"
		if e.Duration > 0 {
			s += " after " + humanizeDuration(e.Duration)
		}
		return s
	case CertExpiring:
		return "[SSL] " + e.Monitor + dash(e.Cause)
	case Slow:
		return "[SLOW] " + e.Monitor + dash(e.Cause)
	default:
		return fmt.Sprintf("[%s] %s", e.Kind, e.Monitor)
	}
}

// headline is the one-line summary shown at the top of the email body.
func (e Event) headline() string {
	switch e.Kind {
	case Down:
		return e.Monitor + " is down"
	case Recovered:
		if e.Duration > 0 {
			return e.Monitor + " is back up — down for " + humanizeDuration(e.Duration)
		}
		return e.Monitor + " is back up"
	case CertExpiring:
		return e.Monitor + ": SSL certificate expiring"
	case Slow:
		return e.Monitor + " is responding slowly"
	default:
		return e.Monitor
	}
}

// rows are the labelled facts about the event, shared by the plain-text and HTML
// bodies so they never drift apart.
func (e Event) rows() [][2]string {
	rows := [][2]string{{"Monitor", e.Monitor}}
	if e.URL != "" {
		rows = append(rows, [2]string{"URL", e.URL})
	}
	switch e.Kind {
	case Down:
		rows = append(rows, [2]string{"Status", "DOWN"})
		rows = append(rows, [2]string{"Detected", when(e.At)})
		if e.Cause != "" {
			rows = append(rows, [2]string{"Reason", e.Cause})
		}
		if e.Region != "" {
			rows = append(rows, [2]string{"Confirmed by", "multiple regions (incl. " + e.Region + ")"})
		}
	case Recovered:
		rows = append(rows, [2]string{"Status", "Back up"})
		rows = append(rows, [2]string{"Recovered", when(e.At)})
		if e.Duration > 0 {
			rows = append(rows, [2]string{"Downtime", humanizeDuration(e.Duration)})
		}
		if e.Region != "" {
			rows = append(rows, [2]string{"Confirmed by", "region " + e.Region})
		}
	case CertExpiring:
		rows = append(rows, [2]string{"Alert", "SSL certificate expiring"})
		if e.Cause != "" {
			rows = append(rows, [2]string{"Detail", e.Cause})
		}
		rows = append(rows, [2]string{"Noticed", when(e.At)})
	case Slow:
		rows = append(rows, [2]string{"Alert", "Slow responses"})
		if e.Cause != "" {
			rows = append(rows, [2]string{"Detail", e.Cause})
		}
		rows = append(rows, [2]string{"Noticed", when(e.At)})
	}
	return rows
}

// accent is the header colour of the HTML email, by severity.
func (e Event) accent() string {
	switch e.Kind {
	case Recovered:
		return "#17915a" // green
	case Down:
		return "#d63b3b" // red
	default:
		return "#b26a00" // amber (cert / slow)
	}
}

// Message renders the human-readable line sent to the webhook.
func (e Event) Message() string {
	switch e.Kind {
	case Down:
		return fmt.Sprintf("🔴 DOWN — %s (agreed by regions incl. %s): %s", e.Monitor, e.Region, e.Cause)
	case CertExpiring:
		return fmt.Sprintf("⚠️ SSL CERT EXPIRING — %s: %s", e.Monitor, e.Cause)
	case Slow:
		return fmt.Sprintf("🐢 SLOW — %s: %s", e.Monitor, e.Cause)
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

// Send emails the event to its recipients as a multipart message: a clean,
// labelled plain-text body plus an HTML card. No recipients = nothing to do.
func (m Email) Send(_ context.Context, ev Event) error {
	if len(ev.To) == 0 {
		return nil
	}
	auth := smtp.PlainAuth("", m.User, m.Pass, m.Host)
	addr := m.Host + ":" + m.Port
	msg := buildMIME(m.From, ev.To, ev.Subject(), ev.emailPlain(), ev.emailHTML())
	return smtp.SendMail(addr, auth, m.From, ev.To, msg)
}

// emailPlain renders the labelled, aligned plain-text body.
func (e Event) emailPlain() string {
	rows := e.rows()
	width := 0
	for _, r := range rows {
		if len(r[0]) > width {
			width = len(r[0])
		}
	}
	var b strings.Builder
	b.WriteString(e.headline())
	b.WriteString("\n\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "  %-*s  %s\n", width+1, r[0]+":", r[1])
	}
	b.WriteString("\n— Uptime Monitor\n")
	return b.String()
}

// emailHTML renders an HTML card: a coloured header plus a table of the facts.
// Uses inline styles and a table layout for broad email-client compatibility.
func (e Event) emailHTML() string {
	accent := e.accent()
	var rowsHTML strings.Builder
	for _, r := range e.rows() {
		val := html.EscapeString(r[1])
		if r[0] == "URL" {
			val = fmt.Sprintf("<a href=%q style=\"color:%s;text-decoration:none;\">%s</a>", r[1], accent, val)
		}
		fmt.Fprintf(&rowsHTML,
			"<tr><td style=\"padding:9px 16px;color:#6b7280;font-size:13px;white-space:nowrap;vertical-align:top;border-top:1px solid #eef0f2;\">%s</td>"+
				"<td style=\"padding:9px 16px;color:#14171a;font-size:14px;font-weight:500;border-top:1px solid #eef0f2;\">%s</td></tr>",
			html.EscapeString(r[0]), val)
	}
	return fmt.Sprintf(`<!doctype html><html><body style="margin:0;background:#f6f7f9;padding:24px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
<div style="max-width:520px;margin:0 auto;border-radius:12px;overflow:hidden;border:1px solid #e6e8eb;">
<div style="background:%s;color:#ffffff;padding:18px 20px;font-size:18px;font-weight:650;">%s</div>
<table style="width:100%%;border-collapse:collapse;background:#ffffff;">%s</table>
<div style="background:#ffffff;padding:14px 20px;color:#9aa2ad;font-size:12px;">Uptime Monitor · you're receiving this because you monitor this site.</div>
</div></body></html>`, accent, html.EscapeString(e.headline()), rowsHTML.String())
}

// SendMessage sends a plain-text email — used for alerts and for transactional
// mail like password resets.
func (m Email) SendMessage(_ context.Context, to []string, subject, body string) error {
	if len(to) == 0 {
		return nil
	}
	auth := smtp.PlainAuth("", m.User, m.Pass, m.Host)
	addr := m.Host + ":" + m.Port
	return smtp.SendMail(addr, auth, m.From, to, buildMessage(m.From, to, subject, body))
}

// buildMessage renders a plain-text RFC 5322 message (headers + body). Used for
// transactional mail like password resets.
func buildMessage(from string, to []string, subject, body string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", strings.Join(to, ", "))
	fmt.Fprintf(&b, "Subject: %s\r\n", subject)
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	b.WriteString("\r\n")
	b.WriteString(body)
	b.WriteString("\r\n")
	return b.Bytes()
}

// buildMIME renders a multipart/alternative message carrying both a plain-text
// and an HTML body, so every mail client shows a clean version.
func buildMIME(from string, to []string, subject, plain, htmlBody string) []byte {
	const boundary = "uptimemon-boundary-a1b2c3d4"
	var b bytes.Buffer
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", strings.Join(to, ", "))
	fmt.Fprintf(&b, "Subject: %s\r\n", subject)
	b.WriteString("MIME-Version: 1.0\r\n")
	fmt.Fprintf(&b, "Content-Type: multipart/alternative; boundary=%q\r\n\r\n", boundary)

	fmt.Fprintf(&b, "--%s\r\n", boundary)
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n\r\n")
	b.WriteString(plain)
	b.WriteString("\r\n")

	fmt.Fprintf(&b, "--%s\r\n", boundary)
	b.WriteString("Content-Type: text/html; charset=utf-8\r\n\r\n")
	b.WriteString(htmlBody)
	b.WriteString("\r\n")

	fmt.Fprintf(&b, "--%s--\r\n", boundary)
	return b.Bytes()
}

// dash prefixes a non-empty string with " - " (for subject lines).
func dash(s string) string {
	if s == "" {
		return ""
	}
	return " - " + s
}

// when formats an event time for humans; a zero time reads as "just now".
func when(t time.Time) string {
	if t.IsZero() {
		return "just now"
	}
	return t.Format("Mon Jan 2, 2006 at 3:04 PM MST")
}

// humanizeDuration renders a downtime span like "8 minutes" or "1 hour 5 minutes".
func humanizeDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		s := int(d.Seconds())
		if s < 1 {
			s = 1
		}
		return plural(s, "second")
	case d < time.Hour:
		return plural(int(d.Minutes()), "minute")
	default:
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		if m == 0 {
			return plural(h, "hour")
		}
		return plural(h, "hour") + " " + plural(m, "minute")
	}
}

func plural(n int, unit string) string {
	if n == 1 {
		return "1 " + unit
	}
	return fmt.Sprintf("%d %ss", n, unit)
}
