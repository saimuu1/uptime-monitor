package alert

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeSMTP starts a minimal in-process SMTP server that captures the message
// body. It advertises AUTH PLAIN and no STARTTLS, so net/smtp will authenticate
// in the clear — allowed because we connect to 127.0.0.1.
func fakeSMTP(t *testing.T) (host, port string, captured <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	got := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)
		reply := func(s string) { w.WriteString(s + "\r\n"); w.Flush() }

		reply("220 fake ESMTP")
		var body strings.Builder
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			cmd := strings.ToUpper(strings.TrimSpace(line))
			switch {
			case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
				reply("250-fake")
				reply("250 AUTH PLAIN")
			case strings.HasPrefix(cmd, "AUTH"):
				reply("235 2.7.0 accepted")
			case strings.HasPrefix(cmd, "MAIL"), strings.HasPrefix(cmd, "RCPT"):
				reply("250 ok")
			case strings.HasPrefix(cmd, "DATA"):
				reply("354 end with .")
				for {
					l, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if l == ".\r\n" {
						break
					}
					body.WriteString(l)
				}
				reply("250 queued")
				got <- body.String()
			case strings.HasPrefix(cmd, "QUIT"):
				reply("221 bye")
				return
			default:
				reply("250 ok")
			}
		}
	}()

	host, port, _ = net.SplitHostPort(ln.Addr().String())
	return host, port, got
}

func TestEmailSend(t *testing.T) {
	host, port, got := fakeSMTP(t)

	err := NewEmail(host, port, "sender@example.com", "app-pass", "sender@example.com").
		Send(context.Background(), Event{
			Monitor: "My API",
			Kind:    Down,
			Region:  "east",
			Cause:   "connection refused",
			To:      []string{"alice@example.com", "bob@example.com"},
		})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case msg := <-got:
		for _, want := range []string{
			"Subject: [DOWN] My API",
			"alice@example.com", "bob@example.com",
			"connection refused",
		} {
			if !strings.Contains(msg, want) {
				t.Errorf("message missing %q\n---\n%s", want, msg)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no message captured")
	}
}

func TestEmailNoRecipientsIsNoop(t *testing.T) {
	// No SMTP server needed: with no recipients, Send must not dial anywhere.
	err := NewEmail("127.0.0.1", "1", "u", "p", "u@x.com").
		Send(context.Background(), Event{Monitor: "x", Kind: Down})
	if err != nil {
		t.Errorf("expected no-op, got %v", err)
	}
}

func TestEmailFormatting(t *testing.T) {
	down := Event{
		Monitor: "My API", URL: "https://api.example.com",
		Kind: Down, Region: "east", Cause: "connection refused",
		At: time.Date(2026, 9, 2, 15, 4, 0, 0, time.UTC),
	}
	if got := down.Subject(); got != "[DOWN] My API - connection refused" {
		t.Errorf("down subject = %q", got)
	}
	plain := down.emailPlain()
	for _, want := range []string{
		"My API is down", "Monitor:", "URL:", "https://api.example.com",
		"Status:", "DOWN", "Reason:", "connection refused",
		"Confirmed by:", "east", "— Uptime Monitor",
	} {
		if !strings.Contains(plain, want) {
			t.Errorf("plain body missing %q\n---\n%s", want, plain)
		}
	}
	htmlBody := down.emailHTML()
	for _, want := range []string{"#d63b3b", "My API is down", `href="https://api.example.com"`} {
		if !strings.Contains(htmlBody, want) {
			t.Errorf("html body missing %q", want)
		}
	}

	// Recovered carries downtime in both the subject and the body.
	rec := Event{Monitor: "My API", Kind: Recovered, Region: "east", Duration: 8 * time.Minute}
	if got := rec.Subject(); got != "[RESOLVED] My API is back up after 8 minutes" {
		t.Errorf("recovered subject = %q", got)
	}
	if p := rec.emailPlain(); !strings.Contains(p, "Downtime:") || !strings.Contains(p, "8 minutes") {
		t.Errorf("recovered plain missing downtime\n%s", p)
	}
	if a := rec.emailHTML(); !strings.Contains(a, "#17915a") {
		t.Error("recovered html should use the green accent")
	}

	// HTML output escapes user-controlled text.
	evil := Event{Monitor: "A<script>", Kind: Down}
	if h := evil.emailHTML(); strings.Contains(h, "<script>") {
		t.Errorf("monitor name not escaped in html:\n%s", h)
	}

	// buildMIME is genuinely multipart with both bodies.
	mime := string(buildMIME("from@x.com", []string{"to@x.com"}, "sub", "PLAIN", "<b>HTML</b>"))
	for _, want := range []string{
		"multipart/alternative", "text/plain; charset=utf-8",
		"text/html; charset=utf-8", "PLAIN", "<b>HTML</b>",
	} {
		if !strings.Contains(mime, want) {
			t.Errorf("MIME missing %q", want)
		}
	}
}

func TestHumanizeDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{30 * time.Second, "30 seconds"},
		{1 * time.Minute, "1 minute"},
		{8 * time.Minute, "8 minutes"},
		{65 * time.Minute, "1 hour 5 minutes"},
		{2 * time.Hour, "2 hours"},
	}
	for _, c := range cases {
		if got := humanizeDuration(c.d); got != c.want {
			t.Errorf("humanizeDuration(%s) = %q, want %q", c.d, got, c.want)
		}
	}
}
