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
