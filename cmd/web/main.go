// Command web serves a simple status page: the current up/down state of each
// monitor plus its 24h uptime, read straight from the database. Plain HTML with
// a meta-refresh — no JavaScript needed.
package main

import (
	"context"
	"crypto/subtle"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/saimuu1/uptime-monitor/internal/env"
	"github.com/saimuu1/uptime-monitor/internal/metrics"
	"github.com/saimuu1/uptime-monitor/internal/store"
	"github.com/saimuu1/uptime-monitor/web"
)

type row struct {
	ID        int64
	Name      string
	URL       string
	Down      bool
	DotClass  string
	UptimePct string
	LastCheck string
}

type page struct {
	Rows      []row
	UpCount   int
	DownCount int
	Updated   string
}

func main() {
	st, err := store.New(context.Background(), env.DBURL())
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer st.Close()

	tmpl := template.Must(template.ParseFS(web.Templates, "templates/status.html"))

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		statuses, err := st.MonitorStatuses(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := tmpl.Execute(w, buildPage(statuses)); err != nil {
			log.Printf("render: %v", err)
		}
	})

	// Add a site: URL required; name/interval/email optional. Upserts to the DB;
	// the scheduler picks it up on its next reconcile (within ~15s).
	mux.HandleFunc("POST /monitors", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		url := normalizeURL(strings.TrimSpace(r.FormValue("url")))
		if url == "" {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		name := strings.TrimSpace(r.FormValue("name"))
		if name == "" {
			name = url
		}
		interval := 30
		if v, err := strconv.Atoi(r.FormValue("interval_seconds")); err == nil && v > 0 {
			interval = v
		}
		if _, err := st.UpsertMonitor(r.Context(), store.Monitor{
			Name:            name,
			URL:             url,
			Method:          "GET",
			IntervalSeconds: interval,
			TimeoutMs:       5000,
			ExpectedStatus:  200,
			Enabled:         true,
			NotifyEmails:    splitEmails(r.FormValue("email")),
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/", http.StatusSeeOther)
	})

	// Remove a site.
	mux.HandleFunc("POST /monitors/delete", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.FormValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		if err := st.DeleteMonitor(r.Context(), id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/", http.StatusSeeOther)
	})

	// Optional login wall. Off when WEB_USER/WEB_PASS are unset (local dev);
	// turn it on before exposing the page to the public internet.
	var handler http.Handler = mux
	if u, p := env.WebUser(), env.WebPass(); u != "" && p != "" {
		handler = basicAuth(mux, u, p)
		log.Print("web: login required (WEB_USER/WEB_PASS set)")
	} else {
		log.Print("web: OPEN — no WEB_USER/WEB_PASS set (fine locally; set them before going public)")
	}

	addr := env.WebAddr()
	log.Printf("status page listening on %s", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatal(err)
	}
}

// basicAuth wraps h with HTTP Basic Auth, except /healthz and /metrics which
// stay open for liveness checks and Prometheus scraping.
func basicAuth(h http.Handler, user, pass string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			h.ServeHTTP(w, r)
			return
		}
		u, p, ok := r.BasicAuth()
		userOK := subtle.ConstantTimeCompare([]byte(u), []byte(user)) == 1
		passOK := subtle.ConstantTimeCompare([]byte(p), []byte(pass)) == 1
		if !ok || !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="Uptime Monitor"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func buildPage(statuses []store.Status) page {
	p := page{Updated: time.Now().Format("15:04:05 MST")}
	for _, s := range statuses {
		r := row{
			ID:        s.ID,
			Name:      s.Name,
			URL:       s.URL,
			Down:      s.Down,
			UptimePct: fmt.Sprintf("%.2f%% (24h)", s.Uptime24h*100),
			LastCheck: "never",
		}
		switch {
		case s.Down:
			r.DotClass = "down"
			p.DownCount++
		case s.Checks24h == 0:
			r.DotClass = "nodata"
			r.UptimePct = "no data"
		default:
			r.DotClass = "up"
			p.UpCount++
		}
		if s.LastCheck != nil {
			r.LastCheck = humanizeAgo(time.Since(*s.LastCheck))
		}
		p.Rows = append(p.Rows, r)
	}
	return p
}

// normalizeURL adds https:// if the user didn't type a scheme.
func normalizeURL(u string) string {
	if u == "" {
		return ""
	}
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		return "https://" + u
	}
	return u
}

// splitEmails turns a comma/space-separated string into a clean list.
func splitEmails(s string) []string {
	var out []string
	for _, part := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' }) {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func humanizeAgo(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
}
