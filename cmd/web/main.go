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

const historyDays = 90

type bar struct {
	Class string // up | partial | down | nodata
	Title string // tooltip, e.g. "Jul 4: 100%"
}

type row struct {
	ID        int64
	Name      string
	URL       string
	Down      bool
	DotClass  string
	UptimePct string
	LastCheck string
	Bars      []bar
	Cert      string // "SSL 87d" etc., empty if unknown
	CertWarn  bool   // cert expiring soon
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

	tmpl := template.Must(template.ParseFS(web.Templates, "templates/*.html"))

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
		history, err := st.UptimeHistory(r.Context(), historyDays)
		if err != nil {
			log.Printf("history: %v", err) // non-fatal: page still renders without bars
		}
		if err := tmpl.ExecuteTemplate(w, "status.html", buildPage(statuses, history)); err != nil {
			log.Printf("render: %v", err)
		}
	})

	// Incident history page.
	mux.HandleFunc("GET /incidents", func(w http.ResponseWriter, r *http.Request) {
		incidents, err := st.RecentIncidents(r.Context(), 50)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := tmpl.ExecuteTemplate(w, "incidents.html", buildIncidents(incidents)); err != nil {
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

func buildPage(statuses []store.Status, history []store.DayUptime) page {
	// Index history by monitor + UTC day so we can look up each bar.
	byDay := make(map[int64]map[string]store.DayUptime)
	for _, d := range history {
		key := d.Day.UTC().Format("2006-01-02")
		if byDay[d.MonitorID] == nil {
			byDay[d.MonitorID] = make(map[string]store.DayUptime)
		}
		byDay[d.MonitorID][key] = d
	}

	p := page{Updated: time.Now().Format("15:04:05 MST")}
	for _, s := range statuses {
		r := row{
			ID:        s.ID,
			Name:      s.Name,
			URL:       s.URL,
			Down:      s.Down,
			UptimePct: fmt.Sprintf("%.2f%% (24h)", s.Uptime24h*100),
			LastCheck: "never",
			Bars:      buildBars(byDay[s.ID]),
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
		if s.CertExpiry != nil {
			days := int(time.Until(*s.CertExpiry).Hours() / 24)
			r.Cert = fmt.Sprintf("SSL %dd", days)
			r.CertWarn = days < 14
		}
		p.Rows = append(p.Rows, r)
	}
	return p
}

type incidentRow struct {
	Monitor  string
	Cause    string
	Duration string
	When     string
	Ongoing  bool
}

type incidentsPage struct {
	Incidents []incidentRow
}

func buildIncidents(incidents []store.Incident) incidentsPage {
	var p incidentsPage
	for _, in := range incidents {
		r := incidentRow{
			Monitor: in.MonitorName,
			Cause:   in.Cause,
			When:    in.StartedAt.Format("Jan 2, 15:04"),
		}
		if in.ResolvedAt == nil {
			r.Ongoing = true
			r.Duration = "ongoing · " + humanizeDuration(time.Since(in.StartedAt))
		} else {
			r.Duration = humanizeDuration(in.ResolvedAt.Sub(in.StartedAt))
		}
		p.Incidents = append(p.Incidents, r)
	}
	return p
}

// humanizeDuration renders a span like "3m 12s" or "1h 4m".
func humanizeDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd %dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}

// buildBars renders the last historyDays days as colored bars, oldest to
// newest. Days with no checks show as "no data" (grey), like a real status page.
func buildBars(days map[string]store.DayUptime) []bar {
	bars := make([]bar, 0, historyDays)
	today := time.Now().UTC()
	for i := historyDays - 1; i >= 0; i-- {
		d := today.AddDate(0, 0, -i)
		label := d.Format("Jan 2")
		day, ok := days[d.Format("2006-01-02")]
		switch {
		case !ok:
			bars = append(bars, bar{Class: "nodata", Title: label + " · no data"})
		case day.Ratio >= 0.999:
			bars = append(bars, bar{Class: "up", Title: fmt.Sprintf("%s · 100%%", label)})
		case day.Ratio >= 0.9:
			bars = append(bars, bar{Class: "partial", Title: fmt.Sprintf("%s · %.1f%%", label, day.Ratio*100)})
		default:
			bars = append(bars, bar{Class: "down", Title: fmt.Sprintf("%s · %.1f%%", label, day.Ratio*100)})
		}
	}
	return bars
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
