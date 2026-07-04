// Command web serves a simple status page: the current up/down state of each
// monitor plus its 24h uptime, read straight from the database. Plain HTML with
// a meta-refresh — no JavaScript needed.
package main

import (
	"context"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"time"

	"github.com/saimuu1/uptime-monitor/internal/env"
	"github.com/saimuu1/uptime-monitor/internal/metrics"
	"github.com/saimuu1/uptime-monitor/internal/store"
	"github.com/saimuu1/uptime-monitor/web"
)

type row struct {
	Name      string
	URL       string
	Down      bool
	DotClass  string
	UptimePct string
	LastCheck string
}

type page struct {
	Rows    []row
	UpCount int
	Updated string
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

	addr := env.WebAddr()
	log.Printf("status page listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func buildPage(statuses []store.Status) page {
	p := page{Updated: time.Now().Format("15:04:05 MST")}
	for _, s := range statuses {
		r := row{
			Name:      s.Name,
			URL:       s.URL,
			Down:      s.Down,
			UptimePct: fmt.Sprintf("%.2f%% (24h)", s.Uptime24h*100),
			LastCheck: "never",
		}
		switch {
		case s.Down:
			r.DotClass = "down"
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
