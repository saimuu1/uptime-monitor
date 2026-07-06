// Package config loads the monitor list from a YAML file and seeds it into the
// database. After the first run the DB is the source of truth, but re-running
// with an edited config upserts the changes.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/saimuu1/uptime-monitor/internal/store"
)

// File is the top-level shape of config.yaml.
type File struct {
	Monitors []MonitorConfig `yaml:"monitors"`
}

// MonitorConfig is one monitor as written in YAML. Zero values are filled with
// the same defaults the DB schema uses.
type MonitorConfig struct {
	Name            string   `yaml:"name"`
	URL             string   `yaml:"url"`
	Method          string   `yaml:"method"`
	IntervalSeconds int      `yaml:"interval_seconds"`
	TimeoutMs       int      `yaml:"timeout_ms"`
	ExpectedStatus  int      `yaml:"expected_status"`
	Enabled         *bool    `yaml:"enabled"` // pointer so an omitted value defaults to true
	NotifyEmails    []string `yaml:"notify_emails"`
	ExpectedKeyword string   `yaml:"expected_keyword"`
	SlowThresholdMs int      `yaml:"slow_threshold_ms"`
}

// Load reads and parses the YAML file at path.
func Load(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if len(f.Monitors) == 0 {
		return nil, fmt.Errorf("config %s has no monitors", path)
	}
	return &f, nil
}

// StoreMonitors converts the parsed config into store.Monitor records with
// schema defaults applied, ready to upsert.
func (f *File) StoreMonitors() []store.Monitor {
	out := make([]store.Monitor, len(f.Monitors))
	for i, m := range f.Monitors {
		out[i] = m.toStoreMonitor()
	}
	return out
}

// toStoreMonitor applies defaults and converts to the store's Monitor type.
func (m MonitorConfig) toStoreMonitor() store.Monitor {
	sm := store.Monitor{
		Name:            m.Name,
		URL:             m.URL,
		Method:          m.Method,
		IntervalSeconds: m.IntervalSeconds,
		TimeoutMs:       m.TimeoutMs,
		ExpectedStatus:  m.ExpectedStatus,
		Enabled:         true,
		NotifyEmails:    m.NotifyEmails,
		ExpectedKeyword: m.ExpectedKeyword,
		SlowThresholdMs: m.SlowThresholdMs,
	}
	if sm.Method == "" {
		sm.Method = "GET"
	}
	if sm.IntervalSeconds == 0 {
		sm.IntervalSeconds = 30
	}
	if sm.TimeoutMs == 0 {
		sm.TimeoutMs = 5000
	}
	if sm.ExpectedStatus == 0 {
		sm.ExpectedStatus = 200
	}
	if m.Enabled != nil {
		sm.Enabled = *m.Enabled
	}
	return sm
}
