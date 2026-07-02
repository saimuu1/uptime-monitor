package evaluate

import (
	"testing"
	"time"
)

func TestTransition(t *testing.T) {
	tests := []struct {
		name   string
		prevUp bool
		nowUp  bool
		want   Event
	}{
		{"up stays up", true, true, None},
		{"up goes down", true, false, Down},
		{"down comes back", false, true, Recovered},
		{"down stays down", false, false, None},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Transition(tt.prevUp, tt.nowUp); got != tt.want {
				t.Errorf("Transition(%v, %v) = %v, want %v", tt.prevUp, tt.nowUp, got, tt.want)
			}
		})
	}
}

// helper: build an engine with instant commit (no flap window) to test the
// consensus vote in isolation.
func instant(initialUp bool) *Monitor {
	return NewMonitor(Config{Freshness: time.Minute, Stability: 0}, initialUp)
}

func TestConsensusMajority(t *testing.T) {
	now := time.Now()
	fresh := func(up bool) Sample { return Sample{Up: up, At: now} }

	tests := []struct {
		name    string
		regions map[string]bool // region -> up
		startUp bool
		want    Event
	}{
		{"single region down", map[string]bool{"a": false}, true, Down},
		{"one of three down stays up", map[string]bool{"a": false, "b": true, "c": true}, true, None},
		{"two of three down goes down", map[string]bool{"a": false, "b": false, "c": true}, true, Down},
		{"tie two regions stays up", map[string]bool{"a": false, "b": true}, true, None},
		{"all recover", map[string]bool{"a": true, "b": true, "c": true}, false, Recovered},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := instant(tt.startUp)
			for region, up := range tt.regions {
				m.Observe(region, fresh(up))
			}
			if got := m.Evaluate(now); got != tt.want {
				t.Errorf("Evaluate = %v, want %v", got, tt.want)
			}
		})
	}
}

// A region whose checker died stops sending samples; once its last sample goes
// stale it must drop out of the vote rather than counting as down.
func TestStaleRegionExcluded(t *testing.T) {
	m := NewMonitor(Config{Freshness: 10 * time.Second, Stability: 0}, true)
	t0 := time.Now()

	// Two regions healthy.
	m.Observe("east", Sample{Up: true, At: t0})
	m.Observe("west", Sample{Up: true, At: t0})
	if got := m.Evaluate(t0); got != None {
		t.Fatalf("healthy start: got %v", got)
	}

	// "east" checker dies (no new samples). 30s later only west reports, and
	// it's still up: consensus must stay up (no false DOWN from the gone region).
	later := t0.Add(30 * time.Second)
	m.Observe("west", Sample{Up: true, At: later})
	if got := m.Evaluate(later); got != None {
		t.Errorf("killed region should not trigger alert, got %v", got)
	}
	if !m.Up() {
		t.Error("monitor should still be up after one region's checker died")
	}
}

// A brief blip shorter than the stability window must NOT open an incident.
func TestFlapSuppressed(t *testing.T) {
	m := NewMonitor(Config{Freshness: time.Minute, Stability: 5 * time.Second}, true)
	t0 := time.Now()

	m.Observe("a", Sample{Up: true, At: t0})
	m.Evaluate(t0)

	// Goes down at t0, but only for 2s...
	m.Observe("a", Sample{Up: false, At: t0.Add(1 * time.Second)})
	if got := m.Evaluate(t0.Add(1 * time.Second)); got != None {
		t.Fatalf("should not commit before stability window: got %v", got)
	}
	// ...and recovers before the 5s window elapses.
	m.Observe("a", Sample{Up: true, At: t0.Add(3 * time.Second)})
	if got := m.Evaluate(t0.Add(3 * time.Second)); got != None {
		t.Errorf("flap should be suppressed, got %v", got)
	}
	if !m.Up() {
		t.Error("monitor should remain up after a suppressed flap")
	}
}

// A sustained outage longer than the stability window MUST commit a DOWN, then
// a sustained recovery MUST commit a RECOVERED.
func TestSustainedOutageCommits(t *testing.T) {
	m := NewMonitor(Config{Freshness: time.Minute, Stability: 5 * time.Second}, true)
	t0 := time.Now()

	m.Observe("a", Sample{Up: false, At: t0})
	if got := m.Evaluate(t0); got != None {
		t.Fatalf("t0: got %v, want None (window not elapsed)", got)
	}
	// 6s later, still down -> commit DOWN.
	down := t0.Add(6 * time.Second)
	m.Observe("a", Sample{Up: false, At: down})
	if got := m.Evaluate(down); got != Down {
		t.Fatalf("sustained outage: got %v, want Down", got)
	}
	// Recovers and holds for the window -> commit RECOVERED.
	m.Observe("a", Sample{Up: true, At: down.Add(1 * time.Second)})
	m.Evaluate(down.Add(1 * time.Second))
	rec := down.Add(7 * time.Second)
	m.Observe("a", Sample{Up: true, At: rec})
	if got := m.Evaluate(rec); got != Recovered {
		t.Fatalf("sustained recovery: got %v, want Recovered", got)
	}
}
