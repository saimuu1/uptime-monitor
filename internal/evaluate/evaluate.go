// Package evaluate holds the up/down decision logic.
//
//   - Transition is the simple v1 state machine (used by cmd/monitor).
//   - Monitor is the v3 engine: it aggregates results across regions into a
//     consensus and suppresses flapping before committing a state change.
//
// Everything here is pure (time is passed in), so the interesting logic —
// majority consensus and flap suppression — is unit-testable without a clock,
// a network, or a database.
package evaluate

import "time"

// Event is what a committed decision means relative to the previous state.
type Event int

const (
	// None: state unchanged.
	None Event = iota
	// Down: the monitor just went from up to down — open an incident + alert.
	Down
	// Recovered: the monitor just came back — resolve the incident + alert.
	Recovered
)

func (e Event) String() string {
	switch e {
	case Down:
		return "DOWN"
	case Recovered:
		return "RECOVERED"
	default:
		return "NONE"
	}
}

// Transition compares the previous up-state to the newly-observed one (v1).
func Transition(prevUp, nowUp bool) Event {
	switch {
	case prevUp && !nowUp:
		return Down
	case !prevUp && nowUp:
		return Recovered
	default:
		return None
	}
}

// Sample is one region's most recent observation.
type Sample struct {
	Up bool
	At time.Time
}

// Config tunes the v3 engine.
type Config struct {
	// Freshness: ignore a region's sample once it's older than this, so a
	// region whose checker died drops out of the vote instead of counting.
	Freshness time.Duration
	// Stability: a new consensus must hold this long before it's committed,
	// which suppresses brief up/down flapping.
	Stability time.Duration
}

// Monitor is the per-monitor consensus + debounce engine.
type Monitor struct {
	cfg          Config
	samples      map[string]Sample // region -> latest sample
	confirmedUp  bool              // last committed state
	pending      bool              // a candidate change is in flight
	pendingUp    bool              // the candidate state
	pendingSince time.Time         // when the candidate first appeared
}

// NewMonitor creates an engine seeded with a known starting state (e.g. from an
// open incident in the DB).
func NewMonitor(cfg Config, initialUp bool) *Monitor {
	return &Monitor{cfg: cfg, samples: make(map[string]Sample), confirmedUp: initialUp}
}

// Observe records the latest sample from a region.
func (m *Monitor) Observe(region string, s Sample) {
	m.samples[region] = s
}

// Up reports the last committed state.
func (m *Monitor) Up() bool { return m.confirmedUp }

// consensus returns the agreed up-state among regions with a fresh sample, and
// whether any fresh sample existed at all. A monitor is "down" only on a strict
// majority of fresh regions reporting down; ties stay up (conservative — this
// is what stops one bad network path from paging you).
func (m *Monitor) consensus(now time.Time) (up bool, ok bool) {
	var total, down int
	for _, s := range m.samples {
		if now.Sub(s.At) > m.cfg.Freshness {
			continue
		}
		total++
		if !s.Up {
			down++
		}
	}
	if total == 0 {
		return false, false
	}
	return 2*down <= total, true
}

// Evaluate recomputes consensus, applies flap suppression, and returns the
// committed transition (or None). It's safe — and expected — to call both on
// every new result and on a periodic tick, so a pending change still commits
// once its stability window elapses even if no new result arrives.
func (m *Monitor) Evaluate(now time.Time) Event {
	consUp, ok := m.consensus(now)
	if !ok {
		return None
	}
	if consUp == m.confirmedUp {
		m.pending = false // consensus agrees with committed state; cancel any candidate
		return None
	}
	// Consensus disagrees with the committed state. Start (or restart) the
	// candidate, and only commit once it has held for the stability window.
	if !m.pending || m.pendingUp != consUp {
		m.pending = true
		m.pendingUp = consUp
		m.pendingSince = now
	}
	if now.Sub(m.pendingSince) >= m.cfg.Stability {
		m.confirmedUp = consUp
		m.pending = false
		if consUp {
			return Recovered
		}
		return Down
	}
	return None
}
