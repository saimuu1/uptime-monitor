// Package message defines the JSON payloads and NATS subjects that connect the
// v2 processes: the scheduler publishes CheckJobs, checkers consume them and
// publish CheckResults, and the evaluator consumes those.
package message

import "time"

const (
	// SubjectCheckRequest carries CheckJobs from the scheduler to checkers.
	SubjectCheckRequest = "checks.request"
	// SubjectCheckResult carries CheckResults from checkers to the evaluator.
	SubjectCheckResult = "checks.result"
	// QueueCheckers is the NATS queue group all checkers join, so each job is
	// delivered to exactly one checker (load balancing).
	QueueCheckers = "checkers"
)

// CheckJob is "please check this monitor now". It carries everything a checker
// needs so checkers stay stateless and never touch the database.
type CheckJob struct {
	MonitorID      int64  `json:"monitor_id"`
	Name           string `json:"name"`
	URL            string `json:"url"`
	Method         string `json:"method"`
	TimeoutMs      int    `json:"timeout_ms"`
	ExpectedStatus int    `json:"expected_status"`
}

// CheckResult is the outcome of one check, tagged with the region it ran from
// and the moment it ran (so the evaluator stores the real check time, not the
// time it happened to process the message).
type CheckResult struct {
	MonitorID  int64     `json:"monitor_id"`
	Name       string    `json:"name"`
	Region     string    `json:"region"`
	CheckedAt  time.Time `json:"checked_at"`
	Up         bool      `json:"up"`
	StatusCode int       `json:"status_code"`
	LatencyMs  int       `json:"latency_ms"`
	Error      string    `json:"error"`
}
