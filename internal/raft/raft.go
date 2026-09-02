// Package raft implements the Raft consensus algorithm from scratch — no
// third-party Raft library. This file is Phase 1: leader election. It brings up
// the node state machine (Follower/Candidate/Leader), the RequestVote and
// AppendEntries (heartbeat-only, for now) RPCs, the election restriction that
// keeps a stale node from winning, and the term/step-down rules that guarantee
// at most one leader per term. Log replication and commitment are Phase 2.
//
// Concurrency model: all Raft state is guarded by a single mutex (r.mu). Two
// background goroutines drive time — an election ticker and a heartbeat ticker —
// and inbound RPCs are handled synchronously by HandleRequestVote /
// HandleAppendEntries. A node never holds its own mutex while making an outbound
// RPC, so two nodes exchanging RPCs can never deadlock.
package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/saimuu1/uptime-monitor/internal/raftlog"
)

// NodeID identifies a cluster member. IDs are 1-based; 0 means "none" (used for
// "voted for nobody").
type NodeID uint64

// State is a node's current role.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Follower"
	}
}

// --- RPC payloads ---

// RequestVoteArgs is a candidate's request for a vote.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the response to a vote request.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs carries a heartbeat (Phase 1) and, from Phase 2, log
// entries. The log fields are part of the stable wire format now; Phase 1 only
// uses Term/LeaderID.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []raftlog.Entry
	LeaderCommit uint64
}

// AppendEntriesReply is the response to an AppendEntries.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

// Network is the outbound transport a node uses to reach its peers. The real
// implementation is TCP/gRPC (a later phase); tests inject an in-memory network
// that can drop, delay, and partition messages. A returned error means "not
// delivered" (a partition or timeout), which a candidate treats as no vote.
type Network interface {
	SendRequestVote(from, to NodeID, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(from, to NodeID, args AppendEntriesArgs) (AppendEntriesReply, error)
}

// Config tunes the election and heartbeat timing. In production the election
// timeout is ~150–300ms; tests shrink it. Randomization within the window is
// what breaks split votes.
type Config struct {
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	TickInterval       time.Duration
}

// DefaultConfig returns production-ish timing.
func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		TickInterval:       10 * time.Millisecond,
	}
}

// Raft is a single node.
type Raft struct {
	id    NodeID
	peers []NodeID // other members (not including self)
	net   Network
	cfg   Config
	dir   string // where hard state is persisted

	mu    sync.Mutex
	rng   *rand.Rand
	log   *raftlog.Log
	state State

	// Persistent across restarts (written before any RPC reply depends on it).
	currentTerm uint64
	votedFor    NodeID

	leaderID         NodeID
	electionDeadline time.Time
	closed           bool // set on Stop; suppresses further disk writes

	// Observability hook: called (without the lock) whenever the role changes.
	onState func(id NodeID, state State, term uint64)

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// New creates a node. dir must be a stable directory for this node's hard state
// and log; on restart the persisted term/vote are reloaded so the node never
// forgets a vote it already cast.
func New(id NodeID, peers []NodeID, net Network, cfg Config, dir string) (*Raft, error) {
	log, err := raftlog.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("raft: open log: %w", err)
	}
	hs, err := raftlog.LoadHardState(dir)
	if err != nil {
		log.Close()
		return nil, fmt.Errorf("raft: load hard state: %w", err)
	}
	r := &Raft{
		id:          id,
		peers:       append([]NodeID(nil), peers...),
		net:         net,
		cfg:         cfg,
		dir:         dir,
		rng:         rand.New(rand.NewSource(int64(id)*2654435761 + time.Now().UnixNano())),
		log:         log,
		state:       Follower,
		currentTerm: hs.Term,
		votedFor:    NodeID(hs.VotedFor),
		stopCh:      make(chan struct{}),
	}
	return r, nil
}

// SetOnState registers a callback fired on every role change (for tests/metrics).
func (r *Raft) SetOnState(fn func(id NodeID, state State, term uint64)) {
	r.mu.Lock()
	r.onState = fn
	r.mu.Unlock()
}

// Start launches the background tickers.
func (r *Raft) Start() {
	r.mu.Lock()
	r.resetElectionTimerLocked()
	r.mu.Unlock()

	r.wg.Add(2)
	go r.electionTicker()
	go r.heartbeatTicker()
}

// Stop halts the node's goroutines and closes its log.
func (r *Raft) Stop() {
	close(r.stopCh)
	r.wg.Wait()
	r.mu.Lock()
	r.closed = true // late per-send goroutines must not write after this
	_ = r.log.Close()
	r.mu.Unlock()
}

// Report returns the node's current role and term (for tests/observability).
func (r *Raft) Report() (State, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.currentTerm
}

// --- timing ---

func (r *Raft) resetElectionTimerLocked() {
	span := r.cfg.ElectionTimeoutMax - r.cfg.ElectionTimeoutMin
	d := r.cfg.ElectionTimeoutMin + time.Duration(r.rng.Int63n(int64(span)+1))
	r.electionDeadline = time.Now().Add(d)
}

func (r *Raft) electionTicker() {
	defer r.wg.Done()
	t := time.NewTicker(r.cfg.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-t.C:
			r.mu.Lock()
			timedOut := r.state != Leader && time.Now().After(r.electionDeadline)
			r.mu.Unlock()
			if timedOut {
				r.startElection()
			}
		}
	}
}

func (r *Raft) heartbeatTicker() {
	defer r.wg.Done()
	t := time.NewTicker(r.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-t.C:
			r.mu.Lock()
			isLeader := r.state == Leader
			r.mu.Unlock()
			if isLeader {
				r.broadcastHeartbeat()
			}
		}
	}
}

// --- persistence ---

// persistLocked writes the term/vote to stable storage. It runs under r.mu; the
// fsync inside briefly blocks the node — acceptable for correctness, and a known
// place to later batch/async without changing semantics.
func (r *Raft) persistLocked() {
	if r.closed {
		return
	}
	if err := raftlog.SaveHardState(r.dir, raftlog.HardState{
		Term:     r.currentTerm,
		VotedFor: uint64(r.votedFor),
	}); err != nil {
		// A failed persist risks a double-vote after a crash; in a real system
		// this is fatal. Tests use a temp dir where this won't fail.
		fmt.Printf("raft %d: persist: %v\n", r.id, err)
	}
}

// --- role transitions (all require r.mu) ---

func (r *Raft) becomeFollowerLocked(term uint64) {
	changed := r.state != Follower
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = 0
		r.persistLocked()
	}
	r.state = Follower
	if changed {
		r.notifyStateLocked()
	}
}

func (r *Raft) becomeLeaderLocked() {
	r.state = Leader
	r.leaderID = r.id
	r.notifyStateLocked()
}

func (r *Raft) notifyStateLocked() {
	if r.onState != nil {
		fn, st, term := r.onState, r.state, r.currentTerm
		// Fire without holding the lock to avoid re-entrancy into Raft.
		go fn(r.id, st, term)
	}
}

func (r *Raft) lastLogLocked() (index, term uint64) {
	index = r.log.LastIndex()
	term, _ = r.log.TermAt(index)
	return index, term
}

func (r *Raft) majority() int { return (len(r.peers)+1)/2 + 1 }

// --- election ---

func (r *Raft) startElection() {
	r.mu.Lock()
	if r.state == Leader {
		r.mu.Unlock()
		return
	}
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.persistLocked()
	r.resetElectionTimerLocked()
	term := r.currentTerm
	lastIdx, lastTerm := r.lastLogLocked()
	r.notifyStateLocked()
	peers := r.peers
	r.mu.Unlock()

	args := RequestVoteArgs{Term: term, CandidateID: r.id, LastLogIndex: lastIdx, LastLogTerm: lastTerm}

	var votes struct {
		sync.Mutex
		count int
	}
	votes.count = 1 // vote for self

	for _, peer := range peers {
		go func(peer NodeID) {
			reply, err := r.net.SendRequestVote(r.id, peer, args)
			if err != nil {
				return // undelivered = no vote
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			// Ignore stale replies: a newer term or role change happened since.
			if r.currentTerm != term || r.state != Candidate {
				return
			}
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				r.resetElectionTimerLocked()
				return
			}
			if !reply.VoteGranted {
				return
			}
			votes.Lock()
			votes.count++
			won := votes.count >= r.majority()
			votes.Unlock()
			if won && r.state == Candidate {
				r.becomeLeaderLocked()
				// Assert authority immediately so followers don't time out.
				go r.broadcastHeartbeat()
			}
		}(peer)
	}
}

func (r *Raft) broadcastHeartbeat() {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	term := r.currentTerm
	peers := r.peers
	prevIdx, prevTerm := r.lastLogLocked()
	r.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      nil, // Phase 1: heartbeat only
	}
	for _, peer := range peers {
		go func(peer NodeID) {
			reply, err := r.net.SendAppendEntries(r.id, peer, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				r.resetElectionTimerLocked()
			}
		}(peer)
	}
}

// --- RPC handlers ---

// HandleRequestVote processes an incoming vote request. It grants a vote only if
// the candidate's term is current, this node hasn't already voted for someone
// else this term, and the candidate's log is at least as up-to-date as ours (the
// election restriction — this is what stops a node missing committed entries
// from ever becoming leader).
func (r *Raft) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		return RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	}
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}

	lastIdx, lastTerm := r.lastLogLocked()
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	grant := (r.votedFor == 0 || r.votedFor == args.CandidateID) && upToDate
	if grant {
		r.votedFor = args.CandidateID
		r.persistLocked()
		r.resetElectionTimerLocked() // granting a vote defers our own election
	}
	return RequestVoteReply{Term: r.currentTerm, VoteGranted: grant}
}

// HandleAppendEntries processes an incoming heartbeat/replication RPC. In Phase 1
// it enforces the term rules and recognizes the sender as leader; the log
// consistency check and entry application arrive in Phase 2.
func (r *Raft) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		return AppendEntriesReply{Term: r.currentTerm, Success: false}
	}
	// A valid leader for this (or a newer) term: adopt the term if newer, and
	// step down from Candidate/Leader — someone else won this term.
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	} else if r.state != Follower {
		r.state = Follower
		r.notifyStateLocked()
	}
	r.leaderID = args.LeaderID
	r.resetElectionTimerLocked()
	return AppendEntriesReply{Term: r.currentTerm, Success: true}
}
