// Package raft implements the Raft consensus algorithm from scratch — no
// third-party Raft library.
//
//   - Phase 1: leader election (state machine, RequestVote with the election
//     restriction, term step-down, persistent term/vote).
//   - Phase 2 (this file): log replication and commitment. The leader replicates
//     its log to followers with a per-follower nextIndex/matchIndex and an
//     accelerated backtracking scheme; AppendEntries enforces the
//     prevLogIndex/prevLogTerm consistency check and truncates conflicting tails;
//     the leader advances commitIndex only for entries from its OWN term
//     (the Figure-8 rule); and committed entries are applied, in order, to a
//     user state machine via a callback.
//
// Concurrency: all Raft state is guarded by r.mu. Three background goroutines
// drive time and application (election ticker, heartbeat/replication ticker,
// apply loop). A node never holds r.mu while making an outbound RPC, so two
// nodes exchanging RPCs cannot deadlock.
package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/saimuu1/uptime-monitor/internal/raftlog"
)

// NodeID identifies a cluster member (1-based; 0 = "none").
type NodeID uint64

// State is a node's role.
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

// ApplyMsg is a committed log entry handed to the state machine, in log order.
type ApplyMsg struct {
	Index uint64
	Term  uint64
	Type  raftlog.EntryType
	Data  []byte
}

// --- RPC payloads ---

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []raftlog.Entry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// Accelerated backtracking hints, set on a failed consistency check so the
	// leader can skip a whole term's worth of entries in one round trip instead
	// of decrementing nextIndex by one at a time.
	ConflictTerm  uint64 // term of the follower's entry at PrevLogIndex (0 if none)
	ConflictIndex uint64 // where the leader should resume probing
}

// InstallSnapshotArgs ships a whole snapshot to a follower that has fallen so far
// behind the leader compacted the entries it needs. (Chunking is a production
// concern; this sends the snapshot in one shot.)
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
}

// Network is the outbound transport. Tests inject an in-memory network with
// partitions; a returned error means "not delivered".
type Network interface {
	SendRequestVote(from, to NodeID, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(from, to NodeID, args AppendEntriesArgs) (AppendEntriesReply, error)
	SendInstallSnapshot(from, to NodeID, args InstallSnapshotArgs) (InstallSnapshotReply, error)
}

// Config tunes election/heartbeat timing.
type Config struct {
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	TickInterval       time.Duration
}

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
	peers []NodeID
	net   Network
	cfg   Config
	dir   string

	mu        sync.Mutex
	applyCond *sync.Cond
	rng       *rand.Rand
	log       *raftlog.Log
	state     State

	// Persistent (written before any dependent RPC reply).
	currentTerm uint64
	votedFor    NodeID

	// Volatile.
	commitIndex uint64
	lastApplied uint64
	leaderID    NodeID

	// Leader-only, reinitialized on election.
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	electionDeadline time.Time
	closed           bool

	apply         func(ApplyMsg)
	snapshotFn    func() []byte // serialize the state machine
	restoreFn     func([]byte)  // load the state machine from a snapshot
	snapThreshold uint64        // compact once this many entries pile up past the snapshot
	onState       func(id NodeID, state State, term uint64)

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// New creates a node, reloading persisted term/vote and log from dir.
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
		nextIndex:   map[NodeID]uint64{},
		matchIndex:  map[NodeID]uint64{},
		stopCh:      make(chan struct{}),
	}
	r.applyCond = sync.NewCond(&r.mu)
	return r, nil
}

// SetApply registers the state-machine apply callback. Call before Start.
func (r *Raft) SetApply(fn func(ApplyMsg)) { r.mu.Lock(); r.apply = fn; r.mu.Unlock() }

// SetSnapshot enables log compaction: snapshot serializes the state machine,
// restore loads it, and threshold is how many entries may pile up past the
// current snapshot before the node compacts. threshold 0 disables compaction.
// Call before Start.
func (r *Raft) SetSnapshot(snapshot func() []byte, restore func([]byte), threshold uint64) {
	r.mu.Lock()
	r.snapshotFn = snapshot
	r.restoreFn = restore
	r.snapThreshold = threshold
	r.mu.Unlock()
}

// SetOnState registers a role-change callback (tests/metrics).
func (r *Raft) SetOnState(fn func(NodeID, State, uint64)) {
	r.mu.Lock()
	r.onState = fn
	r.mu.Unlock()
}

// Start launches the background goroutines. If a snapshot exists on disk, the
// state machine is restored from it and applied progress is seeded to the
// snapshot boundary (so a restart never re-applies compacted entries).
func (r *Raft) Start() {
	r.mu.Lock()
	if si := r.log.SnapshotIndex(); si > 0 {
		if r.restoreFn != nil {
			r.restoreFn(r.log.SnapshotData())
		}
		r.lastApplied = si
		if r.commitIndex < si {
			r.commitIndex = si
		}
	}
	r.resetElectionTimerLocked()
	r.mu.Unlock()
	r.wg.Add(3)
	go r.electionTicker()
	go r.heartbeatTicker()
	go r.applyLoop()
}

// Stop halts the node and closes its log.
func (r *Raft) Stop() {
	close(r.stopCh)
	r.mu.Lock()
	r.closed = true
	r.applyCond.Broadcast() // wake the apply loop so it can exit
	r.mu.Unlock()
	r.wg.Wait()
	r.mu.Lock()
	_ = r.log.Close()
	r.mu.Unlock()
}

// Report returns the node's role and term.
func (r *Raft) Report() (State, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.currentTerm
}

// CommitIndex / LastApplied expose progress (tests/observability).
func (r *Raft) CommitIndex() uint64 { r.mu.Lock(); defer r.mu.Unlock(); return r.commitIndex }
func (r *Raft) LastApplied() uint64 { r.mu.Lock(); defer r.mu.Unlock(); return r.lastApplied }

// Propose submits a command. If this node isn't the leader it returns
// isLeader=false. Otherwise it appends the command and kicks replication,
// returning the index/term the entry will occupy if committed.
func (r *Raft) Propose(data []byte) (index, term uint64, isLeader bool) {
	r.mu.Lock()
	if r.state != Leader || r.closed {
		r.mu.Unlock()
		return 0, 0, false
	}
	e, err := r.log.Append(r.currentTerm, raftlog.EntryCommand, data)
	if err != nil {
		r.mu.Unlock()
		return 0, 0, false
	}
	index, term = e.Index, r.currentTerm
	r.mu.Unlock()

	go r.replicateToAll()
	return index, term, true
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
				r.replicateToAll()
			}
		}
	}
}

// --- apply loop ---

func (r *Raft) applyLoop() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		for !r.closed && r.lastApplied >= r.commitIndex {
			r.applyCond.Wait()
		}
		if r.closed {
			r.mu.Unlock()
			return
		}
		r.lastApplied++
		idx := r.lastApplied
		e, ok := r.log.At(idx)
		if ok && r.apply != nil {
			// Applied under r.mu so it can't interleave with a snapshot restore,
			// which also mutates the state machine. Our state machines are fast;
			// a production system would hand this to a dedicated apply thread.
			r.apply(ApplyMsg{Index: e.Index, Term: e.Term, Type: e.Type, Data: e.Data})
		}
		r.maybeSnapshotLocked(idx)
		r.mu.Unlock()
	}
}

// maybeSnapshotLocked compacts the log once enough entries have piled up past the
// current snapshot. Runs in the apply loop under r.mu, so the snapshot reflects
// exactly the entries applied so far.
func (r *Raft) maybeSnapshotLocked(idx uint64) {
	if r.snapshotFn == nil || r.snapThreshold == 0 {
		return
	}
	if idx-r.log.SnapshotIndex() < r.snapThreshold {
		return
	}
	term, ok := r.log.TermAt(idx)
	if !ok {
		return
	}
	if err := r.log.Snapshot(idx, term, r.snapshotFn()); err != nil {
		fmt.Printf("raft %d: snapshot: %v\n", r.id, err)
	}
}

// --- persistence ---

func (r *Raft) persistLocked() {
	if r.closed {
		return
	}
	if err := raftlog.SaveHardState(r.dir, raftlog.HardState{
		Term: r.currentTerm, VotedFor: uint64(r.votedFor),
	}); err != nil {
		fmt.Printf("raft %d: persist: %v\n", r.id, err)
	}
}

// --- role transitions (require r.mu) ---

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
	// Reinitialize follower progress: assume they match our whole log, back off
	// on rejection.
	last := r.log.LastIndex()
	for _, p := range r.peers {
		r.nextIndex[p] = last + 1
		r.matchIndex[p] = 0
	}
	// Append a no-op in our own term. Committing it (once replicated) also lets
	// us commit entries carried over from previous terms — see maybeAdvanceCommit.
	if _, err := r.log.Append(r.currentTerm, raftlog.EntryNoop, nil); err != nil {
		fmt.Printf("raft %d: leader no-op: %v\n", r.id, err)
	}
	r.notifyStateLocked()
}

func (r *Raft) notifyStateLocked() {
	if r.onState != nil {
		fn, st, term := r.onState, r.state, r.currentTerm
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
	if r.state == Leader || r.closed {
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
	votes.count = 1

	for _, peer := range peers {
		go func(peer NodeID) {
			reply, err := r.net.SendRequestVote(r.id, peer, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
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
				go r.replicateToAll()
			}
		}(peer)
	}
}

// --- replication ---

func (r *Raft) replicateToAll() {
	r.mu.Lock()
	isLeader := r.state == Leader
	peers := r.peers
	r.mu.Unlock()
	if !isLeader {
		return
	}
	for _, peer := range peers {
		go r.replicateTo(peer)
	}
}

// replicateTo sends the follower whatever entries it's missing (or an empty
// heartbeat), then processes the reply: advancing its progress on success, or
// backing up nextIndex on a consistency failure.
func (r *Raft) replicateTo(peer NodeID) {
	r.mu.Lock()
	if r.state != Leader || r.closed {
		r.mu.Unlock()
		return
	}
	term := r.currentTerm
	ni := r.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	// If the follower needs entries we've already compacted away, ship the
	// snapshot instead of an AppendEntries.
	if si := r.log.SnapshotIndex(); ni <= si {
		r.sendSnapshotLocked(peer, term, si)
		return
	}
	prevIndex := ni - 1
	prevTerm, _ := r.log.TermAt(prevIndex)
	entries := r.log.From(ni)
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	reply, err := r.net.SendAppendEntries(r.id, peer, args)
	if err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currentTerm != term || r.state != Leader {
		return // stale
	}
	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		r.resetElectionTimerLocked()
		return
	}
	if reply.Success {
		match := prevIndex + uint64(len(entries))
		if match > r.matchIndex[peer] {
			r.matchIndex[peer] = match
		}
		r.nextIndex[peer] = r.matchIndex[peer] + 1
		r.maybeAdvanceCommitLocked()
		return
	}
	// Failed consistency check: back up using the follower's hint.
	if reply.ConflictTerm == 0 {
		r.nextIndex[peer] = maxU(1, reply.ConflictIndex)
	} else if last := r.lastIndexWithTermLocked(reply.ConflictTerm); last > 0 {
		r.nextIndex[peer] = last + 1
	} else {
		r.nextIndex[peer] = maxU(1, reply.ConflictIndex)
	}
}

// sendSnapshotLocked ships the current snapshot to a lagging follower. It is
// called with r.mu held, releases the lock to make the RPC, and returns with the
// lock released. On success the follower is known to hold everything through the
// snapshot's last-included index.
func (r *Raft) sendSnapshotLocked(peer NodeID, term, si uint64) {
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderID:          r.id,
		LastIncludedIndex: si,
		LastIncludedTerm:  r.log.SnapshotTerm(),
		Data:              r.log.SnapshotData(),
	}
	r.mu.Unlock()

	reply, err := r.net.SendInstallSnapshot(r.id, peer, args)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currentTerm != term || r.state != Leader {
		return
	}
	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		r.resetElectionTimerLocked()
		return
	}
	if si > r.matchIndex[peer] {
		r.matchIndex[peer] = si
	}
	r.nextIndex[peer] = r.matchIndex[peer] + 1
	r.maybeAdvanceCommitLocked()
}

// maybeAdvanceCommitLocked advances commitIndex to the highest N such that a
// majority of nodes have entry N AND log[N].term == currentTerm. Counting only
// current-term entries is the Figure-8 rule: a leader must never consider an
// entry from a previous term committed on replica-count alone, or a later leader
// could still overwrite it.
func (r *Raft) maybeAdvanceCommitLocked() {
	last := r.log.LastIndex()
	advanced := false
	for n := r.commitIndex + 1; n <= last; n++ {
		termN, ok := r.log.TermAt(n)
		if !ok || termN != r.currentTerm {
			continue
		}
		count := 1 // self has it
		for _, p := range r.peers {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if count >= r.majority() {
			r.commitIndex = n
			advanced = true
		}
	}
	if advanced {
		r.applyCond.Broadcast()
	}
}

func (r *Raft) lastIndexWithTermLocked(term uint64) uint64 {
	for i := r.log.LastIndex(); i >= 1; i-- {
		t, ok := r.log.TermAt(i)
		if !ok {
			break
		}
		if t == term {
			return i
		}
		if t < term {
			break // log terms are non-decreasing; no earlier entry can match
		}
	}
	return 0
}

func (r *Raft) firstIndexWithTermLocked(term uint64) uint64 {
	for i := uint64(1); i <= r.log.LastIndex(); i++ {
		if t, _ := r.log.TermAt(i); t == term {
			return i
		}
	}
	return 1
}

// --- RPC handlers ---

func (r *Raft) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return RequestVoteReply{Term: r.currentTerm}
	}
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
		r.resetElectionTimerLocked()
	}
	return RequestVoteReply{Term: r.currentTerm, VoteGranted: grant}
}

// HandleAppendEntries runs the follower side of replication: the term rules, the
// prevLogIndex/prevLogTerm consistency check (with an accelerated conflict hint),
// conflict truncation, appending new entries, and advancing commitIndex.
func (r *Raft) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return AppendEntriesReply{Term: r.currentTerm}
	}
	if args.Term < r.currentTerm {
		return AppendEntriesReply{Term: r.currentTerm, Success: false}
	}
	// Valid leader for this or a newer term.
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	} else if r.state != Follower {
		r.state = Follower
		r.notifyStateLocked()
	}
	r.leaderID = args.LeaderID
	r.resetElectionTimerLocked()

	// Anything at or below our snapshot is already committed; if the leader
	// probes below it, tell it to resume just above the boundary.
	if si := r.log.SnapshotIndex(); args.PrevLogIndex < si {
		return AppendEntriesReply{Term: r.currentTerm, Success: false, ConflictIndex: si + 1}
	}

	last := r.log.LastIndex()

	// Consistency check at PrevLogIndex.
	if args.PrevLogIndex > last {
		// Our log is too short; tell the leader to resume at our end.
		return AppendEntriesReply{Term: r.currentTerm, Success: false, ConflictIndex: last + 1, ConflictTerm: 0}
	}
	if args.PrevLogIndex >= 1 {
		ourTerm, _ := r.log.TermAt(args.PrevLogIndex)
		if ourTerm != args.PrevLogTerm {
			// Report the conflicting term and where it started, so the leader can
			// skip the whole term in one step.
			return AppendEntriesReply{
				Term: r.currentTerm, Success: false,
				ConflictTerm:  ourTerm,
				ConflictIndex: r.firstIndexWithTermLocked(ourTerm),
			}
		}
	}

	// Logs agree up to PrevLogIndex. Append entries, truncating on the first
	// conflict; entries we already have (matching term) are skipped so a stale
	// or duplicate AppendEntries never truncates committed suffixes.
	for i := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= r.log.LastIndex() {
			if t, _ := r.log.TermAt(idx); t == args.Entries[i].Term {
				continue
			}
			if err := r.log.TruncateSuffix(idx); err != nil {
				fmt.Printf("raft %d: truncate: %v\n", r.id, err)
			}
		}
		for _, e := range args.Entries[i:] {
			if err := r.log.AppendEntry(e); err != nil {
				fmt.Printf("raft %d: append: %v\n", r.id, err)
			}
		}
		break
	}

	// Advance commit to min(leaderCommit, index of last new entry).
	if args.LeaderCommit > r.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		if c := minU(args.LeaderCommit, lastNew); c > r.commitIndex {
			r.commitIndex = c
			r.applyCond.Broadcast()
		}
	}
	return AppendEntriesReply{Term: r.currentTerm, Success: true}
}

// HandleInstallSnapshot applies a snapshot from the leader: it enforces the term
// rules, hands the snapshot to raftlog (which compacts or reseeds the log),
// restores the state machine, and fast-forwards applied/commit progress. The
// restore runs under r.mu, so it can't race the apply loop.
func (r *Raft) HandleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return InstallSnapshotReply{Term: r.currentTerm}
	}
	if args.Term < r.currentTerm {
		return InstallSnapshotReply{Term: r.currentTerm}
	}
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	} else if r.state != Follower {
		r.state = Follower
		r.notifyStateLocked()
	}
	r.leaderID = args.LeaderID
	r.resetElectionTimerLocked()

	// Ignore a snapshot we already cover.
	if args.LastIncludedIndex <= r.log.SnapshotIndex() || args.LastIncludedIndex <= r.commitIndex {
		return InstallSnapshotReply{Term: r.currentTerm}
	}

	if err := r.log.InstallSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data); err != nil {
		fmt.Printf("raft %d: install snapshot: %v\n", r.id, err)
		return InstallSnapshotReply{Term: r.currentTerm}
	}
	if r.restoreFn != nil {
		r.restoreFn(args.Data)
	}
	r.lastApplied = args.LastIncludedIndex
	if r.commitIndex < args.LastIncludedIndex {
		r.commitIndex = args.LastIncludedIndex
	}
	return InstallSnapshotReply{Term: r.currentTerm}
}

func minU(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxU(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
