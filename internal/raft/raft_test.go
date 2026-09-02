package raft

import (
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// errUnreachable simulates a dropped/partitioned RPC.
var errUnreachable = errors.New("unreachable")

// netHub is an in-memory network connecting several Raft nodes, with injectable
// partitions. Two nodes exchange RPCs only if they share a partition group.
type netHub struct {
	mu       sync.Mutex
	nodes    map[NodeID]*Raft
	group    map[NodeID]int // same group id => connected
	shutdown bool
}

func newHub() *netHub {
	return &netHub{nodes: map[NodeID]*Raft{}, group: map[NodeID]int{}}
}

func (h *netHub) register(r *Raft) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nodes[r.id] = r
	h.group[r.id] = 0
}

func (h *netHub) route(from, to NodeID) (*Raft, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.shutdown || h.group[from] != h.group[to] {
		return nil, false
	}
	return h.nodes[to], true
}

func (h *netHub) SendRequestVote(from, to NodeID, args RequestVoteArgs) (RequestVoteReply, error) {
	node, ok := h.route(from, to)
	if !ok {
		return RequestVoteReply{}, errUnreachable
	}
	return node.HandleRequestVote(args), nil
}

func (h *netHub) SendAppendEntries(from, to NodeID, args AppendEntriesArgs) (AppendEntriesReply, error) {
	node, ok := h.route(from, to)
	if !ok {
		return AppendEntriesReply{}, errUnreachable
	}
	return node.HandleAppendEntries(args), nil
}

func (h *netHub) heal() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id := range h.group {
		h.group[id] = 0
	}
}

// isolate cuts one node off from everyone else (the rest stay fully connected).
func (h *netHub) isolate(id NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for k := range h.group {
		h.group[k] = 0
	}
	h.group[id] = -int(id) - 1
}

func (h *netHub) stopRouting() {
	h.mu.Lock()
	h.shutdown = true
	h.mu.Unlock()
}

// testConfig uses short, well-separated timings: heartbeats (25ms) are far below
// the election window (80–160ms), so a healthy leader keeps its followers quiet,
// but a dead leader is noticed quickly.
func testConfig() Config {
	return Config{
		ElectionTimeoutMin: 80 * time.Millisecond,
		ElectionTimeoutMax: 160 * time.Millisecond,
		HeartbeatInterval:  25 * time.Millisecond,
		TickInterval:       5 * time.Millisecond,
	}
}

// makeCluster builds n nodes wired through one hub. Nodes are created but not
// started; the caller starts them. Cleanup stops routing, then every node.
func makeCluster(t *testing.T, n int) (*netHub, []*Raft) {
	t.Helper()
	hub := newHub()
	ids := make([]NodeID, n)
	for i := 0; i < n; i++ {
		ids[i] = NodeID(i + 1)
	}
	var nodes []*Raft
	for i := 0; i < n; i++ {
		var peers []NodeID
		for _, id := range ids {
			if id != ids[i] {
				peers = append(peers, id)
			}
		}
		r, err := New(ids[i], peers, hub, testConfig(), t.TempDir())
		if err != nil {
			t.Fatalf("New node %d: %v", ids[i], err)
		}
		hub.register(r)
		nodes = append(nodes, r)
	}
	t.Cleanup(func() {
		hub.stopRouting()
		for _, r := range nodes {
			r.Stop()
		}
	})
	return hub, nodes
}

func startAll(nodes []*Raft) {
	for _, r := range nodes {
		r.Start()
	}
}

// leaders returns the (id, term) of every node currently in the Leader state.
func leaders(nodes []*Raft) []struct {
	id   NodeID
	term uint64
} {
	var out []struct {
		id   NodeID
		term uint64
	}
	for _, r := range nodes {
		if st, term := r.Report(); st == Leader {
			out = append(out, struct {
				id   NodeID
				term uint64
			}{r.id, term})
		}
	}
	return out
}

// waitFor polls cond every 5ms until it's true or the timeout elapses.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func TestSingleLeaderElected(t *testing.T) {
	_, nodes := makeCluster(t, 3)
	startAll(nodes)

	if !waitFor(3*time.Second, func() bool { return len(leaders(nodes)) == 1 }) {
		t.Fatalf("no single leader elected; leaders=%v", leaders(nodes))
	}
	ls := leaders(nodes)
	leaderTerm := ls[0].term

	// The other two must be followers, and everyone must agree on the term.
	followers := 0
	for _, r := range nodes {
		st, term := r.Report()
		if st == Follower {
			followers++
		}
		if term != leaderTerm {
			t.Fatalf("node %d term %d != leader term %d", r.id, term, leaderTerm)
		}
	}
	if followers != 2 {
		t.Fatalf("want 2 followers, got %d", followers)
	}
}

func TestLeaderReelectedAfterFailure(t *testing.T) {
	hub, nodes := makeCluster(t, 3)
	startAll(nodes)

	if !waitFor(3*time.Second, func() bool { return len(leaders(nodes)) == 1 }) {
		t.Fatal("no initial leader")
	}
	old := leaders(nodes)[0]

	// Kill the leader: partition it off. The remaining two must elect a new
	// leader at a strictly higher term.
	hub.isolate(old.id)
	ok := waitFor(3*time.Second, func() bool {
		for _, l := range leaders(nodes) {
			if l.id != old.id && l.term > old.term {
				return true
			}
		}
		return false
	})
	if !ok {
		t.Fatalf("no new leader after isolating %d; leaders=%v", old.id, leaders(nodes))
	}

	// Reconnect the old leader: it must discover the higher term and step down,
	// leaving exactly one leader in the cluster.
	hub.heal()
	if !waitFor(3*time.Second, func() bool { return len(leaders(nodes)) == 1 }) {
		t.Fatalf("old leader did not step down; leaders=%v", leaders(nodes))
	}
}

func TestNoLeaderWithoutMajority(t *testing.T) {
	hub, nodes := makeCluster(t, 3)
	// Isolate every node from every other: no one can reach a majority.
	hub.mu.Lock()
	for id := range hub.group {
		hub.group[id] = -int(id) - 1
	}
	hub.mu.Unlock()
	startAll(nodes)

	// Give them ample time to churn through elections; none may win.
	time.Sleep(1 * time.Second)
	if ls := leaders(nodes); len(ls) != 0 {
		t.Fatalf("a leader emerged without majority: %v", ls)
	}

	// Heal the partition; a leader must now emerge (liveness).
	hub.heal()
	if !waitFor(3*time.Second, func() bool { return len(leaders(nodes)) == 1 }) {
		t.Fatal("no leader after healing the partition")
	}
}

// TestSafetyOneLeaderPerTerm is the core safety property: under continuous
// random partitioning, no two nodes are ever leader in the same term. A checker
// samples every 3ms and records term -> leader id; a second leader for a term
// already claimed by someone else is a safety violation. It also asserts
// liveness (a leader reappears once healed) and that elections actually churned.
func TestSafetyOneLeaderPerTerm(t *testing.T) {
	const n = 5
	hub, nodes := makeCluster(t, n)
	startAll(nodes)

	stop := make(chan struct{})
	var (
		vmu      sync.Mutex
		termHead = map[uint64]NodeID{}
		violated string
		maxTerm  uint64
	)

	// Safety checker.
	var checker sync.WaitGroup
	checker.Add(1)
	go func() {
		defer checker.Done()
		tk := time.NewTicker(3 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				for _, l := range leaders(nodes) {
					vmu.Lock()
					if l.term > maxTerm {
						maxTerm = l.term
					}
					if prev, ok := termHead[l.term]; ok && prev != l.id {
						if violated == "" {
							violated = itoa(l.term, prev, l.id)
						}
					} else {
						termHead[l.term] = l.id
					}
					vmu.Unlock()
				}
			}
		}
	}()

	// Chaos: repartition randomly for ~3s.
	rng := rand.New(rand.NewSource(12345))
	chaosDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(chaosDeadline) {
		switch rng.Intn(3) {
		case 0:
			hub.heal()
		case 1:
			hub.isolate(NodeID(1 + rng.Intn(n)))
		case 2:
			hub.mu.Lock()
			for id := range hub.group {
				hub.group[id] = 1 + rng.Intn(2) // random split into two groups
			}
			hub.mu.Unlock()
		}
		time.Sleep(time.Duration(60+rng.Intn(60)) * time.Millisecond)
	}

	// Heal and require a single stable leader (liveness after chaos).
	hub.heal()
	live := waitFor(5*time.Second, func() bool { return len(leaders(nodes)) == 1 })

	close(stop)
	checker.Wait()

	vmu.Lock()
	defer vmu.Unlock()
	if violated != "" {
		t.Fatalf("SAFETY VIOLATION: two leaders in the same term: %s", violated)
	}
	if !live {
		t.Fatalf("no single leader after healing; leaders=%v", leaders(nodes))
	}
	if maxTerm < 2 {
		t.Fatalf("elections never churned (maxTerm=%d); test wasn't meaningful", maxTerm)
	}
	t.Logf("safety held across chaos; reached term %d", maxTerm)
}

// itoa formats a violation message without pulling in fmt at the hot path.
func itoa(term uint64, a, b NodeID) string {
	return "term=" + u(term) + " leaderA=" + u(uint64(a)) + " leaderB=" + u(uint64(b))
}

func u(x uint64) string {
	if x == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for x > 0 {
		i--
		buf[i] = byte('0' + x%10)
		x /= 10
	}
	return string(buf[i:])
}

// noopNet errors on every send, for driving a single node through its handlers
// deterministically (no background elections interfering).
type noopNet struct{}

func (noopNet) SendRequestVote(NodeID, NodeID, RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{}, errUnreachable
}
func (noopNet) SendAppendEntries(NodeID, NodeID, AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, errUnreachable
}

// TestPersistedVoteSurvivesRestart proves currentTerm and votedFor are durable:
// after a node votes in term 5 and is recreated from the same directory, it must
// remember the term and refuse to vote for a different candidate in that term.
func TestPersistedVoteSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	r, err := New(1, []NodeID{2, 3}, noopNet{}, testConfig(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if reply := r.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: 2}); !reply.VoteGranted {
		t.Fatal("expected to grant vote to candidate 2 in term 5")
	}
	r.Stop()

	// Restart from the same directory.
	r2, err := New(1, []NodeID{2, 3}, noopNet{}, testConfig(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Stop()

	if _, term := r2.Report(); term != 5 {
		t.Fatalf("after restart term = %d, want 5 (not persisted)", term)
	}
	// Already voted for 2 in term 5 => a different candidate must be refused.
	if reply := r2.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: 3}); reply.VoteGranted {
		t.Fatal("granted a second vote in term 5 — votedFor was not persisted")
	}
	// The same candidate re-asking in term 5 is fine (idempotent).
	if reply := r2.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: 2}); !reply.VoteGranted {
		t.Fatal("should still grant the same candidate its vote in term 5")
	}
	// A stale-term candidate is rejected.
	if reply := r2.HandleRequestVote(RequestVoteArgs{Term: 4, CandidateID: 3}); reply.VoteGranted {
		t.Fatal("granted a vote for a stale term")
	}
}
