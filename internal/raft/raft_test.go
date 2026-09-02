package raft

import (
	"bytes"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/saimuu1/uptime-monitor/internal/raftlog"
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

func (h *netHub) SendInstallSnapshot(from, to NodeID, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	node, ok := h.route(from, to)
	if !ok {
		return InstallSnapshotReply{}, errUnreachable
	}
	return node.HandleInstallSnapshot(args), nil
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

	// Wait for a STABLE configuration: exactly one leader, the other two
	// followers, and everyone agreeing on the leader's term. A brief transient
	// where a third node is still mid-election is normal Raft, so we poll for the
	// settled state rather than asserting on the first instant a leader appears.
	stable := func() bool {
		ls := leaders(nodes)
		if len(ls) != 1 {
			return false
		}
		followers := 0
		for _, r := range nodes {
			st, term := r.Report()
			if st == Follower {
				followers++
			}
			if term != ls[0].term {
				return false
			}
		}
		return followers == 2
	}
	if !waitFor(5*time.Second, stable) {
		t.Fatalf("no stable single-leader configuration; leaders=%v", leaders(nodes))
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
func (noopNet) SendInstallSnapshot(NodeID, NodeID, InstallSnapshotArgs) (InstallSnapshotReply, error) {
	return InstallSnapshotReply{}, errUnreachable
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

// --- Phase 2: replication + commitment ---

// seedLog appends entries with the given terms directly (test-only), building a
// specific log shape without running the node.
func seedLog(t *testing.T, r *Raft, terms []uint64) {
	t.Helper()
	for i, term := range terms {
		e := raftlog.Entry{Index: uint64(i + 1), Term: term, Type: raftlog.EntryCommand, Data: []byte{byte('a' + i)}}
		if err := r.log.AppendEntry(e); err != nil {
			t.Fatalf("seed entry %d: %v", i+1, err)
		}
	}
}

// TestAppendEntriesRepairsConflict checks the follower side of replication: the
// consistency check, the accelerated conflict hints, and conflict truncation.
func TestAppendEntriesRepairsConflict(t *testing.T) {
	// Case 1: conflicting tail is truncated and replaced.
	f, err := New(1, []NodeID{2, 3}, noopNet{}, testConfig(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Stop()
	seedLog(t, f, []uint64{1, 1, 2}) // follower has term-2 entry at index 3
	f.currentTerm = 2

	reply := f.HandleAppendEntries(AppendEntriesArgs{
		Term: 3, LeaderID: 2, PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []raftlog.Entry{{Index: 3, Term: 3, Type: raftlog.EntryCommand, Data: []byte("c")}},
	})
	if !reply.Success {
		t.Fatalf("expected success, got %+v", reply)
	}
	if e, ok := f.log.At(3); !ok || e.Term != 3 {
		t.Fatalf("index 3 not repaired: %+v,%v", e, ok)
	}
	if f.log.LastIndex() != 3 {
		t.Fatalf("LastIndex = %d, want 3", f.log.LastIndex())
	}

	// Case 2: log too short => ConflictTerm 0, ConflictIndex = our end + 1.
	f2, _ := New(1, []NodeID{2, 3}, noopNet{}, testConfig(), t.TempDir())
	defer f2.Stop()
	f2.currentTerm = 1
	rep2 := f2.HandleAppendEntries(AppendEntriesArgs{Term: 1, LeaderID: 2, PrevLogIndex: 5, PrevLogTerm: 1})
	if rep2.Success || rep2.ConflictTerm != 0 || rep2.ConflictIndex != 1 {
		t.Fatalf("short-log hint wrong: %+v", rep2)
	}

	// Case 3: term mismatch => ConflictTerm and first index of that term.
	f3, _ := New(1, []NodeID{2, 3}, noopNet{}, testConfig(), t.TempDir())
	defer f3.Stop()
	seedLog(t, f3, []uint64{1, 1, 2})
	f3.currentTerm = 2
	rep3 := f3.HandleAppendEntries(AppendEntriesArgs{Term: 5, LeaderID: 2, PrevLogIndex: 3, PrevLogTerm: 5})
	if rep3.Success || rep3.ConflictTerm != 2 || rep3.ConflictIndex != 3 {
		t.Fatalf("term-conflict hint wrong: %+v", rep3)
	}
}

// --- a replicated key-value state machine for the convergence test ---

type kvSM struct {
	mu sync.Mutex
	m  map[string]string
}

func newKV() *kvSM { return &kvSM{m: map[string]string{}} }

func (s *kvSM) apply(msg ApplyMsg) {
	if msg.Type != raftlog.EntryCommand {
		return // skip leader no-ops
	}
	k, v := decodePut(msg.Data)
	s.mu.Lock()
	s.m[k] = v
	s.mu.Unlock()
}

func (s *kvSM) get(k string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok
}

func (s *kvSM) equalTo(exp map[string]string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.m) != len(exp) {
		return false
	}
	for k, v := range exp {
		if s.m[k] != v {
			return false
		}
	}
	return true
}

func encodePut(k, v string) []byte { return []byte(k + "\x00" + v) }

func decodePut(b []byte) (string, string) {
	i := bytes.IndexByte(b, 0)
	if i < 0 {
		return string(b), ""
	}
	return string(b[:i]), string(b[i+1:])
}

func findLeader(nodes []*Raft) *Raft {
	var best *Raft
	var bestTerm uint64
	for _, r := range nodes {
		if st, term := r.Report(); st == Leader && (best == nil || term > bestTerm) {
			best, bestTerm = r, term
		}
	}
	return best
}

// TestReplicationConverges drives a replicated KV store through ~200 committed
// writes while intermittently isolating a single node (keeping a majority, so
// progress never stalls). At the end it heals the cluster and requires every
// node's state machine to equal the expected map and every node's log to be
// byte-identical — Raft's State Machine Safety property, end to end.
func TestReplicationConverges(t *testing.T) {
	const n, ops = 5, 200
	_, nodes := makeCluster(t, n)
	sms := make([]*kvSM, n)
	smOf := map[*Raft]*kvSM{}
	for i, r := range nodes {
		sms[i] = newKV()
		smOf[r] = sms[i]
		r.SetApply(sms[i].apply)
	}
	hub := nodes[0].net.(*netHub)
	startAll(nodes)

	if !waitFor(3*time.Second, func() bool { return findLeader(nodes) != nil }) {
		t.Fatal("no leader elected")
	}

	rng := rand.New(rand.NewSource(7))
	expected := map[string]string{}
	var isolated NodeID
	isolatedUntil := time.Time{}

	commitOnLeader := func(key, val string) bool {
		l := findLeader(nodes)
		if l == nil {
			return false
		}
		if _, _, ok := l.Propose(encodePut(key, val)); !ok {
			return false
		}
		// Wait until the proposing leader has applied it (i.e. it committed).
		sm := smOf[l]
		return waitFor(2*time.Second, func() bool {
			got, ok := sm.get(key)
			return ok && got == val
		})
	}

	for i := 0; i < ops; i++ {
		// Chaos: heal an expired isolation, or occasionally isolate one node.
		if isolated != 0 && time.Now().After(isolatedUntil) {
			hub.heal()
			isolated = 0
		}
		if isolated == 0 && rng.Intn(6) == 0 {
			isolated = NodeID(1 + rng.Intn(n))
			hub.isolate(isolated)
			isolatedUntil = time.Now().Add(200 * time.Millisecond)
		}

		key, val := "k"+u(uint64(i)), "v"+u(uint64(i))
		deadline := time.Now().Add(8 * time.Second)
		for !commitOnLeader(key, val) {
			if time.Now().After(deadline) {
				t.Fatalf("op %d (%s) never committed", i, key)
			}
			if isolated != 0 { // don't get stuck behind our own partition
				hub.heal()
				isolated = 0
			}
			time.Sleep(15 * time.Millisecond)
		}
		expected[key] = val
	}

	// Heal and require every node's state machine to converge to expected.
	hub.heal()
	converged := waitFor(10*time.Second, func() bool {
		for _, s := range sms {
			if !s.equalTo(expected) {
				return false
			}
		}
		return true
	})
	if !converged {
		for i, s := range sms {
			s.mu.Lock()
			t.Logf("node %d has %d/%d keys", nodes[i].id, len(s.m), len(expected))
			s.mu.Unlock()
		}
		t.Fatal("state machines did not converge to the expected map")
	}

	// Stronger: every node's committed log prefix must be byte-identical.
	minApplied := nodes[0].LastApplied()
	for _, r := range nodes[1:] {
		if a := r.LastApplied(); a < minApplied {
			minApplied = a
		}
	}
	for idx := uint64(1); idx <= minApplied; idx++ {
		want, _ := nodes[0].log.At(idx)
		for _, r := range nodes[1:] {
			got, ok := r.log.At(idx)
			if !ok || got.Term != want.Term || got.Type != want.Type || !bytes.Equal(got.Data, want.Data) {
				t.Fatalf("log divergence at index %d: node1=%+v node%d=%+v", idx, want, r.id, got)
			}
		}
	}
	t.Logf("converged: %d ops applied identically across %d nodes", len(expected), n)
}

// --- Phase 3: snapshotting + log compaction ---

func (s *kvSM) serialize() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b bytes.Buffer
	for k, v := range s.m {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

func (s *kvSM) restore(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[string]string{}
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		if i := bytes.IndexByte(line, '='); i >= 0 {
			m[string(line[:i])] = string(line[i+1:])
		}
	}
	s.m = m
}

func (s *kvSM) size() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.m) }

// TestSnapshotCatchUp isolates a follower while the majority commits enough
// writes that the leader compacts its log past the follower's position. When the
// follower rejoins, the entries it needs are gone, so the leader must catch it up
// with an InstallSnapshot; the follower restores its state machine from the
// snapshot and converges.
func TestSnapshotCatchUp(t *testing.T) {
	const n = 3
	hub, nodes := makeCluster(t, n)
	sms := make([]*kvSM, n)
	smOf := map[*Raft]*kvSM{}
	for i, r := range nodes {
		sms[i] = newKV()
		smOf[r] = sms[i]
		r.SetApply(sms[i].apply)
		r.SetSnapshot(sms[i].serialize, sms[i].restore, 10) // compact every 10 entries
	}
	startAll(nodes)

	if !waitFor(3*time.Second, func() bool { return findLeader(nodes) != nil }) {
		t.Fatal("no leader")
	}
	leader := findLeader(nodes)

	// Isolate a follower.
	var victim *Raft
	for _, r := range nodes {
		if r != leader {
			victim = r
			break
		}
	}
	hub.isolate(victim.id)

	// Commit enough writes that the leader compacts past the victim.
	expected := map[string]string{}
	for i := 0; i < 60; i++ {
		key, val := "k"+u(uint64(i)), "v"+u(uint64(i))
		deadline := time.Now().Add(8 * time.Second)
		for {
			l := findLeader(nodes)
			if l != nil && l != victim {
				if _, _, ok := l.Propose(encodePut(key, val)); ok {
					sm := smOf[l]
					if waitFor(2*time.Second, func() bool { g, ok := sm.get(key); return ok && g == val }) {
						break
					}
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("op %d never committed", i)
			}
			time.Sleep(15 * time.Millisecond)
		}
		expected[key] = val
	}

	if leader.log.SnapshotIndex() == 0 {
		t.Fatal("leader never compacted its log")
	}

	// Heal the victim: it must catch up via InstallSnapshot (its needed entries
	// were compacted away) and converge.
	hub.heal()
	caughtUp := waitFor(8*time.Second, func() bool {
		return victim.log.SnapshotIndex() > 0 && smOf[victim].equalTo(expected)
	})
	if !caughtUp {
		t.Fatalf("victim didn't catch up via snapshot: snapIndex=%d keys=%d/%d",
			victim.log.SnapshotIndex(), smOf[victim].size(), len(expected))
	}
	t.Logf("victim caught up via snapshot to index %d (%d keys)", victim.log.SnapshotIndex(), len(expected))
}
