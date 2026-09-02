// Package raftlog implements the crash-safe, append-only log that underpins the
// from-scratch Raft implementation. It deliberately knows nothing about the Raft
// algorithm: it stores a durable, ordered sequence of Entry records, survives a
// torn write from a crash, and supports the suffix truncation Raft needs when a
// follower's log conflicts with the leader's. Persistent Raft "hard state" (the
// current term and vote) lives here too — it shares the same fsync-before-ack
// durability requirement (see state.go).
//
// The log is 1-indexed to match the Raft paper: index 0 is the empty sentinel.
// Until snapshotting compacts the prefix (Phase 3), the first real entry is
// always index 1 and indices are contiguous.
//
// On-disk record framing (big-endian): payloadLen(4) crc32(4) payload, where
// payload is an encoded Entry. On open, records are replayed until the first one
// that is short or fails its CRC; that record and everything after it are
// discarded as a torn tail. This assumes the only corruption a crash produces is
// an incomplete trailing write — the correct model for an append-only file that
// fsyncs each whole record before acknowledging it.
package raftlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// recordHeaderLen is the framing prefix on disk: payloadLen(4) + crc32(4).
const recordHeaderLen = 8

var crcTable = crc32.MakeTable(crc32.IEEE)

// Log is an append-only, crash-safe sequence of Entries. It is safe for
// concurrent use.
type Log struct {
	mu      sync.Mutex
	dir     string
	f       *os.File
	entries []Entry // in-memory copy; the log stays small (compacted by snapshots)
	offsets []int64 // offsets[i] is the byte offset of entries[i]'s record
	end     int64   // byte offset one past the last intact record
}

// Open opens (creating if necessary) the log under dir, replaying existing
// records into memory and truncating any torn trailing record.
func Open(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raftlog: mkdir: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "log"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("raftlog: open log: %w", err)
	}
	l := &Log{dir: dir, f: f}
	if err := l.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// recover replays the file, stopping at the first torn/corrupt record and
// truncating the file there so the next append starts from a clean boundary.
func (l *Log) recover() error {
	info, err := l.f.Stat()
	if err != nil {
		return fmt.Errorf("raftlog: stat: %w", err)
	}
	size := info.Size()

	var off int64
	expected := uint64(1) // indices must be contiguous from 1 (no compaction yet)
	hdr := make([]byte, recordHeaderLen)
	for off < size {
		if off+recordHeaderLen > size {
			break // torn header
		}
		if _, err := l.f.ReadAt(hdr, off); err != nil {
			return fmt.Errorf("raftlog: read header @%d: %w", off, err)
		}
		payloadLen := binary.BigEndian.Uint32(hdr[0:4])
		wantCRC := binary.BigEndian.Uint32(hdr[4:8])
		recEnd := off + recordHeaderLen + int64(payloadLen)
		if recEnd > size {
			break // torn payload
		}
		payload := make([]byte, payloadLen)
		if _, err := l.f.ReadAt(payload, off+recordHeaderLen); err != nil {
			return fmt.Errorf("raftlog: read payload @%d: %w", off, err)
		}
		if crc32.Checksum(payload, crcTable) != wantCRC {
			break // torn / corrupt trailing record
		}
		e, err := decodeEntry(payload)
		if err != nil || e.Index != expected {
			break // undecodable or non-contiguous: treat as garbage tail
		}
		l.entries = append(l.entries, e)
		l.offsets = append(l.offsets, off)
		off = recEnd
		expected++
	}
	if off < size { // discard the torn tail
		if err := l.f.Truncate(off); err != nil {
			return fmt.Errorf("raftlog: truncate torn tail: %w", err)
		}
		if err := l.f.Sync(); err != nil {
			return fmt.Errorf("raftlog: sync after truncate: %w", err)
		}
	}
	l.end = off
	return nil
}

// LastIndex returns the index of the last entry, or 0 if the log is empty.
func (l *Log) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastIndexLocked()
}

func (l *Log) lastIndexLocked() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// firstIndexLocked is 1 until snapshotting truncates the prefix (Phase 3).
func (l *Log) firstIndexLocked() uint64 {
	if len(l.entries) == 0 {
		return 1
	}
	return l.entries[0].Index
}

// Len returns the number of entries currently in the log.
func (l *Log) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// Append adds one entry with the given term/type/data, assigning it the next
// index. It returns the stored Entry only once it is durable on disk.
func (l *Log) Append(term uint64, typ EntryType, data []byte) (Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := Entry{Index: l.lastIndexLocked() + 1, Term: term, Type: typ, Data: data}
	if err := l.appendLocked(e); err != nil {
		return Entry{}, err
	}
	return e, nil
}

// AppendEntry appends a fully-formed entry verbatim (index and term already
// assigned). This is what the leader-driven replication path in Phase 2 uses.
// The entry's index must be exactly one past the current last index.
func (l *Log) AppendEntry(e Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if want := l.lastIndexLocked() + 1; e.Index != want {
		return fmt.Errorf("raftlog: non-contiguous append: got index %d, want %d", e.Index, want)
	}
	return l.appendLocked(e)
}

// appendLocked frames, writes, and fsyncs a single record, then updates the
// in-memory view. Caller holds l.mu.
func (l *Log) appendLocked(e Entry) error {
	payload := make([]byte, e.encodedLen())
	e.encode(payload)

	rec := make([]byte, recordHeaderLen+len(payload))
	binary.BigEndian.PutUint32(rec[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(rec[4:8], crc32.Checksum(payload, crcTable))
	copy(rec[recordHeaderLen:], payload)

	if _, err := l.f.WriteAt(rec, l.end); err != nil {
		return fmt.Errorf("raftlog: write @%d: %w", l.end, err)
	}
	if err := l.f.Sync(); err != nil { // durable before we acknowledge
		return fmt.Errorf("raftlog: fsync: %w", err)
	}
	l.entries = append(l.entries, e)
	l.offsets = append(l.offsets, l.end)
	l.end += int64(len(rec))
	return nil
}

// TruncateSuffix removes every entry with index >= from. It's how a follower
// drops conflicting tail entries before accepting the leader's (Phase 2).
// Truncating at or below the first index empties the log; a from past the last
// index is a no-op.
func (l *Log) TruncateSuffix(from uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return nil
	}
	first := l.firstIndexLocked()
	if from <= first {
		return l.truncateToLocked(0, 0)
	}
	if from > l.lastIndexLocked() {
		return nil
	}
	pos := int(from - first) // entries[pos] is the first to drop
	return l.truncateToLocked(pos, l.offsets[pos])
}

// truncateToLocked cuts the file at byteOffset and keeps entries[:keep].
func (l *Log) truncateToLocked(keep int, byteOffset int64) error {
	if err := l.f.Truncate(byteOffset); err != nil {
		return fmt.Errorf("raftlog: truncate: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("raftlog: sync after truncate: %w", err)
	}
	l.entries = l.entries[:keep]
	l.offsets = l.offsets[:keep]
	l.end = byteOffset
	return nil
}

// At returns the entry at index, or ok=false if it's out of range.
func (l *Log) At(index uint64) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return Entry{}, false
	}
	first := l.firstIndexLocked()
	if index < first || index > l.lastIndexLocked() {
		return Entry{}, false
	}
	return l.entries[index-first], true
}

// TermAt returns the term of the entry at index. Index 0 is the empty sentinel
// with term 0, matching how Raft treats prevLogTerm for an empty log.
func (l *Log) TermAt(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}
	e, ok := l.At(index)
	if !ok {
		return 0, false
	}
	return e.Term, true
}

// From returns a copy of all entries with index >= lo (what the leader ships to
// a lagging follower). An empty slice means the follower is up to date.
func (l *Log) From(lo uint64) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return nil
	}
	first := l.firstIndexLocked()
	if lo < first {
		lo = first
	}
	if lo > l.lastIndexLocked() {
		return nil
	}
	src := l.entries[lo-first:]
	out := make([]Entry, len(src))
	copy(out, src)
	return out
}

// Sync flushes buffered writes to stable storage. Appends already fsync, so this
// is mainly for callers that want an explicit barrier.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Sync()
}

// Close closes the underlying file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
