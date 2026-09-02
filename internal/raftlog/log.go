// Package raftlog implements the crash-safe, append-only log that underpins the
// from-scratch Raft implementation, plus snapshot persistence for log compaction.
//
// The log is 1-indexed to match the Raft paper (index 0 is the empty sentinel).
// After a snapshot at index S, entries at index <= S are discarded and the log's
// first index becomes S+1; the snapshot's (index, term) is retained so a
// consistency check that references index S still resolves (TermAt(S) == snapTerm).
//
// On-disk record framing (big-endian): payloadLen(4) crc32(4) payload, where the
// payload is an encoded Entry. On open, records are replayed until the first one
// that is short or fails its CRC; that record and everything after it are
// discarded as a torn tail (the crash model for an fsync'd append-only file).
// Compaction rewrites the log via a temp file + atomic rename, so a crash during
// compaction leaves either the old (uncompacted) or new (compacted) log intact.
package raftlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

const recordHeaderLen = 8 // payloadLen(4) + crc32(4)

var crcTable = crc32.MakeTable(crc32.IEEE)

// Log is a crash-safe, append-only sequence of Entries with snapshot-based
// prefix compaction. Safe for concurrent use.
type Log struct {
	mu      sync.Mutex
	dir     string
	f       *os.File
	entries []Entry // in-memory, indices (snapIndex+1 .. snapIndex+len)
	offsets []int64 // offsets[i] is the byte offset of entries[i]'s record
	end     int64   // byte offset one past the last record

	snapIndex uint64 // lastIncludedIndex of the snapshot (0 = none)
	snapTerm  uint64 // lastIncludedTerm of the snapshot
}

func (l *Log) logPath() string { return filepath.Join(l.dir, "log") }

// Open opens (creating if necessary) the log under dir, loading any snapshot
// metadata, replaying log records, and truncating a torn trailing record.
func Open(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raftlog: mkdir: %w", err)
	}
	l := &Log{dir: dir}

	// Load snapshot metadata first: it sets where the log's first index begins.
	if idx, term, _, ok, err := readSnapshotFile(dir); err != nil {
		return nil, err
	} else if ok {
		l.snapIndex, l.snapTerm = idx, term
	}

	f, err := os.OpenFile(l.logPath(), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("raftlog: open log: %w", err)
	}
	l.f = f
	if err := l.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// recover replays the file, skipping records at/below the snapshot, stopping at
// the first torn/corrupt record and truncating there. If it skipped a stale
// prefix (a crash interrupted a previous compaction), it rewrites the file into
// canonical compacted form.
func (l *Log) recover() error {
	info, err := l.f.Stat()
	if err != nil {
		return fmt.Errorf("raftlog: stat: %w", err)
	}
	size := info.Size()

	var off int64
	skippedPrefix := false
	expected := l.snapIndex + 1
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
			break
		}
		e, err := decodeEntry(payload)
		if err != nil {
			break
		}
		if e.Index <= l.snapIndex {
			off = recEnd // covered by the snapshot; drop it
			skippedPrefix = true
			continue
		}
		if e.Index != expected {
			break // non-contiguous: garbage tail
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
	if skippedPrefix { // finish an interrupted compaction
		keep := append([]Entry(nil), l.entries...)
		return l.rewriteLocked(keep)
	}
	return nil
}

// --- indices ---

func (l *Log) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastIndexLocked()
}

func (l *Log) lastIndexLocked() uint64 {
	if len(l.entries) == 0 {
		return l.snapIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// firstIndexLocked is snapIndex+1 (the first index still present in the log).
func (l *Log) firstIndexLocked() uint64 { return l.snapIndex + 1 }

// SnapshotIndex / SnapshotTerm expose the snapshot boundary.
func (l *Log) SnapshotIndex() uint64 { l.mu.Lock(); defer l.mu.Unlock(); return l.snapIndex }
func (l *Log) SnapshotTerm() uint64  { l.mu.Lock(); defer l.mu.Unlock(); return l.snapTerm }

// SnapshotData reads the persisted snapshot bytes (nil if no snapshot).
func (l *Log) SnapshotData() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.snapIndex == 0 {
		return nil
	}
	_, _, data, ok, err := readSnapshotFile(l.dir)
	if err != nil || !ok {
		return nil
	}
	return data
}

func (l *Log) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// --- append ---

func (l *Log) Append(term uint64, typ EntryType, data []byte) (Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := Entry{Index: l.lastIndexLocked() + 1, Term: term, Type: typ, Data: data}
	if err := l.appendLocked(e); err != nil {
		return Entry{}, err
	}
	return e, nil
}

// AppendEntry appends a fully-formed entry (index must be exactly one past the
// current last index).
func (l *Log) AppendEntry(e Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if want := l.lastIndexLocked() + 1; e.Index != want {
		return fmt.Errorf("raftlog: non-contiguous append: got index %d, want %d", e.Index, want)
	}
	return l.appendLocked(e)
}

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
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("raftlog: fsync: %w", err)
	}
	l.entries = append(l.entries, e)
	l.offsets = append(l.offsets, l.end)
	l.end += int64(len(rec))
	return nil
}

// TruncateSuffix removes every entry with index >= from. from must be above the
// snapshot boundary (committed/snapshotted entries are never truncated).
func (l *Log) TruncateSuffix(from uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 || from > l.lastIndexLocked() {
		return nil
	}
	first := l.firstIndexLocked()
	if from <= first { // remove everything above the snapshot
		if err := l.f.Truncate(0); err != nil {
			return fmt.Errorf("raftlog: truncate all: %w", err)
		}
		if err := l.f.Sync(); err != nil {
			return err
		}
		l.entries = l.entries[:0]
		l.offsets = l.offsets[:0]
		l.end = 0
		return nil
	}
	pos := int(from - first)
	cut := l.offsets[pos]
	if err := l.f.Truncate(cut); err != nil {
		return fmt.Errorf("raftlog: truncate suffix: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.entries = l.entries[:pos]
	l.offsets = l.offsets[:pos]
	l.end = cut
	return nil
}

// --- reads ---

func (l *Log) At(index uint64) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 || index < l.firstIndexLocked() || index > l.lastIndexLocked() {
		return Entry{}, false
	}
	return l.entries[index-l.firstIndexLocked()], true
}

// TermAt returns the term of the entry at index. Index 0 is the empty sentinel
// (term 0); the snapshot boundary resolves to the snapshot's term; indices below
// the snapshot are gone (ok=false).
func (l *Log) TermAt(index uint64) (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index == l.snapIndex {
		return l.snapTerm, true // works for index==0 with no snapshot too
	}
	if index < l.firstIndexLocked() || index > l.lastIndexLocked() || len(l.entries) == 0 {
		return 0, false
	}
	return l.entries[index-l.firstIndexLocked()].Term, true
}

// From returns a copy of all entries with index >= lo (clamped to firstIndex).
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

// --- snapshots / compaction ---

// Snapshot records a state-machine snapshot up to `index` (which must be within
// the log) and discards every entry at or below it. Used by a node compacting
// its own log after applying entries.
func (l *Log) Snapshot(index, term uint64, data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index <= l.snapIndex {
		return nil // already snapshotted this far
	}
	if index > l.lastIndexLocked() {
		return fmt.Errorf("raftlog: snapshot index %d beyond last %d", index, l.lastIndexLocked())
	}
	if err := writeSnapshotFile(l.dir, index, term, data); err != nil {
		return err
	}
	drop := int(index - l.firstIndexLocked() + 1)
	keep := append([]Entry(nil), l.entries[drop:]...)
	l.snapIndex, l.snapTerm = index, term
	return l.rewriteLocked(keep)
}

// InstallSnapshot applies a snapshot received from the leader. If the log holds a
// matching entry at `index` (same term), the suffix after it is kept; otherwise
// the entire log is discarded and reseeded from the snapshot boundary.
func (l *Log) InstallSnapshot(index, term uint64, data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index <= l.snapIndex {
		return nil // stale
	}
	if err := writeSnapshotFile(l.dir, index, term, data); err != nil {
		return err
	}
	var keep []Entry
	if index >= l.firstIndexLocked() && index <= l.lastIndexLocked() &&
		l.entries[index-l.firstIndexLocked()].Term == term {
		drop := int(index - l.firstIndexLocked() + 1)
		keep = append([]Entry(nil), l.entries[drop:]...)
	} // else: divergent or far behind => discard all (keep == nil)
	l.snapIndex, l.snapTerm = index, term
	return l.rewriteLocked(keep)
}

// rewriteLocked replaces the log file with exactly `keep`, crash-safely: it
// writes a temp file, fsyncs, atomically renames it over the log, fsyncs the
// dir, then reopens the handle. Offsets/end are recomputed.
func (l *Log) rewriteLocked(keep []Entry) error {
	tmp := l.logPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raftlog: open log tmp: %w", err)
	}
	offsets := make([]int64, len(keep))
	var off int64
	for i, e := range keep {
		payload := make([]byte, e.encodedLen())
		e.encode(payload)
		rec := make([]byte, recordHeaderLen+len(payload))
		binary.BigEndian.PutUint32(rec[0:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(rec[4:8], crc32.Checksum(payload, crcTable))
		copy(rec[recordHeaderLen:], payload)
		if _, err := f.WriteAt(rec, off); err != nil {
			f.Close()
			return fmt.Errorf("raftlog: write log tmp: %w", err)
		}
		offsets[i] = off
		off += int64(len(rec))
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raftlog: sync log tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raftlog: close log tmp: %w", err)
	}
	if err := os.Rename(tmp, l.logPath()); err != nil {
		return fmt.Errorf("raftlog: rename log: %w", err)
	}
	if err := fsyncDir(l.dir); err != nil {
		return err
	}
	// Swap in the new file for future appends.
	if err := l.f.Close(); err != nil {
		return fmt.Errorf("raftlog: close old log: %w", err)
	}
	nf, err := os.OpenFile(l.logPath(), os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("raftlog: reopen log: %w", err)
	}
	l.f = nf
	l.entries = keep
	l.offsets = offsets
	l.end = off
	return nil
}

// --- lifecycle ---

func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Sync()
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
