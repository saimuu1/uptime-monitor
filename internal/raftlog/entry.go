package raftlog

import (
	"encoding/binary"
	"fmt"
)

// EntryType distinguishes ordinary state-machine commands from internal Raft
// bookkeeping entries. Only EntryCommand is used in Phase 0; the others exist so
// the on-disk format doesn't change when later phases need them.
type EntryType uint8

const (
	// EntryCommand is a normal state-machine command (the default).
	EntryCommand EntryType = iota
	// EntryNoop is a leader's no-op, appended on election so the leader has an
	// entry from its own term to commit (the Figure-8 commit rule, Phase 2).
	EntryNoop
	// EntryConfig carries a cluster membership change (Phase 6).
	EntryConfig
)

// entryHeaderLen is the fixed-size prefix of an encoded entry:
// index(8) + term(8) + type(1) + dataLen(4).
const entryHeaderLen = 8 + 8 + 1 + 4

// Entry is a single record in the replicated log. The (Index, Term) pair
// identifies it uniquely across the cluster and is what every Raft consistency
// check compares.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// encodedLen is the number of bytes encode produces for this entry.
func (e Entry) encodedLen() int { return entryHeaderLen + len(e.Data) }

// encode serialises the entry into buf, which must be at least encodedLen long.
// Layout is big-endian: index(8) term(8) type(1) dataLen(4) data(dataLen).
func (e Entry) encode(buf []byte) {
	binary.BigEndian.PutUint64(buf[0:8], e.Index)
	binary.BigEndian.PutUint64(buf[8:16], e.Term)
	buf[16] = byte(e.Type)
	binary.BigEndian.PutUint32(buf[17:21], uint32(len(e.Data)))
	copy(buf[21:], e.Data)
}

// decodeEntry parses the payload written by encode. It returns an error if the
// buffer is too short or the declared data length doesn't match — either means
// the record is torn or corrupt.
func decodeEntry(b []byte) (Entry, error) {
	if len(b) < entryHeaderLen {
		return Entry{}, fmt.Errorf("raftlog: entry payload too short (%d bytes)", len(b))
	}
	e := Entry{
		Index: binary.BigEndian.Uint64(b[0:8]),
		Term:  binary.BigEndian.Uint64(b[8:16]),
		Type:  EntryType(b[16]),
	}
	dataLen := binary.BigEndian.Uint32(b[17:21])
	if int(dataLen) != len(b)-entryHeaderLen {
		return Entry{}, fmt.Errorf("raftlog: entry data length mismatch (hdr=%d actual=%d)", dataLen, len(b)-entryHeaderLen)
	}
	if dataLen > 0 {
		e.Data = append([]byte(nil), b[entryHeaderLen:]...)
	}
	return e, nil
}
