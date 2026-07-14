package algorithm

import (
	"math/rand"

	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
)

var _ SortedList = (*SkipList)(nil)

const (
	maxLevel    = 16
	probability = 0.5
)

type skipListNode struct {
	key   kv.Key
	value kv.Value
	next  []*skipListNode
}

func newSkipListNode(key kv.Key, value kv.Value, level int) *skipListNode {
	return &skipListNode{
		key:   key,
		value: value,
		next:  make([]*skipListNode, level),
	}
}

// SkipList is a probabilistic sorted data structure providing O(log n) average
// complexity for Get, Set, and Delete — replacing the O(n log n) SortedArray.
type SkipList struct {
	head  *skipListNode // sentinel head; its key is never compared
	level int           // highest level currently in use (1-indexed)
	size  int           // total byte size: sum of len(key)+len(value) for all nodes
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:  newSkipListNode("", nil, maxLevel),
		level: 1,
	}
}

// BuildSkipList constructs a SkipList from a pre-existing slice of records.
// Input order does not matter; all records are inserted individually.
func BuildSkipList(data []kv.Record) *SkipList {
	s := NewSkipList()
	for _, r := range data {
		s.Set(r.Key, r.Value)
	}
	return s
}

// randomLevel picks a new node height using the geometric distribution with p=0.5.
func (s *SkipList) randomLevel() int {
	level := 1
	for level < maxLevel && rand.Float64() < probability {
		level++
	}
	return level
}

// Get returns the value stored for key and true, or ("", false) if not present.
// A node storing a nil value (tombstone) returns (nil, true).
func (s *SkipList) Get(key kv.Key) (kv.Value, bool) {
	current := s.head
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
	}
	candidate := current.next[0]
	if candidate != nil && candidate.key == key {
		return candidate.value, true
	}
	return kv.Value(""), false
}

// Set inserts or updates the value for key in O(log n) average time.
func (s *SkipList) Set(key kv.Key, value kv.Value) {
	// update[i] is the rightmost node at level i whose next key < key
	update := make([]*skipListNode, maxLevel)
	current := s.head

	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current
	}

	candidate := current.next[0]
	if candidate != nil && candidate.key == key {
		// Key already exists: update value in place, adjust byte size.
		s.size -= kv.Record{Key: key, Value: candidate.value}.Size()
		s.size += kv.Record{Key: key, Value: value}.Size()
		candidate.value = value
		return
	}

	// New key: pick a random level and splice in the new node.
	newLevel := s.randomLevel()
	if newLevel > s.level {
		for i := s.level; i < newLevel; i++ {
			update[i] = s.head
		}
		s.level = newLevel
	}

	node := newSkipListNode(key, value, newLevel)
	for i := 0; i < newLevel; i++ {
		node.next[i] = update[i].next[i]
		update[i].next[i] = node
	}

	s.size += kv.Record{Key: key, Value: value}.Size()
}

// Delete removes the node with the given key in O(log n) average time.
func (s *SkipList) Delete(key kv.Key) {
	update := make([]*skipListNode, maxLevel)
	current := s.head

	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current
	}

	target := current.next[0]
	if target == nil || target.key != key {
		return // key not present
	}

	for i := 0; i < s.level; i++ {
		if update[i].next[i] != target {
			break
		}
		update[i].next[i] = target.next[i]
	}

	s.size -= kv.Record{Key: key, Value: target.value}.Size()

	// Shrink level if top levels are now empty.
	for s.level > 1 && s.head.next[s.level-1] == nil {
		s.level--
	}
}

// Size returns the total byte footprint of all key-value pairs.
func (s *SkipList) Size() int {
	return s.size
}

// GetAll returns all records in sorted key order by traversing level 0.
func (s *SkipList) GetAll() []kv.Record {
	var records []kv.Record
	for node := s.head.next[0]; node != nil; node = node.next[0] {
		records = append(records, kv.Record{Key: node.key, Value: node.value})
	}
	return records
}

// Clone returns an independent deep copy of the skip list.
func (s *SkipList) Clone() SortedList {
	dst := NewSkipList()
	for node := s.head.next[0]; node != nil; node = node.next[0] {
		var valueCopy kv.Value
		if node.value != nil {
			valueCopy = make(kv.Value, len(node.value))
			copy(valueCopy, node.value)
		}
		dst.Set(node.key, valueCopy)
	}
	return dst
}
