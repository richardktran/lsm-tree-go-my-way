package algorithm

import "github.com/richardktran/lsm-tree-go-my-way/internal/kv"

var _ SortedList = (*SkipList)(nil)

type SkipList struct{}

// Clone implements SortedList.
func (s *SkipList) Clone() SortedList {
	panic("unimplemented")
}

// Delete implements SortedList.
func (s *SkipList) Delete(key kv.Key) {
	panic("unimplemented")
}

// Get implements SortedList.
func (s *SkipList) Get(key kv.Key) (kv.Value, bool) {
	panic("unimplemented")
}

// GetAll implements SortedList.
func (s *SkipList) GetAll() []kv.Record {
	panic("unimplemented")
}

// Set implements SortedList.
func (s *SkipList) Set(key kv.Key, value kv.Value) {
	panic("unimplemented")
}

// Size implements SortedList.
func (s *SkipList) Size() int {
	panic("unimplemented")
}
