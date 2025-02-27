package algorithm

import (
	"sort"

	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
)

type Record struct {
	key   kv.Key
	value kv.Value
}

type SortedList struct {
	data []Record
}

func NewSortedList() *SortedList {
	return &SortedList{
		data: make([]Record, 0),
	}
}

func (s *SortedList) Insert(key kv.Key, value kv.Value) {
	_, exists := s.Get(key)
	if exists {
		s.Delete(key)
	}

	s.data = append(s.data, Record{key: key, value: value})
	s.Sort()
}

func (s *SortedList) Get(key kv.Key) (kv.Value, bool) {
	// binary search for the key
	low := 0
	high := len(s.data) - 1

	for low <= high {
		mid := low + (high-low)/2

		if s.data[mid].key == key {
			return s.data[mid].value, true
		}

		if s.data[mid].key < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return "", false
}

func (s *SortedList) Delete(key kv.Key) {
	// binary search for the key
	low := 0
	high := len(s.data) - 1

	for low <= high {
		mid := low + (high-low)/2

		if s.data[mid].key == key {
			s.data = append(s.data[:mid], s.data[mid+1:]...)
			return
		}

		if s.data[mid].key < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
}

func (s *SortedList) Sort() {
	sort.Slice(s.data[:], func(i, j int) bool {
		return s.data[i].key < s.data[j].key
	})
}
