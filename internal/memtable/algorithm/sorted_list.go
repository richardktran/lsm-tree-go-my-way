package algorithm

import (
	"log"
	"sort"

	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
)

type SortedList struct {
	data []kv.Record
	size int
}

func NewSortedList() *SortedList {
	return &SortedList{
		data: make([]kv.Record, 0),
		size: 0,
	}
}

func (s *SortedList) Clone() *SortedList {
	newList := NewSortedList()
	_ = copy(newList.data, s.data)
	newList.size = s.size
	return newList
}

func (s *SortedList) Insert(key kv.Key, value kv.Value) {
	_, exists := s.Get(key)
	if exists {
		s.Delete(key)
	}

	s.data = append(s.data, kv.Record{Key: key, Value: value})
	s.size += kv.Record{Key: key, Value: value}.Size()
	s.Sort()
	log.Println("Size of sorted list: ", s.size)
}

func (s *SortedList) Get(key kv.Key) (kv.Value, bool) {
	// binary search for the key
	low := 0
	high := len(s.data) - 1

	for low <= high {
		mid := low + (high-low)/2

		if s.data[mid].Key == key {
			return s.data[mid].Value, true
		}

		if s.data[mid].Key < key {
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

		if s.data[mid].Key == key {
			s.size -= kv.Record{Key: key, Value: s.data[mid].Value}.Size()
			s.data = append(s.data[:mid], s.data[mid+1:]...)
			return
		}

		if s.data[mid].Key < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
}

func (s *SortedList) Sort() {
	sort.Slice(s.data[:], func(i, j int) bool {
		return s.data[i].Key < s.data[j].Key
	})
}

func (s *SortedList) Size() int {
	return s.size
}

func (s *SortedList) GetAll() []kv.Record {
	return s.data
}
