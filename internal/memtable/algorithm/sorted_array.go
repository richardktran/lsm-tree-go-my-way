package algorithm

import (
	"log"
	"sort"

	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
)

var _ SortedList = (*SortedArray)(nil)

type SortedArray struct {
	data []kv.Record
	size int
}

func NewSortedArray() *SortedArray {
	return &SortedArray{
		data: make([]kv.Record, 0),
		size: 0,
	}
}

func BuildSortedArray(data []kv.Record) *SortedArray {
	sort.Slice(data[:], func(i, j int) bool {
		return data[i].Key < data[j].Key
	})

	size := 0
	for _, record := range data {
		size += record.Size()
	}

	return &SortedArray{
		data: data,
		size: size,
	}
}

func (s *SortedArray) Clone() SortedList {
	newList := NewSortedArray()
	newList.data = make([]kv.Record, len(s.data))
	copy(newList.data, s.data)
	newList.size = s.size
	return newList
}

func (s *SortedArray) Set(key kv.Key, value kv.Value) {
	_, exists := s.Get(key)
	if exists {
		s.Delete(key)
	}

	s.data = append(s.data, kv.Record{Key: key, Value: value})
	s.size += kv.Record{Key: key, Value: value}.Size()
	s.Sort()
	log.Println("Size of sorted list: ", s.size)
}

func (s *SortedArray) Get(key kv.Key) (kv.Value, bool) {
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

func (s *SortedArray) Delete(key kv.Key) {
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

func (s *SortedArray) Sort() {
	sort.Slice(s.data[:], func(i, j int) bool {
		return s.data[i].Key < s.data[j].Key
	})
}

func (s *SortedArray) Size() int {
	return s.size
}

func (s *SortedArray) GetAll() []kv.Record {
	return s.data
}
