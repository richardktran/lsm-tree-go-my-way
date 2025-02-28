package lsmtree

import (
	"log"
	"sync"

	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/richardktran/lsm-tree-go-my-way/internal/store"
)

var _ store.Store = (*LSMTreeStore)(nil)

type LSMTreeStore struct {
	config    Config
	storeLock sync.RWMutex
	memTable  *MemTable
}

type Config struct {
	MemTableSizeThreshold int
}

func NewStore(config Config) *LSMTreeStore {
	return &LSMTreeStore{
		memTable: NewMemTable(),
		config:   config,
	}
}

func (s *LSMTreeStore) Get(key kv.Key) (kv.Value, bool) {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()

	return s.memTable.Get(key)
}

func (s *LSMTreeStore) Set(key kv.Key, value kv.Value) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	recordSize := kv.Record{Key: key, Value: value}.Size()

	// Check if memTable is full
	if s.memTable.Size()+recordSize > s.config.MemTableSizeThreshold {
		s.flushMemTable(s.memTable.Clone())
		s.memTable = NewMemTable()
	}

	s.memTable.Set(key, value)
}

func (s *LSMTreeStore) Delete(key kv.Key) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	s.memTable.Delete(key)
}

// TODO: Implement this function after create SSTable
func (s *LSMTreeStore) flushMemTable(memTable *MemTable) {
	log.Print("Flushing memTable to disk")
	// Create new SSTable
	// Add values from memTable to SSTable
	// Flush SSTable to disk
	// Update sparse index
	// update bloom filter
}
