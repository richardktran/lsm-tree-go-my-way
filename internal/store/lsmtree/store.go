package lsmtree

import (
	"sync"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/richardktran/lsm-tree-go-my-way/internal/memtable"
	"github.com/richardktran/lsm-tree-go-my-way/internal/sstable"
	"github.com/richardktran/lsm-tree-go-my-way/internal/store"
)

var _ store.Store = (*LSMTreeStore)(nil)

type LSMTreeStore struct {
	config    config.Config
	storeLock sync.RWMutex
	memTable  *memtable.MemTable
	ssTables  []*sstable.SSTable
}

func NewStore(config config.Config) *LSMTreeStore {
	return &LSMTreeStore{
		memTable: memtable.NewMemTable(),
		config:   config,
		ssTables: make([]*sstable.SSTable, 0),
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

	s.memTable.Set(key, value)

	// Check if memTable is full
	if s.memTable.Size() >= s.config.MemTableSizeThreshold {
		s.flushMemTable(s.memTable.Clone())
		s.memTable = memtable.NewMemTable()
	}
}

func (s *LSMTreeStore) Delete(key kv.Key) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	s.memTable.Delete(key)
}

func (s *LSMTreeStore) flushMemTable(memTable memtable.MemTable) {
	ssTable := sstable.NewSSTable(uint64(len(s.ssTables)), s.config)

	go ssTable.Flush(memTable)

	s.ssTables = append(s.ssTables, ssTable)
}
