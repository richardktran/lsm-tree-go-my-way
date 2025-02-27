package lsmtree

import (
	"sync"

	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/richardktran/lsm-tree-go-my-way/internal/store"
)

var _ store.Store = (*LSMTreeStore)(nil)

type LSMTreeStore struct {
	storeLock sync.RWMutex
	memTable  *MemTable
}

func NewStore() *LSMTreeStore {
	return &LSMTreeStore{
		memTable: NewMemTable(),
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
}

func (s *LSMTreeStore) Delete(key kv.Key) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	s.memTable.Delete(key)
}
