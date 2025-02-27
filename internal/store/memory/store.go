package memory

import (
	"sync"

	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/richardktran/lsm-tree-go-my-way/internal/store"
)

var _ store.Store = (*MemoryStore)(nil)

type MemoryStore struct {
	data map[kv.Key]kv.Value
	mu   sync.RWMutex
}

func NewStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[kv.Key]kv.Value),
	}
}

func (s *MemoryStore) Get(key kv.Key) (kv.Value, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, exists := s.data[key]

	return val, exists
}

func (s *MemoryStore) Set(key kv.Key, value kv.Value) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

func (s *MemoryStore) Delete(key kv.Key) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}
