package memory

import (
	"sync"

	"github.com/richardktran/lsm-tree-go-my-way/internal/store"
)

var _ store.Store = (*MemoryStore)(nil)

type MemoryStore struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[string]string),
	}
}

func (s *MemoryStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, exists := s.data[key]

	return val, exists
}

func (s *MemoryStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

func (s *MemoryStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}
