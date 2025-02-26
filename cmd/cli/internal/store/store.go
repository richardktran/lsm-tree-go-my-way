package store

import "sync"

type Store struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, exists := s.data[key]

	return val, exists
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}
