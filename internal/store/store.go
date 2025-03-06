package store

import "github.com/richardktran/lsm-tree-go-my-way/internal/kv"

type Store interface {
	// Get retrieves the value for the given key.
	Get(key kv.Key) (kv.Value, bool)

	// Set sets the value for the given key.
	Set(key kv.Key, value kv.Value)

	// Delete deletes the key from the store.
	Delete(key kv.Key)
}
