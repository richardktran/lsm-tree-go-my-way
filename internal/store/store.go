package store

import "github.com/richardktran/lsm-tree-go-my-way/internal/kv"

type Store interface {
	Get(key kv.Key) (kv.Value, bool)
	Set(key kv.Key, value kv.Value)
	Delete(key kv.Key)
}
