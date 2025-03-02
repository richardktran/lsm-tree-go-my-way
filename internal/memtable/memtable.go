package memtable

import (
	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/richardktran/lsm-tree-go-my-way/internal/memtable/algorithm"
	"github.com/richardktran/lsm-tree-go-my-way/internal/wal"
)

type MemTable struct {
	sortedData algorithm.SortedList
}

func NewMemTable() *MemTable {
	return &MemTable{
		sortedData: algorithm.NewSortedArray(),
	}
}

func (m *MemTable) Clone() MemTable {
	return MemTable{
		sortedData: m.sortedData.Clone(),
	}
}

func (m *MemTable) Get(key kv.Key) (kv.Value, bool) {

	return m.sortedData.Get(key)
}

func (m *MemTable) Set(key kv.Key, value kv.Value) {
	m.sortedData.Set(key, value)
}

func (m *MemTable) Delete(key kv.Key) {
	m.sortedData.Delete(key)
}

func (m *MemTable) Size() int {
	return m.sortedData.Size()
}

func (m *MemTable) GetAll() []kv.Record {
	return m.sortedData.GetAll()
}

func LoadFromWAL(wal *wal.WAL) (*MemTable, error) {
	lastTimestamp, err := wal.ReadLastItemFromMetaLog()
	if err != nil {
		return nil, err
	}

	// Read commit log
	records, err := wal.ReadCommitLogAfterTimestamp(lastTimestamp)
	if err != nil {
		return nil, err
	}

	return &MemTable{
		sortedData: algorithm.BuildSortedArray(records),
	}, nil
}
