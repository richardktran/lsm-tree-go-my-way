package memtable

import (
	"fmt"

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
	if val, _ := m.sortedData.Get(key); val == nil {
		return kv.Value(""), true
	}

	return m.sortedData.Get(key)
}

func (m *MemTable) Set(key kv.Key, value kv.Value) {
	m.sortedData.Set(key, value)
}

func (m *MemTable) Delete(key kv.Key) {
	m.sortedData.Set(key, nil) // nil value indicates tombstone
}

func (m *MemTable) Size() int {
	return m.sortedData.Size()
}

func (m *MemTable) GetAll() []kv.Record {
	allData := m.sortedData.GetAll()

	// Filter out tombstones
	var records []kv.Record
	for _, record := range allData {
		if record.Value == nil {
			records = append(records, kv.Record{
				Key:   record.Key,
				Value: kv.Value(""),
			})
		} else {
			records = append(records, record)
		}
	}

	return records
}

func LoadFromWAL(wal *wal.WAL) (*MemTable, error) {
	memTable := NewMemTable()

	lastTimestamp, _ := wal.ReadLastItemFromMetaLog()

	// Read commit log
	records, err := wal.ReadCommitLogAfterTimestamp(lastTimestamp)
	if err != nil {
		return memTable, fmt.Errorf("error reading commit log after timestamp %d: %w", lastTimestamp, err)
	}

	if len(records) > 0 {
		memTable.sortedData = algorithm.BuildSortedArray(records)
	}

	return memTable, nil
}
