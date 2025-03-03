package lsmtree

import (
	"log"
	"sync"
	"time"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/richardktran/lsm-tree-go-my-way/internal/memtable"
	"github.com/richardktran/lsm-tree-go-my-way/internal/sstable"
	"github.com/richardktran/lsm-tree-go-my-way/internal/store"
	"github.com/richardktran/lsm-tree-go-my-way/internal/wal"
)

var _ store.Store = (*LSMTreeStore)(nil)

type LSMTreeStore struct {
	config    config.Config
	storeLock sync.RWMutex
	memTable  *memtable.MemTable
	ssTables  []*sstable.SSTable
	wal       *wal.WAL
	dirConfig config.DirectoryConfig
}

func NewStore(config config.Config, dirConfig config.DirectoryConfig) *LSMTreeStore {
	wal, err := wal.NewWAL(dirConfig.WALDir)
	if err != nil {
		panic(err)
	}

	memTable, err := memtable.LoadFromWAL(wal)
	if err != nil {
		log.Println("Error loading memtable from WAL: ", err)
	}

	return &LSMTreeStore{
		memTable:  memTable,
		config:    config,
		ssTables:  make([]*sstable.SSTable, 0),
		wal:       wal,
		dirConfig: dirConfig,
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

	curTimestamp := time.Now().Unix()

	record := kv.Record{
		Key:   key,
		Value: value,
	}

	if s.wal != nil {
		if _, err := s.wal.WriteCommitLog(record, curTimestamp); err != nil {
			panic(err)
		}
	}

	s.memTable.Set(key, value)

	// Check if memTable is full
	if s.memTable.Size() >= s.config.MemTableSizeThreshold {
		s.flushMemTable(s.memTable.Clone())
		s.memTable = memtable.NewMemTable()

		if s.wal != nil {
			if _, err := s.wal.WriteMetaLog(curTimestamp); err != nil {
				panic(err)
			}
		}
	}
}

func (s *LSMTreeStore) Delete(key kv.Key) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	s.memTable.Delete(key)
}

func (s *LSMTreeStore) flushMemTable(memTable memtable.MemTable) {
	ssTable := sstable.NewSSTable(uint64(len(s.ssTables)), s.config)

	go ssTable.Flush(memTable, s.dirConfig)

	s.ssTables = append(s.ssTables, ssTable)
}
