package lsmtree

import (
	"log"
	"os"
	"sort"
	"strconv"
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
	tree := &LSMTreeStore{
		config:    config,
		ssTables:  make([]*sstable.SSTable, 0),
		dirConfig: dirConfig,
	}

	wal, err := wal.NewWAL(dirConfig.WALDir)
	if err != nil {
		panic(err)
	}
	tree.wal = wal

	memTable, err := memtable.LoadFromWAL(wal)
	if err != nil {
		log.Println("Error loading memtable from WAL: ", err)
	}

	tree.memTable = memTable

	ssTables, err := tree.loadSSTables()
	if err != nil {
		log.Println("Error loading SSTables: ", err)
	}

	tree.ssTables = ssTables

	return tree
}

func (s *LSMTreeStore) Get(key kv.Key) (kv.Value, bool) {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()

	if value, found := s.memTable.Get(key); found {
		return value, true
	}

	for i := len(s.ssTables) - 1; i >= 0; i-- {
		if value, found := s.ssTables[i].Get(key); found {
			return value, true
		}
	}

	return "", false
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
	ssTable := sstable.NewSSTable(uint64(len(s.ssTables)), s.config, s.dirConfig)

	go ssTable.Flush(memTable, s.dirConfig)

	s.ssTables = append(s.ssTables, ssTable)
	s.sortSSTables()
}

func (s *LSMTreeStore) Close() error {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	for _, ssTable := range s.ssTables {
		if err := ssTable.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (s *LSMTreeStore) sortSSTables() {
	sort.Slice(s.ssTables[:], func(i, j int) bool {
		return s.ssTables[i].CreatedAt > s.ssTables[j].CreatedAt
	})
}

func (s *LSMTreeStore) loadSSTables() ([]*sstable.SSTable, error) {
	ssTables := make([]*sstable.SSTable, 0)
	dirs, err := os.ReadDir(s.dirConfig.SSTableDir)
	if err != nil {
		return ssTables, err
	}

	levels := make([]int, 0)

	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}

		level, err := strconv.Atoi(dir.Name())
		if err != nil {
			continue
		}
		levels = append(levels, level)
	}

	for _, level := range levels {
		ssTable := sstable.NewSSTable(uint64(level), s.config, s.dirConfig)

		ssTables = append(ssTables, ssTable)
	}

	s.sortSSTables()

	return ssTables, nil
}
