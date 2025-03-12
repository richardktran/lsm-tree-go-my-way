package lsmtree

import (
	"fmt"
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

// Ensure LSMTreeStore implements the store.Store interface
var _ store.Store = (*LSMTreeStore)(nil)

type LSMTreeStore struct {
	config    *config.Config
	dirConfig *config.DirectoryConfig
	storeLock sync.RWMutex
	memTable  *memtable.MemTable
	ssTables  []*sstable.SSTable
	wal       *wal.WAL
}

// NewStore creates a new LSMTreeStore instance, initializes the WAL, memTable, and SSTables from disk
func NewStore(config *config.Config, dirConfig *config.DirectoryConfig) *LSMTreeStore {
	initDirs(config.RootDataDir, dirConfig)

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

// Get searches the memTable first then the SSTables
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

/*
Set adds a new key-value pair to the memTable. If the memTable is full, it is flushed to disk as an SSTable.
The memTable is then reset to an empty state. The WAL is also updated with the new record and a meta log.
*/
func (s *LSMTreeStore) Set(key kv.Key, value kv.Value) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	curTimestamp := uint64(time.Now().Unix())
	record := kv.Record{
		Key:   key,
		Value: value,
	}

	// Write to WAL
	if s.wal != nil {
		if _, err := s.wal.WriteCommitLog(&record, &curTimestamp); err != nil {
			panic(err)
		}
	}

	s.memTable.Set(key, value)

	// Check if memTable is full
	if s.memTable.Size() >= s.config.MemTableSizeThreshold {
		// Flush a clone of the memTable to disk, clone to prevent reading while writing
		s.flushMemTable(s.memTable.Clone(), &curTimestamp)
		s.memTable = memtable.NewMemTable()

		// Write meta log in order to recover the memTable from the last flush
		if s.wal != nil {
			if _, err := s.wal.WriteMetaLog(&curTimestamp); err != nil {
				panic(err)
			}
		}
	}
}

// Delete removes a key-value pair from the memTable
// TODO: Will need to implement a tombstone mechanism to handle deletes
func (s *LSMTreeStore) Delete(key kv.Key) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	s.memTable.Delete(key)
}

// Close closes the store and all SSTables
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

// initDirs adds the root directory to the beginning of all the directories in the DirectoryConfig
// and creates the directories if they do not exist.
func initDirs(rootDir string, dirConfig *config.DirectoryConfig) {
	if _, err := os.Stat(rootDir); os.IsNotExist(err) {
		os.Mkdir(rootDir, os.ModePerm)
	}

	dirs := []*string{
		&dirConfig.WALDir,
		&dirConfig.SSTableDir,
		&dirConfig.SparseIndexDir,
	}
	for _, dir := range dirs {
		*dir = fmt.Sprintf("%s/%s", rootDir, *dir)
		if _, err := os.Stat(*dir); os.IsNotExist(err) {
			os.Mkdir(*dir, os.ModePerm)
		}
	}
}

/*
loadSSTables loop through the SSTable directory and load all SSTables into memory.
Add those SSTables to the list of SSTables
*/
func (s *LSMTreeStore) loadSSTables() ([]*sstable.SSTable, error) {
	ssTables := make([]*sstable.SSTable, 0)
	dirs, err := os.ReadDir(s.dirConfig.SSTableDir)
	if err != nil {
		return ssTables, err
	}

	ssTableIds := make([]int, 0)

	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}

		ssTableId, err := strconv.Atoi(dir.Name())
		if err != nil {
			continue
		}
		ssTableIds = append(ssTableIds, ssTableId)
	}

	for _, ssTableId := range ssTableIds {
		ssTable := sstable.NewSSTable(uint64(ssTableId), s.config, s.dirConfig)

		ssTables = append(ssTables, ssTable)
	}

	s.sortSSTables()

	return ssTables, nil
}

/*
FlushMemTable create a new thread to flush the memTable to disk as an SSTable.
The memTable is then reset to an empty state to receive new key-value pairs.
Update the new SSTable to the list of SSTables
*/
func (s *LSMTreeStore) flushMemTable(memTable memtable.MemTable, timestamp *uint64) {
	ssTable := sstable.NewSSTable(*timestamp, s.config, s.dirConfig)

	go ssTable.Flush(memTable, s.dirConfig)

	s.ssTables = append(s.ssTables, ssTable)
	s.sortSSTables()
}

// sortSSTables sorts the SSTables by creation time in descending order
func (s *LSMTreeStore) sortSSTables() {
	sort.Slice(s.ssTables[:], func(i, j int) bool {
		return s.ssTables[i].CreatedAt > s.ssTables[j].CreatedAt
	})
}
