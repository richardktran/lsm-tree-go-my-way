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
	wal       *wal.WAL
	flushWg sync.WaitGroup
	SSTable
	MemTable
}

type SSTable struct {
	sstableLock sync.RWMutex
	ssTables    []*sstable.SSTable
}

type MemTable struct {
	memTableLock    sync.RWMutex
	memTable        *memtable.MemTable
	freezedMemTable *memtable.MemTable
}

// NewStore creates a new LSMTreeStore instance, initializes the WAL, memTable, and SSTables from disk
func NewStore(config *config.Config, dirConfig *config.DirectoryConfig) *LSMTreeStore {
	initDirs(config.RootDataDir, dirConfig)

	tree := &LSMTreeStore{
		config:    config,
		dirConfig: dirConfig,
		SSTable: SSTable{
			ssTables: make([]*sstable.SSTable, 0),
		},
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

	s.memTableLock.RLock()
	defer s.memTableLock.RUnlock()

	// Check in-memory tables first
	for _, table := range []*memtable.MemTable{s.freezedMemTable, s.memTable} {
		if table != nil {
			if value, found := table.Get(key); found {
				if len(value) == 0 { // Check for tombstone
					return kv.Value(""), false
				}
				return value, true
			}
		}
	}

	s.sstableLock.RLock()
	defer s.sstableLock.RUnlock()
	// Check SSTables in reverse order
	for i := len(s.ssTables) - 1; i >= 0; i-- {
		if !s.ssTables[i].BloomFilter.MightContain(string(key)) {
			continue
		}
		if value, found := s.ssTables[i].Get(key); found {
			if len(value) == 0 { // empty value means tombstone — key was deleted
				return kv.Value(""), false
			}
			return value, true
		}
	}

	return kv.Value(""), false
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

	// Check if memTable is full
	if s.memTable.Size()+record.Size() >= s.config.MemTableSizeThreshold {
		// Flush a clone of the memTable to disk, clone to prevent reading while writing

		s.memTableLock.Lock()
		freezedMemtable := s.memTable.Clone()
		s.freezedMemTable = &freezedMemtable // must be set under memTableLock (same lock as the nil-clear in flushMemTable)
		s.memTableLock.Unlock()
		s.flushWg.Add(1)
		go s.flushMemTable(freezedMemtable, &curTimestamp)
		s.memTable = memtable.NewMemTable()

		// Write meta log in order to recover the memTable from the last flush
		if s.wal != nil {
			if _, err := s.wal.WriteMetaLog(&curTimestamp); err != nil {
				panic(err)
			}
		}
	}

	s.memTable.Set(key, value)
}

// Delete removes a key-value pair by insert a tombstone record into the memTable
func (s *LSMTreeStore) Delete(key kv.Key) {
	s.Set(key, nil)
}

// Close closes the store and all SSTables
func (s *LSMTreeStore) Close() error {
	s.sstableLock.Lock()
	defer s.sstableLock.Unlock()

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
flushMemTable creates a new SSTable from a frozen MemTable snapshot and appends
it to the SSTable list. After appending it checks whether automatic compaction
should be triggered based on the configured CompactionThreshold
*/
func (s *LSMTreeStore) flushMemTable(freezedMemTable memtable.MemTable, timestamp *uint64) {
	defer s.flushWg.Done()

	s.sstableLock.Lock()
	defer s.sstableLock.Unlock()

	ssTableID := uint64(time.Now().UnixNano())
	ssTable := sstable.NewSSTable(ssTableID, s.config, s.dirConfig)
	ssTable.Flush(freezedMemTable)
	ssTable.FlushWait()

	s.memTableLock.Lock()
	s.freezedMemTable = nil
	s.memTableLock.Unlock()

	s.ssTables = append(s.ssTables, ssTable)
	s.sortSSTables()

	// Trigger automatic compaction when the threshold is reached.
	if s.config.CompactionThreshold > 0 && len(s.ssTables) >= s.config.CompactionThreshold {
		if err := s.compactLocked(); err != nil {
			log.Printf("lsmtree: auto-compaction error: %v", err)
		}
	}
}

// WaitForFlush blocks until all in-flight background flush goroutines have finished
func (s *LSMTreeStore) WaitForFlush() {
	s.flushWg.Wait()
}

// Compact runs a full compaction of all SSTables: it merges them into a single SSTable, keeping only the newest version of each key and dropping tombstones.
// It is a no-op when the number of SSTables is below the CompactionThreshold (or when CompactionThreshold is 0, acting as an unconditional manual trigger).
func (s *LSMTreeStore) Compact() error {
	s.sstableLock.Lock()
	defer s.sstableLock.Unlock()
	return s.compactLocked()
}

// compactLocked performs the compaction. It must be called with sstableLock held.
//
// Algorithm:
//  1. If the SSTable count is below CompactionThreshold (and threshold > 0), return early.
//  2. Iterate SSTables newest-first (s.ssTables is sorted newest-first).
//  3. For each record, keep only the first occurrence of each key (= newest version).
//  4. Drop tombstones (empty Value) — safe because there are no lower levels to mask.
//  5. Sort the surviving records by key.
//  6. Close and delete all existing SSTables from disk.
//  7. Write a single new SSTable with the merged records.
func (s *LSMTreeStore) compactLocked() error {
	if s.config.CompactionThreshold > 0 && len(s.ssTables) < s.config.CompactionThreshold {
		return nil
	}
	if len(s.ssTables) == 0 {
		return nil
	}

	log.Printf("lsmtree: compaction starting, merging %d SSTables", len(s.ssTables))

	// s.ssTables is sorted newest-first; first occurrence of a key is the winner.
	seen := make(map[kv.Key]struct{}, 64)
	var merged []kv.Record

	for _, table := range s.ssTables {
		for _, record := range table.GetAll() {
			if _, exists := seen[record.Key]; !exists {
				seen[record.Key] = struct{}{}
				merged = append(merged, record)
			}
		}
	}

	// Drop tombstones (len(Value)==0). Since we have a single level, no lower
	// SSTable can resurface a deleted key, so tombstones can be safely removed.
	compacted := merged[:0]
	for _, r := range merged {
		if len(r.Value) > 0 {
			compacted = append(compacted, r)
		}
	}

	// Sort surviving records by key so the new SSTable is properly ordered.
	sort.Slice(compacted, func(i, j int) bool {
		return compacted[i].Key < compacted[j].Key
	})

	// Close and remove all existing SSTables from disk.
	for _, table := range s.ssTables {
		if err := table.CloseAndDelete(); err != nil {
			return fmt.Errorf("lsmtree: compaction cleanup: %w", err)
		}
	}
	s.ssTables = s.ssTables[:0]

	// Write the merged result as a new SSTable, then close and reload it from disk
	if len(compacted) > 0 {
		newID := uint64(time.Now().UnixNano())
		newTable := sstable.NewSSTable(newID, s.config, s.dirConfig)
		newTable.FlushRecords(compacted)
		newTable.FlushWait()

		if err := newTable.Close(); err != nil {
			log.Printf("lsmtree: compaction: error closing new SSTable: %v", err)
		}

		reloaded := sstable.NewSSTable(newID, s.config, s.dirConfig)
		s.ssTables = []*sstable.SSTable{reloaded}
	}

	log.Printf("lsmtree: compaction done, %d records in 1 SSTable", len(compacted))
	return nil
}

// sortSSTables sorts the SSTables by creation time in descending order
func (s *LSMTreeStore) sortSSTables() {
	sort.Slice(s.ssTables[:], func(i, j int) bool {
		return s.ssTables[i].CreatedAt > s.ssTables[j].CreatedAt
	})
}
