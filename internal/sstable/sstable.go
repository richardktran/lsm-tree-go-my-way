package sstable

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/richardktran/lsm-tree-go-my-way/internal/memtable"
	"github.com/richardktran/lsm-tree-go-my-way/pkg/bloomfilter"
)

/*
SSTable is a sorted string table
Folder name pattern: data/sstables/<id>/**
  - id: the id representing the SSTable

Sparse Index: Store base offset of each block, each key:offset represents the start of a block
  - Folder name pattern: indexes/<id>.index
  - File format: <key>:<offset>
  - key: the key of the record
  - offset: the offset of the record in the SSTable file

Each SSTable is composed of multiple blocks
Folder name pattern: data/sstables/<id>/<offset>.sst
  - offset: the offset of the block in the SSTable file
  - File format: <keyLen><key><valueLen><value>
  - keyLen: the length of the key
  - key: the key of the record
  - valueLen: the length of the value
  - value: the value of the record
*/
type sparseEntry struct {
	key    kv.Key
	offset uint64
}

type SSTable struct {
	id               uint64
	sparseEntries    []sparseEntry // always sorted by key
	sparseIndexLock  sync.Mutex
	blocks           []Block
	config           config.Config
	dirConfig        *config.DirectoryConfig
	sparseLogFile    *os.File
	sparseLogChannel chan sparseEntry // write-ahead log for SparseIndex
	sparseIndexWg    sync.WaitGroup   // tracks the persistSparseIndex goroutine
	CreatedAt        int64
	flushWg          sync.WaitGroup
	BloomFilter      *bloomfilter.BloomFilter
}

/*
NewSSTable creates a new SSTable instance, initializes the sparse index and recovers the blocks
*/
func NewSSTable(id uint64, config *config.Config, dirConfig *config.DirectoryConfig) *SSTable {
	s := &SSTable{
		id:               id,
		sparseEntries:    make([]sparseEntry, 0),
		blocks:           make([]Block, 0),
		config:           *config,
		sparseLogChannel: make(chan sparseEntry, config.SparseWALBufferSize),
		dirConfig:        dirConfig,
		CreatedAt:        time.Now().UnixNano(),
	}

	sparseIndexFolderPath := path.Join(dirConfig.SparseIndexDir)

	if _, err := os.Stat(sparseIndexFolderPath); os.IsNotExist(err) {
		os.MkdirAll(dirConfig.SparseIndexDir, 0755)
	}

	indexFilePath := path.Join(dirConfig.SparseIndexDir, fmt.Sprintf("%d.index", id))

	s.recoverSparseIndex(indexFilePath)
	s.recoverBlocks()

	sparseLogFile, err := os.OpenFile(indexFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return s
	}

	s.sparseLogFile = sparseLogFile

	// Start a goroutine to consume the record from the sparseLogChannel and write to the sparse index WAL.
	// sparseIndexWg tracks this goroutine so Close() can wait for all writes to land before closing the file.
	s.sparseIndexWg.Add(1)
	go func() {
		defer s.sparseIndexWg.Done()
		s.persistSparseIndex()
	}()

	// Build the bloom filter
	s.BloomFilter = bloomfilter.NewBloomFilter(config.BloomFilterSize, config.BloomFilterHashCount)
	for _, block := range s.blocks {
		records, _ := block.GetAll()
		for _, record := range records {
			s.BloomFilter.Add(string(record.Key))
		}
	}

	return s
}

/*
Get look up the key in sparse index that closest and <= the key, get the base offset of the block
From the block, find the key and return the value.
If the key is not found, find the next block until the key is found.
If the key is not found in any block, return false
*/
func (s *SSTable) Get(key kv.Key) (kv.Value, bool) {
	startOffset, ok := s.findSparseOffset(key)
	if !ok {
		return kv.Value(""), false
	}

	for _, block := range s.blocks {
		if block.baseOffset < startOffset {
			break
		}

		value, found := block.Get(key)
		if found {
			return value, true
		}
	}

	return kv.Value(""), false
}

// findSparseOffset binary-searches sparseEntries for the largest entry with key <= target.
func (s *SSTable) findSparseOffset(key kv.Key) (uint64, bool) {
	n := len(s.sparseEntries)
	if n == 0 {
		return 0, false
	}

	// sort.Search returns the smallest i where sparseEntries[i].key > key,
	// so the largest entry with key <= target is at i-1.
	i := sort.Search(n, func(i int) bool {
		return s.sparseEntries[i].key > key
	})
	if i == 0 {
		return 0, false
	}

	return s.sparseEntries[i-1].offset, true
}

// addSparseEntry appends a sparse index entry (keys are inserted in sorted order during flush)
// and queues it for persistence.
func (s *SSTable) addSparseEntry(key kv.Key, offset uint64) {
	entry := sparseEntry{key: key, offset: offset}
	s.sparseIndexLock.Lock()
	s.sparseEntries = append(s.sparseEntries, entry)
	s.sparseIndexLock.Unlock()
	s.sparseLogChannel <- entry
}

/*
Flush get all records from the memtable, add to a block until the block is full.
When the block is full, write the block to disk and create a new block.
Each block is added to the SSTable.
The base offset of each block is stored in the sparse index.
Write the record to the sparseLogChannel to persist the sparse index to disk (will be consumed by persistSparseIndex)
*/
func (s *SSTable) Flush(memtable memtable.MemTable) {
	s.flushWg.Add(1)
	defer s.flushWg.Done()

	var baseOffset uint64 = 0
	block, err := NewBlock(s.id, baseOffset, s.dirConfig)

	if err != nil {
		log.Println("Error creating new block: ", err)
		return
	}
	for index, record := range memtable.GetAll() {
		if block.IsMax(s.config.SSTableBlockSize) {
			s.blocks = append(s.blocks, *block)
			block.Close()
			block, err = NewBlock(s.id, baseOffset, s.dirConfig)
			if err != nil {
				log.Println("Error creating new block: ", err)
				return
			}

			s.addSparseEntry(record.Key, baseOffset)
		}

		if index == 0 {
			s.addSparseEntry(record.Key, baseOffset)
		}

		blockLen, _, err := block.Add(record)
		if err != nil {
			log.Println("Error adding record to block: ", err)
			return
		}

		baseOffset += uint64(blockLen)
	}

	s.blocks = append(s.blocks, *block)

	// sort blocks by base offset
	s.SortBlocks()
}

func (s *SSTable) SortBlocks() {
	sort.Slice(s.blocks, func(i, j int) bool {
		return s.blocks[i].baseOffset > s.blocks[j].baseOffset
	})
}

func (s *SSTable) FlushWait() {
	s.flushWg.Wait()
}

// Close closes the sparseLogChannel and the sparse index WAL
func (s *SSTable) Close() error {
	s.flushWg.Wait()
	close(s.sparseLogChannel)
	s.sparseIndexWg.Wait()
	return s.sparseLogFile.Close()
}

// GetAll returns every record stored across all blocks of this SSTable
func (s *SSTable) GetAll() []kv.Record {
	var records []kv.Record
	for _, b := range s.blocks {
		// Open a fresh handle regardless of whether the stored one is still open.
		freshBlock, err := NewBlock(s.id, b.baseOffset, s.dirConfig)
		if err != nil {
			log.Printf("sstable.GetAll: error opening block at offset %d: %v", b.baseOffset, err)
			continue
		}
		recs, readErr := freshBlock.GetAll()
		_ = freshBlock.Close()
		if readErr != nil {
			log.Printf("sstable.GetAll: error reading block at offset %d: %v", b.baseOffset, readErr)
			continue
		}
		records = append(records, recs...)
	}
	return records
}

// FlushRecords writes a pre-sorted slice of records directly to this SSTable
func (s *SSTable) FlushRecords(records []kv.Record) {
	s.flushWg.Add(1)
	defer s.flushWg.Done()

	if len(records) == 0 {
		return
	}

	var baseOffset uint64
	block, err := NewBlock(s.id, baseOffset, s.dirConfig)
	if err != nil {
		log.Println("FlushRecords: error creating block:", err)
		return
	}

	for index, record := range records {
		if block.IsMax(s.config.SSTableBlockSize) {
			s.blocks = append(s.blocks, *block)
			block.Close()
			block, err = NewBlock(s.id, baseOffset, s.dirConfig)
			if err != nil {
				log.Println("FlushRecords: error creating block:", err)
				return
			}

			s.addSparseEntry(record.Key, baseOffset)
		}

		if index == 0 {
			s.addSparseEntry(record.Key, baseOffset)
		}

		blockLen, _, err := block.Add(record)
		if err != nil {
			log.Println("FlushRecords: error adding record:", err)
			return
		}
		baseOffset += uint64(blockLen)
	}

	s.blocks = append(s.blocks, *block)
	s.SortBlocks()
}

// DeleteFromDisk removes all on-disk artefacts belonging to this SSTable:
// the block directory and the sparse-index file
func (s *SSTable) DeleteFromDisk() error {
	blockDir := path.Join(s.dirConfig.SSTableDir, fmt.Sprintf("%d", s.id))
	if err := os.RemoveAll(blockDir); err != nil {
		return fmt.Errorf("sstable.DeleteFromDisk: remove blocks: %w", err)
	}

	indexFilePath := path.Join(s.dirConfig.SparseIndexDir, fmt.Sprintf("%d.index", s.id))
	if err := os.Remove(indexFilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("sstable.DeleteFromDisk: remove index: %w", err)
	}

	return nil
}

// CloseAndDelete is a convenience wrapper that closes the SSTable and then
// removes all its on-disk files.
func (s *SSTable) CloseAndDelete() error {
	if err := s.Close(); err != nil {
		return err
	}
	return s.DeleteFromDisk()
}

/*
recoverBlocks reads the SSTable directory and recovers all blocks in memory.
NewBlock is called to create or open the block by id of sstable and offset of block.
Blocks are sorted descending by baseOffset (same invariant as after Flush) so that
SSTable.Get() works correctly.
*/
func (s *SSTable) recoverBlocks() {
	blocks := make([]Block, 0)

	blockDir := path.Join(s.dirConfig.SSTableDir, fmt.Sprintf("%d", s.id))
	files, err := os.ReadDir(blockDir)
	if err != nil {
		s.blocks = blocks
		return
	}

	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 64)
		block, err := NewBlock(s.id, off, s.dirConfig)
		if err != nil {
			log.Println("Error creating block: ", err)
			continue
		}

		blocks = append(blocks, *block)
	}

	s.blocks = blocks
	s.SortBlocks()
}

/*
persistSparseIndex receives entries from the sparseLogChannel and writes them to the sparse index WAL
Whenever a new entry is added to the sparseLogChannel, it is written to the sparse index WAL
*/
func (s *SSTable) persistSparseIndex() {
	for entry := range s.sparseLogChannel {
		line := fmt.Sprintf("%s:%d\n", entry.key, entry.offset)
		_, err := s.sparseLogFile.WriteString(line)
		if err != nil {
			log.Println("Error writing to sparse index WAL: ", err)
		}
	}
}

// recoverSparseIndex reads the sparse index file and recovers the sorted sparseEntries slice in memory.
func (s *SSTable) recoverSparseIndex(filePath string) {
	sparseLogFile, err := os.Open(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Println("Error opening sparse index file: ", err)
		}
		return
	}
	defer sparseLogFile.Close()
	scanner := bufio.NewScanner(sparseLogFile)

	// Use a map first so duplicate keys in the WAL keep the latest offset, matching prior behavior.
	index := make(map[kv.Key]uint64)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			log.Println("Invalid sparse index line: ", line)
			continue
		}

		key := kv.Key(parts[0])
		offset, err := strconv.Atoi(parts[1])

		if err != nil {
			log.Println("Error parsing offset: ", err)
			continue
		}

		index[key] = uint64(offset)
	}

	s.sparseEntries = make([]sparseEntry, 0, len(index))
	for key, offset := range index {
		s.sparseEntries = append(s.sparseEntries, sparseEntry{key: key, offset: offset})
	}
	sort.Slice(s.sparseEntries, func(i, j int) bool {
		return s.sparseEntries[i].key < s.sparseEntries[j].key
	})
}
