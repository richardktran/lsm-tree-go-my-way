package sstable

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/richardktran/lsm-tree-go-my-way/internal/memtable"
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
type SSTable struct {
	id               uint64
	sparseIndex      map[kv.Key]uint64 // key -> offset
	blocks           []Block
	config           config.Config
	dirConfig        *config.DirectoryConfig
	sparseLogFile    *os.File
	sparseLogChannel chan kv.Record // write-ahead log for SparseIndex
	CreatedAt        int64
	flushWg          sync.WaitGroup
}

/*
NewSSTable creates a new SSTable instance, initializes the sparse index and recovers the blocks
*/
func NewSSTable(id uint64, config *config.Config, dirConfig *config.DirectoryConfig) *SSTable {
	s := &SSTable{
		id:               id,
		sparseIndex:      make(map[kv.Key]uint64),
		blocks:           make([]Block, 0),
		config:           *config,
		sparseLogChannel: make(chan kv.Record, config.SparseWALBufferSize),
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

	// Start a goroutine to consume the record from the sparseLogChannel and write to the sparse index WAL
	go s.persistSparseIndex()

	return s
}

/*
Get look up the key in sparse index that closest and <= the key, get the base offset of the block
From the block, find the key and return the value.
If the key is not found, find the next block until the key is found.
If the key is not found in any block, return false
*/
func (s *SSTable) Get(key kv.Key) (kv.Value, bool) {
	// TODO: Implement a binary search to find the block that contains the key
	var closestKey kv.Key
	for k := range s.sparseIndex {
		if k <= key {
			closestKey = k
		}
	}
	startOffset := s.sparseIndex[closestKey]

	for _, block := range s.blocks {
		if block.baseOffset < startOffset {
			continue
		}

		value, found := block.Get(key)
		if found {
			return value, true
		}
	}

	return kv.Value(""), false
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

			s.sparseIndex[record.Key] = baseOffset
			s.sparseLogChannel <- record
		}

		if index == 0 {
			s.sparseIndex[record.Key] = baseOffset
			s.sparseLogChannel <- record
		}

		blockLen, _, err := block.Add(record)
		if err != nil {
			log.Println("Error adding record to block: ", err)
			return
		}

		baseOffset += uint64(blockLen)
	}

	s.blocks = append(s.blocks, *block)
}

func (s *SSTable) FlushWait() {
	s.flushWg.Wait()
}

// Close closes the sparseLogChannel and the sparse index WAL
func (s *SSTable) Close() error {
	s.flushWg.Wait()
	close(s.sparseLogChannel)
	return s.sparseLogFile.Close()
}

/*
recoverBlocks reads the SSTable directory and recovers all blocks in memory
NewBlock is called to create or open the block by id of sstable and offset of block
Update the blocks list with the recovered blocks in memory
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
}

/*
persistSparseIndex receives records from the sparseLogChannel and writes them to the sparse index WAL
Whenever a new record is added to the sparseLogChannel, it is written to the sparse index WAL
*/
func (s *SSTable) persistSparseIndex() {
	defer s.sparseLogFile.Close()

	for record := range s.sparseLogChannel {
		entry := fmt.Sprintf("%s:%d\n", record.Key, s.sparseIndex[record.Key])
		_, err := s.sparseLogFile.WriteString(entry)
		if err != nil {
			log.Println("Error writing to sparse index WAL: ", err)
		}
	}
}

// recoverSparseIndex reads the sparse index file and recovers the sparse index map in memory.
func (s *SSTable) recoverSparseIndex(filePath string) {
	sparseLogFile, err := os.Open(filePath)
	if err != nil {
		log.Println("Error opening sparse index file: ", err)
		return
	}
	defer sparseLogFile.Close()
	scanner := bufio.NewScanner(sparseLogFile)

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

		s.sparseIndex[key] = uint64(offset)
	}

}
