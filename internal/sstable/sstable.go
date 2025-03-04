package sstable

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/richardktran/lsm-tree-go-my-way/internal/memtable"
)

/*
SSTable is a sorted string table
File name: sstables/<layer>/offset.sst
  - layer: the level of the SSTable
  - offset: the offset of the SSTable in the level
  - sst: SSTable file extension

Sparse Index:
  - File name: sstables/<layer>/offset.index

Each file represents a block
*/
type SSTable struct {
	level            uint64
	sparseIndex      map[kv.Key]uint64 // key -> offset
	blocks           []Block
	config           config.Config
	dirConfig        config.DirectoryConfig
	sparseLogFile    *os.File
	sparseLogChannel chan kv.Record // write-ahead log for SparseIndex
	CreatedAt        int64
}

func NewSSTable(level uint64, config config.Config, dirConfig config.DirectoryConfig) *SSTable {
	s := &SSTable{
		level:            level,
		sparseIndex:      make(map[kv.Key]uint64),
		blocks:           make([]Block, 0),
		config:           config,
		sparseLogChannel: make(chan kv.Record, config.SparseWALBufferSize),
		dirConfig:        dirConfig,
		CreatedAt:        time.Now().UnixNano(),
	}

	folderPath := path.Join(dirConfig.SparseIndexDir)

	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		os.MkdirAll(dirConfig.SparseIndexDir, 0755)
	}

	filePath := path.Join(dirConfig.SparseIndexDir, fmt.Sprintf("%d.index", level))

	s.recover(filePath)

	sparseLogFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		log.Println("Error opening sparse index file: ", err)
		return s
	}

	s.sparseLogFile = sparseLogFile

	go s.writeWAL()

	return s
}

func (s *SSTable) Get(key kv.Key) (kv.Value, bool) {
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

	return "", false
}

func (s *SSTable) Flush(memtable memtable.MemTable, dirConfig config.DirectoryConfig) {
	var baseOffset uint64 = 0
	block, err := NewBlock(s.level, baseOffset, dirConfig)

	if err != nil {
		log.Println("Error creating new block: ", err)
		return
	}
	log.Println("Flushing memtable to SSTable")
	for index, record := range memtable.GetAll() {
		if block.IsMax(s.config.SSTableBlockSize) {
			s.blocks = append(s.blocks, *block)
			block.Close()
			block, err = NewBlock(s.level, baseOffset, dirConfig)
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

		blockLen, pos, err := block.Add(record)
		if err != nil {
			log.Println("Error adding record to block: ", err)
			return
		}

		log.Printf("Added record to block at position %d with baseOffset %d and len = %d\n", pos, baseOffset, blockLen)

		baseOffset += uint64(blockLen)
	}

	s.blocks = append(s.blocks, *block)
}

func (s *SSTable) Close() error {
	close(s.sparseLogChannel)
	return s.sparseLogFile.Close()
}

func (s *SSTable) writeWAL() {
	defer s.sparseLogFile.Close()

	for record := range s.sparseLogChannel {
		entry := fmt.Sprintf("%s:%d\n", record.Key, s.sparseIndex[record.Key])
		_, err := s.sparseLogFile.WriteString(entry)
		if err != nil {
			log.Println("Error writing to sparse index WAL: ", err)
		}
	}
}

func (s *SSTable) recover(filePath string) {
	s.recoverSparseIndex(filePath)
	s.recoverBlocks()
}

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

func (s *SSTable) recoverBlocks() {
	blocks := make([]Block, 0)

	blockDir := path.Join(s.dirConfig.SSTableDir, fmt.Sprintf("%d", s.level))
	files, err := os.ReadDir(blockDir)
	if err != nil {
		s.blocks = blocks
		return
	}

	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 64)
		block, err := NewBlock(s.level, off, s.dirConfig)
		if err != nil {
			log.Println("Error creating block: ", err)
			continue
		}

		blocks = append(blocks, *block)
	}

	s.blocks = blocks
}
