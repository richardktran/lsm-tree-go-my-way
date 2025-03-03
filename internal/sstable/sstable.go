package sstable

import (
	"fmt"
	"log"
	"os"
	"path"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/richardktran/lsm-tree-go-my-way/internal/memtable"
)

/*
SSTable is a sorted string table
File name: <layer>.offset.sst
  - layer: the level of the SSTable
  - offset: the offset of the SSTable in the level
  - sst: SSTable file extension

Each file represents a block
*/
type SSTable struct {
	level            uint64
	sparseIndex      map[kv.Key]uint64 // key -> offset
	blocks           []Block
	createdAt        uint64 // Unix timestamp
	config           config.Config
	sparseLogFile    *os.File
	sparseLogChannel chan kv.Record // write-ahead log for SparseIndex
}

func NewSSTable(level uint64, config config.Config, dirConfig config.DirectoryConfig) *SSTable {
	s := &SSTable{
		level:            level,
		sparseIndex:      make(map[kv.Key]uint64),
		blocks:           make([]Block, 0),
		createdAt:        0,
		config:           config,
		sparseLogChannel: make(chan kv.Record, config.SparseWALBufferSize),
	}

	folderPath := path.Join(dirConfig.SparseIndexDir)

	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		os.MkdirAll(dirConfig.SparseIndexDir, 0755)
	}

	filePath := path.Join(dirConfig.SparseIndexDir, fmt.Sprintf("%d.index", level))
	sparseLogFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		log.Println("Error opening sparse index file: ", err)
		return s
	}

	s.sparseLogFile = sparseLogFile

	go s.writeWAL()

	return s

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

func (s *SSTable) Flush(memtable memtable.MemTable, dirConfig config.DirectoryConfig) {
	var baseOffset uint64 = 0
	block, err := NewBlock(s.level, baseOffset, dirConfig)

	if err != nil {
		log.Println("Error creating new block: ", err)
		return
	}
	log.Println("Flushing memtable to SSTable")
	for _, record := range memtable.GetAll() {
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
