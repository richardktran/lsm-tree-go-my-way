package sstable

import (
	"log"

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
	level       uint64
	sparseIndex map[kv.Key]uint64 // key -> offset
	blocks      []Block
	createdAt   uint64 // Unix timestamp
	config      config.Config
}

func NewSSTable(level uint64, config config.Config) *SSTable {
	return &SSTable{
		level:       level,
		sparseIndex: make(map[kv.Key]uint64),
		blocks:      make([]Block, 0),
		createdAt:   0,
		config:      config,
	}
}

func (s *SSTable) Flush(memtable memtable.MemTable) {
	var baseOffset uint64 = 0
	block, err := NewBlock(s.level, baseOffset)

	if err != nil {
		log.Println("Error creating new block: ", err)
		return
	}
	log.Println("Flushing memtable to SSTable")
	for _, record := range memtable.GetAll() {
		if block.IsMax(s.config.SSTableBlockSize) {
			s.blocks = append(s.blocks, *block)
			block.Close()
			block, err = NewBlock(s.level, baseOffset)
			if err != nil {
				log.Println("Error creating new block: ", err)
				return
			}

			s.sparseIndex[record.Key] = baseOffset
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
