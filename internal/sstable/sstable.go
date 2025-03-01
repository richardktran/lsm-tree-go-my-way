package sstable

import (
	"log"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
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
	sparseIndex map[string]int64 // key -> offset
	blocks      []Block
	createdAt   uint64 // Unix timestamp
	config      config.Config
}

func NewSSTable(level uint64, config config.Config) *SSTable {
	return &SSTable{
		level:       level,
		sparseIndex: make(map[string]int64),
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
	log.Println("Len of memtable: ", len(memtable.GetAll()))
	for _, record := range memtable.GetAll() {
		if block.IsMax(s.config.SSTableBlockSize) {
			s.blocks = append(s.blocks, *block)
			block.Close()
			block, err = NewBlock(s.level, baseOffset)
			if err != nil {
				log.Println("Error creating new block: ", err)
				return
			}
		}

		_, pos, err := block.Add(record)
		if err != nil {
			log.Println("Error adding record to block: ", err)
			return
		}

		baseOffset += uint64(pos)
	}

	s.blocks = append(s.blocks, *block)
}
