package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path"

	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type Block struct {
	file       *os.File
	baseOffset uint64
	buf        *bufio.Writer
}

func NewBlock(level, baseOffset uint64) (*Block, error) {
	file, err := os.OpenFile(
		path.Join("data", fmt.Sprintf("%d.%d.sst", level, baseOffset)),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	return &Block{
		file:       file,
		baseOffset: baseOffset,
		buf:        bufio.NewWriter(file),
	}, nil
}

func (b *Block) Add(record kv.Record) (n uint64, pos uint64, err error) {
	keyLen := uint64(len(record.Key))
	valueLen := uint64(len(record.Value))

	key := []byte(record.Key)
	value := []byte(record.Value)

	if err := binary.Write(b.buf, enc, keyLen); err != nil {
		return 0, 0, err
	}

	keyBytes, err := b.buf.Write(key)

	if err != nil {
		return 0, 0, err
	}

	if err := binary.Write(b.buf, enc, valueLen); err != nil {
		return 0, 0, err
	}

	valueBytes, err := b.buf.Write(value)

	if err != nil {
		return 0, 0, err
	}

	b.buf.Flush()

	numberOfByte := 2*lenWidth + keyBytes + valueBytes

	return uint64(numberOfByte), b.baseOffset, nil
}

func (b *Block) IsMax(threshold uint64) bool {
	stat, _ := b.file.Stat()
	return stat.Size() >= int64(threshold)
}

func (b *Block) Close() error {
	return b.file.Close()
}
