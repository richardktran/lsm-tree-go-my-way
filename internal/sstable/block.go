package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
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

func NewBlock(level, baseOffset uint64, dirConfig config.DirectoryConfig) (*Block, error) {
	sstableFolder := path.Join(dirConfig.SSTableDir, fmt.Sprintf("%d", level))
	if _, err := os.Stat(sstableFolder); os.IsNotExist(err) {
		os.MkdirAll(sstableFolder, 0755)
	}

	filePath := path.Join(sstableFolder, fmt.Sprintf("%d.sst", baseOffset))
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

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

func (b *Block) Get(key kv.Key) (kv.Value, bool) {
	b.buf.Flush()

	_, err := b.file.Seek(0, io.SeekStart)
	if err != nil {
		return "", false
	}

	reader := bufio.NewReader(b.file)

	for {
		var keyLen uint64
		err := binary.Read(reader, enc, &keyLen)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", false
		}

		keyData := make([]byte, keyLen)
		_, err = io.ReadFull(reader, keyData)
		if err != nil {
			return "", false
		}

		var valueLen uint64
		err = binary.Read(reader, enc, &valueLen)
		if err != nil {
			return "", false
		}

		value := make([]byte, valueLen)
		_, err = io.ReadFull(reader, value)
		if err != nil {
			return "", false
		}

		if kv.Key(keyData) == key {
			return kv.Value(value), true
		}
	}

	return "", false
}

func (b *Block) IsMax(threshold uint64) bool {
	stat, _ := b.file.Stat()
	return stat.Size() >= int64(threshold)
}

func (b *Block) Close() error {
	return b.file.Close()
}
