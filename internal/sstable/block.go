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

// enc is the binary encoding used for writing and reading
var (
	enc = binary.BigEndian
)

// lenWidth is the byte size to represent the length of the key and value
const (
	lenWidth = 8
)

type Block struct {
	file           *os.File
	baseOffset     uint64
	nextItemOffset uint64
	buf            *bufio.Writer
}

/*
NewBlock creates or opens a block file
From id of sstable and baseOffset, we can identify the block file on disk
*/
func NewBlock(sstableId, baseOffset uint64, dirConfig *config.DirectoryConfig) (*Block, error) {
	sstableFolder := path.Join(dirConfig.SSTableDir, fmt.Sprintf("%d", sstableId))
	if _, err := os.Stat(sstableFolder); os.IsNotExist(err) {
		os.MkdirAll(sstableFolder, 0755)
	}

	filePath := path.Join(sstableFolder, fmt.Sprintf("%d.sst", baseOffset))
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		return nil, err
	}

	return &Block{
		file:           file,
		baseOffset:     baseOffset,
		nextItemOffset: 0,
		buf:            bufio.NewWriter(file),
	}, nil
}

/*
Add writes a record to the block file with format: <keyLen><key><valueLen><value>

- keyLen: the length of the key
- key: the key of the record
- valueLen: the length of the value
- value: the value of the record
Number of bytes written to the block file is returned: lenWidth + keyBytes + lenWidth + valueBytes
*/
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
	b.nextItemOffset += uint64(numberOfByte)

	return uint64(numberOfByte), b.nextItemOffset - uint64(numberOfByte), nil
}

// Get reads from the beginning of the block file and returns the value of the key if found
func (b *Block) Get(key kv.Key) (kv.Value, bool) {
	b.buf.Flush()

	_, err := b.file.Seek(0, io.SeekStart)
	if err != nil {
		return kv.Value(""), false
	}

	reader := bufio.NewReader(b.file)

	for {
		var keyLen uint64
		err := binary.Read(reader, enc, &keyLen)
		if err == io.EOF {
			break
		}
		if err != nil {
			return kv.Value(""), false
		}

		keyData := make([]byte, keyLen)
		_, err = io.ReadFull(reader, keyData)
		if err != nil {
			return kv.Value(""), false
		}

		var valueLen uint64
		err = binary.Read(reader, enc, &valueLen)
		if err != nil {
			return kv.Value(""), false
		}

		value := make([]byte, valueLen)
		_, err = io.ReadFull(reader, value)
		if err != nil {
			return kv.Value(""), false
		}

		if kv.Key(keyData) == key {
			if valueLen == 0 {
				return kv.Value(""), false
			}

			return kv.Value(value), true
		}
	}

	return kv.Value(""), false
}

// IsMax checks if the block file has reached the threshold size, used to determine if a new block is needed
func (b *Block) IsMax(threshold uint64) bool {
	stat, _ := b.file.Stat()
	return stat.Size() >= int64(threshold)
}

// Close closes the block file
func (b *Block) Close() error {
	return b.file.Close()
}
