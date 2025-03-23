package sstable

import (
	"bufio"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/richardktran/lsm-tree-go-my-way/internal/memtable"
	"github.com/stretchr/testify/require"
)

func TestSSTable(t *testing.T) {
	d, err := os.MkdirTemp("", "sstable-test")
	require.NoError(t, err)
	defer os.RemoveAll(d)

	dirConfig := &config.DirectoryConfig{
		SSTableDir:     d + "/sstable",
		WALDir:         d + "/wal",
		SparseIndexDir: d + "/indexes",
	}
	cfg := &config.Config{
		SparseWALBufferSize: 2,
		SSTableBlockSize:    40, // block size is 40 bytes (2 records)
	}

	sstableId := uint64(1)

	memtable := memtable.NewMemTable()
	for i := 1; i <= 4; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		memtable.Set(key, value)
	}

	testCloseSSTable(t, cfg, dirConfig)

	sstable := NewSSTable(sstableId, cfg, dirConfig)
	testFlushFromMemTableToSSTable(t, memtable, sstable, cfg, dirConfig)

	sstable.Close()

	sstable = NewSSTable(sstableId, cfg, dirConfig)
	testRecoverStateOfSSTable(t, sstable, cfg, dirConfig)

	testFindADeletedKey(t, d)

	sstable.Close()
}

func testCloseSSTable(t *testing.T, cfg *config.Config, dirConfig *config.DirectoryConfig) {
	t.Helper()

	sstable := NewSSTable(uint64(1), cfg, dirConfig)

	require.NoError(t, sstable.Close())
}

func testFlushFromMemTableToSSTable(t *testing.T, memtable *memtable.MemTable, sstable *SSTable, cfg *config.Config, dirConfig *config.DirectoryConfig) {
	t.Helper()

	sstable.Flush(*memtable)

	time.Sleep(2 * time.Second)

	require.Equal(t, 2, len(sstable.blocks)) // 4 records => 2 blocks

	checkSSTableFiles(t, sstable.id, cfg, dirConfig)
	checkSparseIndex(t, sstable, map[string]uint64{"k1": 0, "k3": 40})
}

func testRecoverStateOfSSTable(t *testing.T, sstable *SSTable, cfg *config.Config, dirConfig *config.DirectoryConfig) {
	sstableId := sstable.id

	// Sleep to make sure blocks and index are recovered
	time.Sleep(2 * time.Second)

	require.Equal(t, 2, len(sstable.blocks)) // 4 records => 2 blocks

	checkSSTableFiles(t, sstableId, cfg, dirConfig)
	checkSparseIndex(t, sstable, map[string]uint64{"k1": 0, "k3": 40})
}

func testFindADeletedKey(t *testing.T, rootDir string) {
	dirConfig := &config.DirectoryConfig{
		SSTableDir:     rootDir + "/sstable",
		WALDir:         rootDir + "/wal",
		SparseIndexDir: rootDir + "/indexes",
	}
	cfg := &config.Config{
		SparseWALBufferSize: 2,
		SSTableBlockSize:    40, // block size is 40 bytes (2 records)
	}

	sstableId := uint64(1)

	memtable := memtable.NewMemTable()
	for i := 1; i <= 4; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		memtable.Set(key, value)
	}

	key := kv.Key("k2")
	value := kv.Value("")
	memtable.Delete(key)

	sstable := NewSSTable(sstableId, cfg, dirConfig)
	defer sstable.Close()

	sstable.Flush(*memtable)

	sstable.FlushWait()

	// read k2 from sstable
	v, found := sstable.Get(key)
	require.True(t, found) // Found the tombstone record
	require.Equal(t, value, v)
}

func checkSSTableFiles(t *testing.T, sstableId uint64, cfg *config.Config, dirConfig *config.DirectoryConfig) {
	for i := 0; i < 2; i++ {
		filePath := path.Join(dirConfig.SSTableDir, strconv.Itoa(int(sstableId)), strconv.Itoa(i*int(cfg.SSTableBlockSize))+".sst")
		_, err := os.Stat(filePath)
		require.NoError(t, err)
	}

	indexFilePath := path.Join(dirConfig.SparseIndexDir, strconv.Itoa(int(sstableId))+".index")
	_, err := os.Stat(indexFilePath)
	require.NoError(t, err)
}

func checkSparseIndex(t *testing.T, sstable *SSTable, expected map[string]uint64) {
	require.Equal(t, len(expected), len(sstable.sparseIndex))
	for key, offset := range expected {
		require.Equal(t, offset, sstable.sparseIndex[kv.Key(key)])
	}

	indexFilePath := path.Join(sstable.dirConfig.SparseIndexDir, strconv.Itoa(int(sstable.id))+".index")
	indexFile, err := os.Open(indexFilePath)
	require.NoError(t, err)
	defer indexFile.Close()

	scanner := bufio.NewScanner(indexFile)
	result := make(map[string]uint64)
	for scanner.Scan() {
		line := scanner.Text()
		require.NotEmpty(t, line)

		parts := strings.Split(line, ":")
		require.Equal(t, 2, len(parts))

		key := parts[0]
		offset, err := strconv.Atoi(parts[1])
		require.NoError(t, err)

		result[key] = uint64(offset)
	}

	require.Equal(t, len(expected), len(result))
	for key, offset := range expected {
		require.Equal(t, offset, result[key])
	}
}
