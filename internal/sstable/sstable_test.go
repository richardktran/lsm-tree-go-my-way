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
	for scenario, fn := range map[string]func(t *testing.T, dirConfig *config.DirectoryConfig){
		"Test close SSTable":                  testCloseSSTable,
		"Test Flush from MemTable to SSTable": testFlushFromMemTableToSSTable,
		// "Read data flushed to SSTable":        testReadDataFlushed,
		// "Recover blocks":                      testBlocksRecovery,
		// "Recover SparseIndex":                 testSparseIndexRecovery,
	} {
		t.Run(scenario, func(t *testing.T) {
			d, err := os.MkdirTemp("", "sstable-test")
			require.NoError(t, err)
			defer os.RemoveAll(d)

			dirConfig := &config.DirectoryConfig{
				SSTableDir:     d + "/sstable",
				WALDir:         d + "/wal",
				SparseIndexDir: d + "/indexes",
			}
			fn(t, dirConfig)
		})
	}
}

func testCloseSSTable(t *testing.T, dirConfig *config.DirectoryConfig) {
	cfg := &config.Config{
		SparseWALBufferSize: 3,
		SSTableBlockSize:    40,
	}

	sstable := NewSSTable(uint64(1), cfg, dirConfig)

	require.NoError(t, sstable.Close())
}

func testFlushFromMemTableToSSTable(t *testing.T, dirConfig *config.DirectoryConfig) {
	sstableId := uint64(1)

	memtable := memtable.NewMemTable()
	for i := 1; i <= 4; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		memtable.Set(key, value)
	}

	cfg := &config.Config{
		SparseWALBufferSize: 2,
		SSTableBlockSize:    40, // block size is 40 bytes (2 records)
	}

	sstable := NewSSTable(sstableId, cfg, dirConfig)
	defer sstable.Close()

	sstable.Flush(*memtable)

	time.Sleep(2 * time.Second)

	require.Equal(t, 2, len(sstable.blocks)) // 4 records => 2 blocks

	// Check the block files
	for i := 0; i < 2; i++ {
		filePath := path.Join(dirConfig.SSTableDir, strconv.Itoa(int(sstableId)), strconv.Itoa(i*int(cfg.SSTableBlockSize))+".sst")
		_, err := os.Stat(filePath)
		require.NoError(t, err)
	}

	// Check index file
	indexFilePath := path.Join(dirConfig.SparseIndexDir, strconv.Itoa(int(sstableId))+".index")
	_, err := os.Stat(indexFilePath)
	require.NoError(t, err)

	// Check the sparse index
	require.Equal(t, 2, len(sstable.sparseIndex)) // k1:0, k3:40
	require.Equal(t, uint64(0), sstable.sparseIndex[kv.Key("k1")])
	require.Equal(t, uint64(40), sstable.sparseIndex[kv.Key("k3")])

	// Check content of the index file
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

	require.Equal(t, 2, len(result))
	require.Equal(t, uint64(0), result["k1"])
	require.Equal(t, uint64(40), result["k3"])
}
