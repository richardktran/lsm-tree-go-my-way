package sstable

import (
	"os"
	"strconv"
	"testing"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/stretchr/testify/require"
)

func TestBlock(t *testing.T) {
	d, err := os.MkdirTemp("", "block-test")
	require.NoError(t, err)
	defer os.RemoveAll(d)

	block, err := NewBlock(1, 0, &config.DirectoryConfig{
		SSTableDir: d + "sstable",
	})

	require.NoError(t, err)
	require.NotNil(t, block)

	// Add a record to the block
	addRecord(t, block)
	readRecord(t, block)

	checkIsMaxSize(t, block)

	// Close the block
	require.NoError(t, block.Close())
}

func addRecord(t *testing.T, block *Block) {
	t.Helper()

	for i := 0; i < 5; i++ {
		record := kv.Record{
			Key:   kv.Key("k" + strconv.Itoa(i)),
			Value: kv.Value("v" + strconv.Itoa(i)),
		}

		noBytes, pos, err := block.Add(record)
		require.NoError(t, err)
		require.Equal(t, uint64(20), noBytes)
		require.Equal(t, uint64(i*20), pos)
	}
}

func readRecord(t *testing.T, block *Block) {
	t.Helper()

	for i := 0; i < 5; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		val := kv.Value("v" + strconv.Itoa(i))

		record, found := block.Get(key)
		require.True(t, found)
		require.Equal(t, val, record)
	}

	key := kv.Key("k222")

	record, found := block.Get(key)
	require.False(t, found)
	require.Empty(t, record)
}

func checkIsMaxSize(t *testing.T, block *Block) {
	t.Helper()

	// Add a record that exceeds the block size
	record := kv.Record{
		Key:   kv.Key("k" + strconv.Itoa(6)),
		Value: kv.Value("v" + strconv.Itoa(6)),
	}

	// Current size is 100 bytes (5 records)
	block.Add(record)
	// Now is 120 bytes

	require.True(t, block.IsMax(100))
	require.False(t, block.IsMax(121))
}
