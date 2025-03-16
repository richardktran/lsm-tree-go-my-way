package lsmtree

import (
	"os"
	"strconv"
	"testing"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/stretchr/testify/require"
)

func TestLSMTreeStore(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, store *LSMTreeStore){
		"Set/Get key on Store":           testGetSetKeyOnStore,
		"Check trigger flush to SSTable": testTriggerFlushToSSTable,
		"Test Delete key on Store":       testDeleteKeyOnStore,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "server-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			appConfig := &config.Config{
				Host:                  "127.0.0.1",
				Port:                  "6969",
				MemTableSizeThreshold: 30, // bytes
				SSTableBlockSize:      20, //bytes
				SparseWALBufferSize:   2,  // records
				RootDataDir:           dir,
			}
			dirConfig := &config.DirectoryConfig{
				WALDir:         "wal",
				SSTableDir:     "sstables",
				SparseIndexDir: "indexes",
			}

			store := NewStore(appConfig, dirConfig)
			defer store.Close()

			fn(t, store)
		})
	}
}

func testGetSetKeyOnStore(t *testing.T, store *LSMTreeStore) {
	for i := 0; i < 3; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		store.Set(key, value)
		v, found := store.Get(key)
		require.True(t, found)
		require.Equal(t, value, v)
	}

	// Overwrite key
	store.Set(kv.Key("k3"), kv.Value("v3333"))
	v, found := store.Get(kv.Key("k3"))
	require.True(t, found)
	require.Equal(t, kv.Value("v3333"), v)

	// Find non-existent key
	v, found = store.Get(kv.Key("k4"))
	require.False(t, found)
	require.Equal(t, kv.Value(""), v)
}

// TODO: Currently failed because the flush flow is wrong, will fix it later
func testTriggerFlushToSSTable(t *testing.T, store *LSMTreeStore) {
	// Threshold is 30 bytes, init with 7 records will not trigger flush (each record is 4 bytes)
	for i := 0; i < 7; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		store.Set(key, value)
		v, found := store.Get(key)
		require.True(t, found)
		require.Equal(t, value, v)
	}

	sstableDir, err := os.ReadDir(store.dirConfig.SSTableDir)
	require.NoError(t, err)
	require.Empty(t, sstableDir)

	for i := 7; i < 14; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		store.Set(key, value)
		v, found := store.Get(key)
		require.Equal(t, value, v)
		require.True(t, found)
	}

	sstableDir, err = os.ReadDir(store.dirConfig.SSTableDir)
	require.NoError(t, err)
	require.NotEmpty(t, sstableDir)
}

func testDeleteKeyOnStore(t *testing.T, store *LSMTreeStore) {
	for i := 0; i < 3; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		store.Set(key, value)
		v, found := store.Get(key)
		require.True(t, found)
		require.Equal(t, value, v)
	}

	store.Delete(kv.Key("k1"))
	v, found := store.Get(kv.Key("k1"))
	require.False(t, found)
	require.Equal(t, kv.Value(""), v)

	// TODO: Test delete after recovery from WAL
	// TODO: Delete after flush to SSTable, SHOULD NOT still get value after flush to sstable but delete in memtable
}
