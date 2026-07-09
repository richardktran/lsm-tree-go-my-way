package lsmtree

import (
	"fmt"
	"os"
	"testing"

	"github.com/richardktran/lsm-tree-go-my-way/internal/config"
	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/stretchr/testify/require"
)

// newCompactionStore creates an LSMTreeStore with a tiny MemTable threshold so
// flushes (and therefore SSTable creation) can be triggered with just a handful
// of writes. compactionThreshold controls when compaction fires automatically
// (0 disables automatic compaction).
func newCompactionStore(t *testing.T, compactionThreshold int) (*LSMTreeStore, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "compaction-test")
	require.NoError(t, err)

	// MemTableSizeThreshold = 10 bytes; each record "kN":"vN" is 4 bytes,
	// so every 3rd Set call crosses the threshold and triggers a flush.
	appConfig := &config.Config{
		MemTableSizeThreshold: 10,
		SSTableBlockSize:      40,
		SparseWALBufferSize:   10,
		BloomFilterSize:       1000,
		BloomFilterHashCount:  3,
		RootDataDir:           dir,
		CompactionThreshold:   compactionThreshold,
	}
	dirConfig := &config.DirectoryConfig{
		WALDir:         "wal",
		SSTableDir:     "sstables",
		SparseIndexDir: "indexes",
	}

	store := NewStore(appConfig, dirConfig)
	return store, func() {
		store.Close()
		os.RemoveAll(dir)
	}
}

// forceFlush writes n records with the given prefix into the store, triggering
// at least one SSTable flush. Each record is "prefix_kN":"prefix_vN".
// It then waits for all background flushes to complete.
func forceFlush(store *LSMTreeStore, prefix string, n int) {
	for i := 0; i < n; i++ {
		key := kv.Key(fmt.Sprintf("%s_k%d", prefix, i))
		val := kv.Value(fmt.Sprintf("%s_v%d", prefix, i))
		store.Set(key, val)
	}
	store.WaitForFlush()
}

// sstableCount returns the current number of SSTables in the store.
// The sstableLock is not acquired intentionally — tests call this after
// WaitForFlush, when no background goroutine is modifying s.ssTables.
func sstableCount(store *LSMTreeStore) int {
	store.sstableLock.RLock()
	defer store.sstableLock.RUnlock()
	return len(store.ssTables)
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

func TestCompaction(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T){
		"Compact is no-op when below threshold":                   testNoCompactionBelowThreshold,
		"Compact merges SSTables into one":                        testCompactionMergesSSTables,
		"Compact drops tombstones":                                testCompactionDropsTombstones,
		"Compact deduplicates keys keeping newest value":          testCompactionDeduplicatesKeys,
		"Auto compaction triggered after flush exceeds threshold": testAutoCompactionTriggeredOnFlush,
	} {
		t.Run(scenario, func(t *testing.T) {
			fn(t)
		})
	}
}

// ---------------------------------------------------------------------------
// Individual test functions
// ---------------------------------------------------------------------------

// testNoCompactionBelowThreshold verifies that Compact() is a no-op when the
// number of SSTables is below the compaction threshold.
func testNoCompactionBelowThreshold(t *testing.T) {
	store, cleanup := newCompactionStore(t, 10) // threshold = 10
	defer cleanup()

	// Create 3 SSTables (well below threshold of 10).
	forceFlush(store, "a", 5)
	forceFlush(store, "b", 5)
	forceFlush(store, "c", 5)

	before := sstableCount(store)
	require.GreaterOrEqual(t, before, 1, "expected at least one SSTable before compaction")

	err := store.Compact()
	require.NoError(t, err)

	// SSTable count must not have changed.
	after := sstableCount(store)
	require.Equal(t, before, after, "Compact() must be a no-op when below the threshold")
}

// testCompactionMergesSSTables verifies that, after compaction, multiple
// SSTables are merged into a single one and all data remains readable.
func testCompactionMergesSSTables(t *testing.T) {
	store, cleanup := newCompactionStore(t, 0) // threshold = 0 → manual only
	defer cleanup()

	// Write 3 batches to create ≥3 SSTables.
	forceFlush(store, "a", 5)
	forceFlush(store, "b", 5)
	forceFlush(store, "c", 5)

	require.GreaterOrEqual(t, sstableCount(store), 2, "expected multiple SSTables before compaction")

	err := store.Compact()
	require.NoError(t, err)

	require.Equal(t, 1, sstableCount(store), "expected exactly 1 SSTable after compaction")

	// All data must still be readable.
	for _, prefix := range []string{"a", "b", "c"} {
		for i := 0; i < 5; i++ {
			key := kv.Key(fmt.Sprintf("%s_k%d", prefix, i))
			expectedVal := kv.Value(fmt.Sprintf("%s_v%d", prefix, i))
			val, found := store.Get(key)
			require.True(t, found, "key %q missing after compaction", key)
			require.Equal(t, expectedVal, val, "wrong value for key %q after compaction", key)
		}
	}
}

// testCompactionDropsTombstones verifies that deleted keys are not present in
// the compacted SSTable and cannot be read back.
func testCompactionDropsTombstones(t *testing.T) {
	store, cleanup := newCompactionStore(t, 0)
	defer cleanup()

	// Write keys and then delete some; flush both batches.
	forceFlush(store, "x", 5) // x_k0…x_k4 present

	// Delete x_k1 and x_k3 — these will create tombstones in the memtable.
	store.Delete(kv.Key("x_k1"))
	store.Delete(kv.Key("x_k3"))
	forceFlush(store, "y", 5) // triggers another flush that includes the tombstones

	require.GreaterOrEqual(t, sstableCount(store), 1)

	err := store.Compact()
	require.NoError(t, err)

	require.Equal(t, 1, sstableCount(store))

	// Deleted keys must not be found.
	for _, k := range []string{"x_k1", "x_k3"} {
		val, found := store.Get(kv.Key(k))
		require.False(t, found, "deleted key %q must not be found after compaction, got %q", k, val)
	}

	// Non-deleted keys must still be accessible.
	for _, k := range []string{"x_k0", "x_k2", "x_k4"} {
		_, found := store.Get(kv.Key(k))
		require.True(t, found, "key %q must be found after compaction", k)
	}
}

// testCompactionDeduplicatesKeys verifies that when the same key appears in
// multiple SSTables, only the newest value survives compaction.
func testCompactionDeduplicatesKeys(t *testing.T) {
	store, cleanup := newCompactionStore(t, 0)
	defer cleanup()

	// First flush: set "shared" = "old_value"
	store.Set(kv.Key("shared"), kv.Value("old_value"))
	forceFlush(store, "batch1", 5) // ensures "shared" is flushed to an SSTable

	// Second flush: overwrite "shared" = "new_value"
	store.Set(kv.Key("shared"), kv.Value("new_value"))
	forceFlush(store, "batch2", 5) // ensures the overwrite is flushed

	require.GreaterOrEqual(t, sstableCount(store), 2, "expected multiple SSTables before dedup test")

	err := store.Compact()
	require.NoError(t, err)

	require.Equal(t, 1, sstableCount(store))

	// Only the newest value must survive.
	val, found := store.Get(kv.Key("shared"))
	require.True(t, found)
	require.Equal(t, kv.Value("new_value"), val, "expected newest value after compaction, got %q", val)
}

// testAutoCompactionTriggeredOnFlush verifies that when the number of SSTables
// reaches the configured CompactionThreshold after a flush, compaction is
// triggered automatically and reduces the SSTable count.
func testAutoCompactionTriggeredOnFlush(t *testing.T) {
	const threshold = 3
	store, cleanup := newCompactionStore(t, threshold)
	defer cleanup()

	// Write enough data to create at least `threshold` SSTables.
	// Each forceFlush creates ≥1 SSTable.
	forceFlush(store, "a", 5)
	forceFlush(store, "b", 5)
	forceFlush(store, "c", 5)
	forceFlush(store, "d", 5) // this extra flush should trigger auto-compaction

	// After automatic compaction the SSTable count must be below threshold.
	count := sstableCount(store)
	require.Less(t, count, threshold, "expected SSTable count < threshold after auto-compaction, got %d", count)

	// All data must still be readable.
	for _, prefix := range []string{"a", "b", "c", "d"} {
		for i := 0; i < 5; i++ {
			key := kv.Key(fmt.Sprintf("%s_k%d", prefix, i))
			expectedVal := kv.Value(fmt.Sprintf("%s_v%d", prefix, i))
			val, found := store.Get(key)
			require.True(t, found, "key %q missing after auto-compaction", key)
			require.Equal(t, expectedVal, val)
		}
	}
}
