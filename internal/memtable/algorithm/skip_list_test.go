package algorithm

import (
	"strconv"
	"testing"

	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/stretchr/testify/require"
)

func TestSkipList(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, sl *SkipList){
		"get/set key on SkipList":           testSkipListGetSet,
		"delete key on SkipList":            testSkipListDelete,
		"get size of the SkipList":          testSkipListSize,
		"get all records in sorted order":   testSkipListGetAllSorted,
		"insert a record with the same key": testSkipListUpsert,
		"clone the SkipList":                testSkipListClone,
		"set tombstone (nil value)":         testSkipListTombstone,
		"build SkipList from records":       testBuildSkipList,
	} {
		t.Run(scenario, func(t *testing.T) {
			sl := NewSkipList()
			fn(t, sl)
		})
	}
}

func testSkipListGetSet(t *testing.T, sl *SkipList) {
	// Insert keys out of order to confirm sorted traversal.
	for i := 5; i >= 0; i-- {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		sl.Set(key, value)

		v, found := sl.Get(key)
		require.True(t, found)
		require.Equal(t, value, v)

		require.True(t, skipListSorted(sl), "order invariant violated after inserting %s", key)
	}
}

func testSkipListDelete(t *testing.T, sl *SkipList) {
	for i := 0; i < 5; i++ {
		sl.Set(kv.Key("k"+strconv.Itoa(i)), kv.Value("v"+strconv.Itoa(i)))
	}

	sl.Delete(kv.Key("k3"))
	_, found := sl.Get(kv.Key("k3"))
	require.False(t, found)

	// Remaining keys are still accessible and in order.
	require.True(t, skipListSorted(sl))
	require.Equal(t, 4, len(sl.GetAll()))
}

func testSkipListSize(t *testing.T, sl *SkipList) {
	for i := 0; i < 5; i++ {
		sl.Set(kv.Key("k"+strconv.Itoa(i)), kv.Value("v"+strconv.Itoa(i)))
	}
	// "k0"+"v0" = 4 bytes, × 5 = 20 bytes
	require.Equal(t, 20, sl.Size())

	sl.Set(kv.Key("k33232"), kv.Value("khoa dep trai qua"))
	require.Equal(t, 43, sl.Size())
}

func testSkipListGetAllSorted(t *testing.T, sl *SkipList) {
	for i := 0; i < 5; i++ {
		sl.Set(kv.Key("k"+strconv.Itoa(i)), kv.Value("v"+strconv.Itoa(i)))
	}

	records := sl.GetAll()
	for i := 1; i < len(records); i++ {
		require.True(t, records[i-1].Key <= records[i].Key,
			"records not sorted: %s > %s", records[i-1].Key, records[i].Key)
	}
}

func testSkipListUpsert(t *testing.T, sl *SkipList) {
	sl.Set(kv.Key("k1"), kv.Value("v1"))
	v, found := sl.Get(kv.Key("k1"))
	require.True(t, found)
	require.Equal(t, kv.Value("v1"), v)

	// Overwrite — size should stay the same (same key/value length).
	sl.Set(kv.Key("k1"), kv.Value("v2"))
	v, found = sl.Get(kv.Key("k1"))
	require.True(t, found)
	require.Equal(t, kv.Value("v2"), v)
	require.Equal(t, 1, len(sl.GetAll()), "upsert must not create duplicate nodes")
}

func testSkipListClone(t *testing.T, sl *SkipList) {
	for i := 0; i < 5; i++ {
		sl.Set(kv.Key("k"+strconv.Itoa(i)), kv.Value("v"+strconv.Itoa(i)))
	}

	clone := sl.Clone()
	require.Equal(t, sl.Size(), clone.Size())
	require.Equal(t, sl.GetAll(), clone.GetAll())

	// Mutating the original must not affect the clone.
	sl.Set(kv.Key("k1"), kv.Value("v2"))
	origVal, _ := sl.Get(kv.Key("k1"))
	require.Equal(t, kv.Value("v2"), origVal)

	cloneVal, found := clone.Get(kv.Key("k1"))
	require.True(t, found)
	require.Equal(t, kv.Value("v1"), cloneVal)
}

func testSkipListTombstone(t *testing.T, sl *SkipList) {
	sl.Set(kv.Key("k1"), kv.Value("v1"))
	sizeWithValue := sl.Size()

	// Store a nil value (tombstone).
	sl.Set(kv.Key("k1"), nil)
	val, found := sl.Get(kv.Key("k1"))
	require.True(t, found, "tombstone node must be found")
	require.Nil(t, val, "tombstone value must be nil")

	// Size should reflect key-only (len("k1") = 2, len(nil) = 0).
	require.Less(t, sl.Size(), sizeWithValue)
	require.Equal(t, len("k1"), sl.Size())
}

func testBuildSkipList(t *testing.T, _ *SkipList) {
	records := []kv.Record{
		{Key: "k3", Value: kv.Value("v3")},
		{Key: "k1", Value: kv.Value("v1")},
		{Key: "k2", Value: kv.Value("v2")},
	}
	sl := BuildSkipList(records)

	all := sl.GetAll()
	require.Len(t, all, 3)
	require.Equal(t, kv.Key("k1"), all[0].Key)
	require.Equal(t, kv.Key("k2"), all[1].Key)
	require.Equal(t, kv.Key("k3"), all[2].Key)

	require.Equal(t, 12, sl.Size()) // 3×(2+2) = 12 bytes
}

// skipListSorted returns true when level-0 traversal is in non-decreasing key order.
func skipListSorted(sl *SkipList) bool {
	node := sl.head.next[0]
	for node != nil && node.next[0] != nil {
		if node.key > node.next[0].key {
			return false
		}
		node = node.next[0]
	}
	return true
}
