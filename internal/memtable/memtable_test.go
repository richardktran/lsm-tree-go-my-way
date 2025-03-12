package memtable

import (
	"strconv"
	"testing"

	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/stretchr/testify/require"
)

func TestMemtable(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, table *MemTable){
		"get/set key (value)":               testGetSetKeyOn,
		"delete key":                        testDeleteKeyOn,
		"get size of the list":              testGetSizeOfTheList,
		"get all with sorted order":         testGetAllWithSortedOrder,
		"insert a record with the same key": testInsertRecordWithTheSameKey,
		"clone the list":                    testCloneTheList,
	} {
		t.Run(scenario, func(t *testing.T) {
			list := NewMemTable()
			fn(t, list)
		})
	}
}

func testGetSetKeyOn(t *testing.T, table *MemTable) {
	for i := 5; i >= 0; i-- {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		table.Set(key, value)
		require.True(t, testOrderOfArray(t, table))

		v, found := table.Get(key)
		require.True(t, found)
		require.Equal(t, value, v)
	}

	// Check the order of the table
	require.True(t, testOrderOfArray(t, table))
}

func testDeleteKeyOn(t *testing.T, table *MemTable) {
	for i := 0; i < 5; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		table.Set(key, value)
	}

	// Delete the key
	table.Delete(kv.Key("k3"))
	_, found := table.Get(kv.Key("k3"))
	require.False(t, found)

	// Check the order of the table
	require.True(t, testOrderOfArray(t, table))
}

func testGetSizeOfTheList(t *testing.T, table *MemTable) {
	for i := 0; i < 5; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		table.Set(key, value)
	}

	require.Equal(t, 20, table.Size()) // 4 bytes per record

	table.Set(kv.Key("k33232"), kv.Value("khoa dep trai qua"))
	require.Equal(t, 43, table.Size())
}

func testGetAllWithSortedOrder(t *testing.T, table *MemTable) {
	for i := 0; i < 5; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		table.Set(key, value)
	}

	allRecords := table.GetAll()
	for i := 1; i < len(allRecords); i++ {
		require.True(t, allRecords[i-1].Key <= allRecords[i].Key)
	}
}

func testInsertRecordWithTheSameKey(t *testing.T, table *MemTable) {
	table.Set(kv.Key("k1"), kv.Value("v1"))
	v, found := table.Get(kv.Key("k1"))
	require.True(t, found)
	require.Equal(t, kv.Value("v1"), v)

	table.Set(kv.Key("k1"), kv.Value("v2"))

	v, found = table.Get(kv.Key("k1"))
	require.True(t, found)
	require.Equal(t, kv.Value("v2"), v)
}

func testCloneTheList(t *testing.T, table *MemTable) {
	for i := 0; i < 5; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		table.Set(key, value)
	}

	clone := table.Clone()
	require.Equal(t, table.Size(), clone.Size())
	require.Equal(t, table.GetAll(), clone.GetAll())

	// check different memory address
	table.Set(kv.Key("k1"), kv.Value("v2"))
	currentValue, found := table.Get(kv.Key("k1"))
	require.True(t, found)
	require.Equal(t, kv.Value("v2"), currentValue)

	clonedValue, found := clone.Get(kv.Key("k1"))
	require.True(t, found)
	require.Equal(t, kv.Value("v1"), clonedValue)
}

func testOrderOfArray(t *testing.T, table *MemTable) bool {
	t.Helper()
	data := table.GetAll()
	for i := 1; i < len(data); i++ {
		if data[i-1].Key > data[i].Key {
			return false
		}
	}

	return true
}
