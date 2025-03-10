package algorithm

import (
	"strconv"
	"testing"

	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
	"github.com/stretchr/testify/require"
)

func TestSortedArray(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, list *SortedArray){
		"get/set key (value) on SortedArray": testGetSetKeyOnSortedArray,
		"delete key on SortedArray":          testDeleteKeyOnSortedArray,
		"get size of the list":               testGetSizeOfTheList,
		"get all with sorted order":          testGetAllWithSortedOrder,
		"insert a record with the same key":  testInsertRecordWithTheSameKey,
	} {
		t.Run(scenario, func(t *testing.T) {
			list := NewSortedArray()
			fn(t, list)
		})
	}
}

func testGetSetKeyOnSortedArray(t *testing.T, list *SortedArray) {
	for i := 5; i >= 0; i-- {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		list.Set(key, value)
		require.True(t, testOrderOfArray(t, list))

		v, found := list.Get(key)
		require.True(t, found)
		require.Equal(t, value, v)
	}

	// Check the order of the list
	require.True(t, testOrderOfArray(t, list))
}

func testDeleteKeyOnSortedArray(t *testing.T, list *SortedArray) {
	for i := 0; i < 5; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		list.Set(key, value)
	}

	// Delete the key
	list.Delete(kv.Key("k3"))
	_, found := list.Get(kv.Key("k3"))
	require.False(t, found)

	// Check the order of the list
	require.True(t, testOrderOfArray(t, list))
}

func testGetSizeOfTheList(t *testing.T, list *SortedArray) {
	for i := 0; i < 5; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		list.Set(key, value)
	}

	require.Equal(t, 20, list.Size()) // 4 bytes per record

	list.Set(kv.Key("k33232"), kv.Value("khoa dep trai qua"))
	require.Equal(t, 43, list.Size())
}

func testGetAllWithSortedOrder(t *testing.T, list *SortedArray) {
	for i := 0; i < 5; i++ {
		key := kv.Key("k" + strconv.Itoa(i))
		value := kv.Value("v" + strconv.Itoa(i))
		list.Set(key, value)
	}

	allRecords := list.GetAll()
	for i := 1; i < len(allRecords); i++ {
		require.True(t, allRecords[i-1].Key <= allRecords[i].Key)
	}
}

func testInsertRecordWithTheSameKey(t *testing.T, list *SortedArray) {
	list.Set(kv.Key("k1"), kv.Value("v1"))
	v, found := list.Get(kv.Key("k1"))
	require.True(t, found)
	require.Equal(t, kv.Value("v1"), v)

	list.Set(kv.Key("k1"), kv.Value("v2"))

	v, found = list.Get(kv.Key("k1"))
	require.True(t, found)
	require.Equal(t, kv.Value("v2"), v)
}

func testOrderOfArray(t *testing.T, list *SortedArray) bool {
	t.Helper()
	for i := 1; i < len(list.data); i++ {
		if list.data[i-1].Key > list.data[i].Key {
			return false
		}
	}

	return true
}
