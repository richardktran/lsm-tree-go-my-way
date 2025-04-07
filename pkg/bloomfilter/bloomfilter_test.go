package bloomfilter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddAndMightContain(t *testing.T) {
	bf := NewBloomFilter(1000, 3)

	// Test adding and checking for a key
	key := "test-key"
	bf.Add(key)

	require.True(t, bf.MightContain(key), "Expected key '%s' to be in the Bloom filter, but it was not", key)

	// Test checking for a key that was not added
	nonExistentKey := "non-existent-key"
	require.False(t, bf.MightContain(nonExistentKey), "Expected key '%s' to not be in the Bloom filter, but it was", nonExistentKey)
}

func TestFalsePositiveRate(t *testing.T) {
	bf := NewBloomFilter(1000, 3)

	// Add some keys
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range keys {
		bf.Add(key)
	}

	// Check for false positives
	falsePositiveKey := "false-positive-key"
	if bf.MightContain(falsePositiveKey) {
		t.Logf("False positive detected for key '%s'", falsePositiveKey)
	}
}

func TestEmptyFilter(t *testing.T) {
	bf := NewBloomFilter(1000, 3)

	// Check for a key in an empty Bloom filter
	key := "some-key"
	require.False(t, bf.MightContain(key), "Expected key '%s' to not be in the empty Bloom filter, but it was", key)
}
