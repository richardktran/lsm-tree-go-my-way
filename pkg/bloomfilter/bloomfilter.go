package bloomfilter

import (
	"github.com/spaolacci/murmur3"
)

// BloomFilter represents a Bloom filter structure
type BloomFilter struct {
	Bitset      []bool
	size        uint64
	numOfHashes int
}

// NewBloomFilter creates a new Bloom filter with the given size and number of hash functions
func NewBloomFilter(size uint64, numHashes int) *BloomFilter {
	return &BloomFilter{
		Bitset:      make([]bool, size),
		size:        size,
		numOfHashes: numHashes,
	}
}

// Add inserts an key into the Bloom filter
func (bf *BloomFilter) Add(key string) {
	for i := range bf.numOfHashes {
		hash := murmur3.Sum64WithSeed([]byte(key), uint32(i))
		index := hash % uint64(bf.size)
		bf.Bitset[index] = true
	}
}

// Contains checks if an key is possibly in the Bloom filter
func (bf *BloomFilter) MightContain(key string) bool {
	for i := range bf.numOfHashes {
		hash := murmur3.Sum64WithSeed([]byte(key), uint32(i))

		index := hash % uint64(bf.size)
		if !bf.Bitset[index] {
			return false
		}
	}
	return true
}
