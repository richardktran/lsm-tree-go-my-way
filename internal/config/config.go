package config

type Config struct {
	Host                  string
	Port                  string
	MemTableSizeThreshold int
	SSTableBlockSize      uint64
	RootDataDir           string
	SparseWALBufferSize   uint64
	BloomFilterSize       uint64
	BloomFilterHashCount  int
}
