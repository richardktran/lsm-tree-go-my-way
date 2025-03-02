package config

type Config struct {
	MemTableSizeThreshold int
	SSTableBlockSize      uint64
	RootDataDir           string
}
