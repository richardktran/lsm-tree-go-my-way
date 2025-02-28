package kv

type Key string

type Value string

type Record struct {
	Key   Key
	Value Value
}

// Size returns the size of the record in bytes (key + value)
// Key (1 byte) + Value (1 byte) = 2 bytes
func (r Record) Size() int {
	return len(r.Key) + len(r.Value)
}
