package kv

type Key string

type Value []byte

type Record struct {
	Key   Key   `json:"key"`
	Value Value `json:"value"`
}

// Size returns the size of the record in bytes (key + value)
// Key (1 byte) + Value (1 byte) = 2 bytes
func (r Record) Size() int {
	return len(r.Key) + len(r.Value)
}

func (r Record) IsDeletedRecord() bool {
	return r.Value == nil
}
