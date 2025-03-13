package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecordSize(t *testing.T) {
	tests := []struct {
		name     string
		record   Record
		expected int
	}{
		{
			name: "Empty key and value",
			record: Record{
				Key:   "",
				Value: Value(""),
			},
			expected: 0,
		},
		{
			name: "Non-empty key and value",
			record: Record{
				Key:   "key",
				Value: Value("value"),
			},
			expected: 8,
		},
		{
			name: "Long key and value",
			record: Record{
				Key:   "longerkey",
				Value: Value("longervalue"),
			},
			expected: 20,
		},
		{
			name: "Key only",
			record: Record{
				Key:   "justkeyonly",
				Value: Value(""),
			},
			expected: 11,
		},
		{
			name: "Value only",
			record: Record{
				Key:   "",
				Value: Value("valueonly"),
			},
			expected: 9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.record.Size())
		})
	}
}
