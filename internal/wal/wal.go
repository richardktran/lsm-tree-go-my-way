package wal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/richardktran/lsm-tree-go-my-way/internal/kv"
)

type WAL struct {
	commitLogLock sync.RWMutex
	metaLogLock   sync.RWMutex
	CommitLogPath string
	MetaLogPath   string
}

func NewWAL(walDir string) (*WAL, error) {
	commitLogPath := filepath.Join(walDir, "wal.log")
	metaLogPath := filepath.Join(walDir, "wal.meta")

	if _, err := os.Stat(walDir); os.IsNotExist(err) {
		if err := os.Mkdir(walDir, 0755); err != nil {
			return nil, err
		}
	}

	return &WAL{
		CommitLogPath: commitLogPath,
		MetaLogPath:   metaLogPath,
	}, nil
}

func (w *WAL) WriteCommitLog(record *kv.Record, timestamp *uint64) (int, error) {
	w.commitLogLock.Lock()
	defer w.commitLogLock.Unlock()

	data := fmt.Sprintf("%s:%s:%d\n", record.Key, record.Value, timestamp)

	commitLog, err := os.OpenFile(w.CommitLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer commitLog.Close()

	return commitLog.Write([]byte(data))
}

func (w *WAL) WriteMetaLog(timestamp *uint64) (int, error) {
	w.metaLogLock.Lock()
	defer w.metaLogLock.Unlock()

	data := fmt.Sprintf("%d\n", timestamp)

	metaLog, err := os.OpenFile(w.MetaLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer metaLog.Close()

	return metaLog.Write([]byte(data))
}

func (w *WAL) ReadCommitLog() ([]byte, error) {
	w.commitLogLock.RLock()
	defer w.commitLogLock.RUnlock()

	return os.ReadFile(w.CommitLogPath)
}

func (w *WAL) ReadMetaLog() ([]byte, error) {
	w.metaLogLock.RLock()
	defer w.metaLogLock.RUnlock()

	return os.ReadFile(w.MetaLogPath)
}

func (w *WAL) ReadLastItemFromMetaLog() (int64, error) {
	w.metaLogLock.RLock()
	defer w.metaLogLock.RUnlock()

	data, err := os.ReadFile(w.MetaLogPath)
	if err != nil {
		return 0, err
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		return 0, fmt.Errorf("meta log is empty")
	}

	var lastTimestamp int64
	if _, err := fmt.Sscanf(lines[len(lines)-1], "%d", &lastTimestamp); err != nil {
		return 0, err
	}

	return lastTimestamp, nil
}

func (w *WAL) ReadCommitLogAfterTimestamp(timestamp int64) ([]kv.Record, error) {
	w.commitLogLock.RLock()
	defer w.commitLogLock.RUnlock()

	file, err := os.Open(w.CommitLogPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var records []kv.Record
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid commit log format")
		}

		var ts int64
		if _, err := fmt.Sscanf(parts[2], "%d", &ts); err != nil {
			return nil, err
		}

		if ts >= timestamp {
			if parts[1] == "" {
				// delete key if value is empty
				record := kv.Record{
					Key:   kv.Key(parts[0]),
					Value: nil,
				}
				records = append(records, record)
			} else {
				records = append(records, kv.Record{
					Key:   kv.Key(parts[0]),
					Value: kv.Value(parts[1]),
				})
			}

		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	valueMap := make(map[kv.Key]kv.Value)
	for _, record := range records {
		valueMap[record.Key] = record.Value
	}

	var filteredRecords []kv.Record
	for key, value := range valueMap {
		filteredRecords = append(filteredRecords, kv.Record{
			Key:   key,
			Value: value,
		})
	}

	return filteredRecords, nil
}
