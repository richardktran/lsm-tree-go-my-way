package wal

import (
	"fmt"
	"os"
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
	commitLogPath := walDir + "/wal.log"
	metaLogPath := walDir + "/wal.meta"

	_, err := os.Stat(walDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(walDir, 0755)
		if err != nil {
			return nil, err
		}
	}

	return &WAL{
		CommitLogPath: commitLogPath,
		MetaLogPath:   metaLogPath,
	}, nil
}

func (w *WAL) WriteCommitLog(record kv.Record, timestamp int64) (int, error) {
	w.commitLogLock.Lock()
	defer w.commitLogLock.Unlock()

	data := fmt.Appendf(nil, "%s:%s:%d\n", record.Key, record.Value, timestamp)

	commitLog, err := os.OpenFile(w.CommitLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer commitLog.Close()

	return commitLog.Write(data)
}

func (w *WAL) WriteMetaLog(timestamp int64) (int, error) {
	w.metaLogLock.Lock()
	defer w.metaLogLock.Unlock()

	data := []byte(fmt.Sprintf("%d\n", timestamp))

	metaLog, err := os.OpenFile(w.MetaLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer metaLog.Close()

	return metaLog.Write(data)
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
	_, err = fmt.Sscanf(lines[len(lines)-1], "%d", &lastTimestamp)
	if err != nil {
		return 0, err
	}

	return lastTimestamp, nil
}

func (w *WAL) ReadCommitLogAfterTimestamp(timestamp int64) ([]kv.Record, error) {
	w.commitLogLock.RLock()
	defer w.commitLogLock.RUnlock()

	data, err := os.ReadFile(w.CommitLogPath)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("commit log is empty")
	}

	var records []kv.Record

	for _, line := range lines {
		parts := strings.Split(line, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid commit log format")
		}

		var ts int64
		_, err := fmt.Sscanf(parts[2], "%d", &ts)
		if err != nil {
			return nil, err
		}

		if ts > timestamp {
			parts := strings.Split(line, ":")
			records = append(records, kv.Record{
				Key:   kv.Key(parts[0]),
				Value: kv.Value(parts[1]),
			})
		}
	}

	return records, nil
}
