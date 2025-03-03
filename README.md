# LSM-Tree in Golang
An implementation of Key-Value Store using Log-Structured Merge Tree (LSM Tree) data structure in Golang.

## Notes
- One segment is one SSTable has been flushed from MemTable
- Each segment (SSTable) is immutable and has a Sparse Index respectively
- Each segment is sorted by key and do not have any duplicate key

## How to build Sparse Index
- Sort key-value in the order of key before flushing to SSTable (already sorted in MemTable)
- Write data to SSTable in fixed size block (e.g. 4KB, 8KB, 16KB)
- Save the offset of the first key in each block to Sparse Index

## How to read data in a segment
- Find the key that is <= and closed (key X) the key we are looking for in the Sparse Index
- Jump to the offset of the block that contains key X
- Read the block that contains key X from the offset as the starting point and iterate through the block to find the key we are looking for


## TODO
- [x] Build skeleton for `LSM Tree`
- [x] Implement basic `MemTable` with Sorted Array as underlying data structure
- [x] Implement `Write Ahead Log (WAL)` and recover the memtable from WAL
- [x] Persist the `Sparse Index` to disk in order to recover the Sparse Index in memory
- [x] Recover from `Write Ahead Sparse Index Log` to `Sparse Index`
- [ ] Should store offset 0 in the Sparse Index when the first key is written to the SSTable
- [ ] Recover `SSTable` from disk by adding `Blocks` information to memory
- [ ] Enhance `WAL` written by channel to avoid blocking the main thread
- [ ] Create `SSTable` with each segment 
- [ ] Complete Flush to `SSTable` of `MemTable`
- [ ] Handle `Read` operation
- [ ] Improve `MemTable` by using `Skip List` as underlying data structure
- [ ] Improve Read by `Bloom Filter`
- [ ] Implement `Compaction`