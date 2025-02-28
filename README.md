# lsm-tree-go-my-way
An implementation of Key-Value Store using Log-Structured Merge Tree (LSM Tree) data structure in Golang.

## TODO
- [x] Build skeleton for `LSM Tree`
- [x] Implement basic `MemTable` with Sorted Array as underlying data structure
- [] Implement `SSTable`
- [] Complete Flush to `SSTable` of `MemTable`
- [] Handle `Read` operation
- [] Improve `MemTable` by using `Skip List` as underlying data structure
- [] Improve Read by `Bloom Filter`
- [] Implement `Write Ahead Log (WAL)`
- [] Implement `Compaction`