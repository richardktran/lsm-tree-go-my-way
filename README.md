# LSM-Tree in Golang

An implementation of Key-Value Store using Log-Structured Merge Tree (LSM Tree) data structure in Golang.

## Introduction

Log-Structured Merge Trees (LSM Trees) are a data structure typically used in databases and file systems to manage large volumes of data. They are designed to provide high write throughput by batching writes and periodically merging them into larger structures. This implementation provides a simple key-value store using LSM Trees in Golang.

To know more about LSM Trees, you can read my article here: [LSM Tree - The Lazy Genius Behind the Power of NoSQL](https://open.substack.com/pub/richardktran/p/lsm-tree-the-lazy-genius-behind-the).

## Installation

To install this project, you need to have Golang installed on your machine. You can download and install Golang from [here](https://golang.org/dl/).

Clone the repository:

```sh
git clone https://github.com/richardktran/lsm-tree-go-my-way.git
cd lsm-tree-go-my-way
```

Build the project:

```sh
make build
```

## Usage

To use this key-value store, you can run the built executable. It will open a prompt where you can type commands to interact with the store. Below is an example of how to use it:

```sh
./bin/lsmt
```

At the prompt, you can use the following commands:

- `SET <key> <value>`: Insert a key-value pair
- `GET <key>`: Retrieve the value for a given key
- `DEL <key>`: Delete a key-value pair

Example:

```sh
> SET key1 value1
> GET key1
value1
> DEL key1
> GET key1
(nil)
```

## Personal Notes
- One segment is one SSTable has been flushed from MemTable
- Each segment (SSTable) is immutable and has a Sparse Index respectively
- Each segment is sorted by key and do not have any duplicate key

### How to build Sparse Index
- Sort key-value in the order of key before flushing to SSTable (already sorted in MemTable)
- Write data to SSTable in fixed size block (e.g. 4KB, 8KB, 16KB)
- Save the offset of the first key in each block to Sparse Index

### How to read data in a segment
- Find the key that is <= and closed (key X) the key we are looking for in the Sparse Index
- Jump to the offset of the block that contains key X
- Read the block that contains key X from the offset as the starting point and iterate through the block to find the key we are looking for

### TODO
- [x] Build skeleton for `LSM Tree`
- [x] Implement basic `MemTable` with Sorted Array as underlying data structure
- [x] Implement `Write Ahead Log (WAL)` and recover the memtable from WAL
- [x] Persist the `Sparse Index` to disk in order to recover the Sparse Index in memory
- [x] Recover from `Write Ahead Sparse Index Log` to `Sparse Index`
- [x] Should store offset 0 in the Sparse Index when the first key is written to the SSTable
- [x] Recover `SSTable` from disk by adding `Blocks` information to memory
- [x] Handle `Read` operation
- [x] Handle `Delete` operation
- [ ] Handle the case when read the key that is flushing to SSTable
- [ ] Handle the case when server is crashed during flushing to SSTable
- [ ] Enhance `WAL` written by channel to avoid blocking the main thread, 
- [ ] Avoid overhead when open and close the WAL file for each write or read
- [ ] Implement `Compaction` to merge multiple SSTables into one SSTable
- [ ] Complete Flush to `SSTable` of `MemTable`
- [ ] Improve `MemTable` by using `Skip List` as underlying data structure
- [ ] Improve Read by `Bloom Filter`
- [ ] Implement `Compaction`