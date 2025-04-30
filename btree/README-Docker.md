# BTree Benchmark Docker Setup

This README explains how to build and run the BTree benchmark using Docker.

## Prerequisites

- Docker installed on your system
- Git to clone the repository

## Building the Docker Image

From the BTree directory (containing the Dockerfile), run:

```bash
docker build -t btree-benchmark .
```

This will create a Docker image with all required dependencies:

- Ubuntu 20.04 base
- Build tools (gcc, make, cmake)
- Database libraries: LevelDB, RocksDB, LMDB, WiredTiger, SQLite  
- YCSB-cpp benchmark suite

## Running the Benchmark

You can run the benchmark with default parameters:

```bash
docker run btree-benchmark
```

Or with custom parameters:

```bash
docker run btree-benchmark python3 /app/BTree/run.py \
  --num_threads 4 \
  --num_records 1000000 \
  --num_operations_per_thread 1000000 \
  --lmdb_mapsize 5368709120 \
  --db_path /tmp/btree-benchmark
```

## Configuration Options

The benchmark script supports the following parameters:

- `--num_threads`: Number of threads to run YCSB with (0 means use all CPU cores)
- `--num_records`: Number of records to store in the database
- `--num_operations_per_thread`: Number of operations to run per thread
- `--lmdb_mapsize`: Amount of space for LMDB's map
- `--db_path`: Backing file location for LMDB

## Output

The benchmark results will be saved to a `results.csv` file in the benchmark directory.
