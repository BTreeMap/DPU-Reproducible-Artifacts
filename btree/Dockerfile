FROM ubuntu:24.04

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary tools and libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libsnappy-dev \
    libgflags-dev \
    libz-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    python3 \
    python3-pip \
    curl \
    pkg-config \
    libssl-dev \
    automake \
    autoconf \
    libtool \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install LevelDB
RUN git clone --recurse-submodules https://github.com/google/leveldb.git /tmp/leveldb \
    && cd /tmp/leveldb \
    && mkdir -p build \
    && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Release .. \
    && make -j$(nproc) \
    && make install \
    && rm -rf /tmp/leveldb

# Install RocksDB
RUN git clone --depth 1 --branch v6.27.3 https://github.com/facebook/rocksdb.git /tmp/rocksdb \
    && cd /tmp/rocksdb \
    && PORTABLE=1 make -j$(nproc) shared_lib \
    && make install-shared \
    && rm -rf /tmp/rocksdb

# Install LMDB
RUN git clone --depth 1 https://github.com/LMDB/lmdb.git /tmp/lmdb \
    && cd /tmp/lmdb/libraries/liblmdb \
    && make -j$(nproc) \
    && make install \
    && rm -rf /tmp/lmdb

# Install WiredTiger
RUN git clone --depth 1 https://github.com/wiredtiger/wiredtiger.git /tmp/wiredtiger \
    && cd /tmp/wiredtiger \
    && ./autogen.sh \
    && ./configure --enable-snappy \
    && make -j$(nproc) \
    && make install \
    && rm -rf /tmp/wiredtiger

# Install SQLite
RUN apt-get update && apt-get install -y libsqlite3-dev && apt-get clean

# Set up the working directory
WORKDIR /app

# Clone YCSB-cpp
RUN git clone https://github.com/ls4154/YCSB-cpp.git \
    && cd YCSB-cpp \
    && git submodule update --init

# Build YCSB-cpp
RUN cd YCSB-cpp \
    && mkdir build \
    && cd build \
    && cmake -DBIND_ROCKSDB=1 -DBIND_WIREDTIGER=1 -DBIND_LMDB=1 -DBIND_LEVELDB=1 -DWITH_SNAPPY=1 -DWITH_LZ4=1 -DWITH_ZSTD=1 .. \
    && make -j$(nproc)

# Copy the benchmark directory
COPY . /app/BTree/

# Set LD_LIBRARY_PATH to find shared libraries
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

# Default command
CMD ["python3", "/app/BTree/run.py"]