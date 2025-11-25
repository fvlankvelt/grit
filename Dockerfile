FROM opensuse/tumbleweed AS builder

RUN zypper update -y \
 && zypper install -y --no-recommends \
    gcc-c++ \
    cmake \
    rocksdb-devel \
    gflags-devel \
    snappy-devel \
    zlib-devel \
    libbz2-devel \
    liblz4-devel \
    libzstd-devel \
 && touch /usr/lib64/librocksdb.a

WORKDIR /src/
COPY . /src/
RUN cmake -B /build
RUN cmake --build /build

FROM opensuse/tumbleweed
RUN zypper update -y \
 && zypper install -y --no-recommends \
    librocksdb10
WORKDIR /app
COPY --from=builder /build/Grit /app
CMD ["/app/Grit"]
