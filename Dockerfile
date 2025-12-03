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
    protobuf-devel \
    grpc-devel \
 && touch /usr/lib64/librocksdb.a

COPY . /src/
WORKDIR /src/
RUN cmake -B /build
RUN cmake --build /build

FROM opensuse/tumbleweed
RUN zypper update -y \
 && zypper install -y --no-recommends \
    librocksdb10 \
    libgrpc++1_76 \
    abseil-cpp-devel \
    libssl59
WORKDIR /app
COPY --from=builder /build/Grit /app
CMD ["/app/Grit"]
