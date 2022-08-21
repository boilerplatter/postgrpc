FROM rust:1.63 AS builder

# set up working directory
WORKDIR /app

# install third-party packages
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get -y update && \
  apt-get install -y \
  libclang-dev \
  libssl-dev \
  openssl \
  pkg-config \
  protobuf-compiler

# copy the workspace source code
RUN mkdir -p ./postgrpc
RUN mkdir -p ./postguard
COPY ./postgrpc/src ./postgrpc/src
COPY ./postgrpc/proto ./postgrpc/proto
COPY ./postgrpc/build.rs ./postgrpc/
COPY ./postgrpc/Cargo.* ./postgrpc/
COPY ./postguard/src ./postguard/src
COPY ./postguard/Cargo.* ./postguard/
COPY Cargo.toml .

# compile postgrpc from source
RUN cargo build --package postgrpc --release --all-features

# use multi-stage build for smaller production image
FROM gcr.io/distroless/cc
COPY --from=builder /app/target/release/postgrpc /

# Run postgrpc 
CMD ["/postgrpc"]
