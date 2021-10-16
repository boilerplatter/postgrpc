FROM rust:1.55 AS builder

# set up working directory
WORKDIR /app

# install required formatter
RUN rustup component add rustfmt

# install third-party packages
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get -y update && \
  apt-get install -y \
  libclang-dev \
  libssl-dev \
  openssl \
  pkg-config

# copy the source code
COPY . .

# compile postgrpc from source
WORKDIR ./postgrpc
RUN cargo build --release

# use multi-stage build for smaller production image
FROM gcr.io/distroless/cc
COPY --from=builder /app/target/release/postgrpc /

# Run postgrpc 
CMD ["/postgrpc"]
