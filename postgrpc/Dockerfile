FROM rust:1.55

# install required formatter
RUN rustup component add rustfmt

# install third-party packages
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get -y update && \
  apt-get install -y \
  libssl-dev \
  openssl \
  pkg-config

# copy the source code
COPY . .

# compile postgrpc from source
WORKDIR ./postgrpc
RUN cargo build --release

# Run postgrpc 
CMD ../target/release/postgrpc
