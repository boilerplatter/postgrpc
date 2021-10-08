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

# install third-party packages
# ENV DEBIAN_FRONTEND=noninteractive
# RUN apt-get -y update && \
#   apt-get install -y \
#   ca-certificates \
#   curl

# # install gRPC tools
# RUN curl -L \
#   https://github.com/fullstorydev/grpcurl/releases/download/v1.8.2/grpcurl_1.8.2_linux_x86_64.tar.gz | \
#   tar xvz && \
#   mv ./grpcurl /usr/local/bin/grpcurl && \
#   chmod +x /usr/local/bin/grpcurl

# RUN curl -L \
#   https://github.com/bojand/ghz/releases/download/v0.103.0/ghz-linux-x86_64.tar.gz | \
#   tar xvz && \
#   mv ./ghz /usr/local/bin/ghz && \
#   chmod +x /usr/local/bin/ghz

# # switch to the postgres user
# USER postgres
# WORKDIR ~

# # Set up the default bash entrypoint
# ENTRYPOINT service postgresql start && \
#   psql -c "CREATE USER test WITH PASSWORD 'secretpassword'" && \
#   psql -c 'CREATE DATABASE testdb OWNER test' && \
#   bash

# TODO: postgrpc as systemctl daemon

