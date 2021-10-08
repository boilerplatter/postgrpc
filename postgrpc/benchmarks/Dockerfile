FROM ubuntu:20.04

# set up working directories
RUN mkdir -p /results

# install third-party packages
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get -y update && \
  apt-get install -y \
  ca-certificates \
  curl \
  postgresql-contrib

# install gRPC tools
RUN curl -L \
  https://github.com/fullstorydev/grpcurl/releases/download/v1.8.2/grpcurl_1.8.2_linux_x86_64.tar.gz | \
  tar xvz && \
  mv ./grpcurl /usr/local/bin/grpcurl && \
  chmod +x /usr/local/bin/grpcurl

RUN curl -L \
  https://github.com/bojand/ghz/releases/download/v0.103.0/ghz-linux-x86_64.tar.gz | \
  tar xvz && \
  mv ./ghz /usr/local/bin/ghz && \
  chmod +x /usr/local/bin/ghz

# copy over the test queries for pg_bench
COPY ./queries ./queries

# run tests, sending output to files
ENV PGPASSWORD=supersecretpassword
ENTRYPOINT pgbench \
  -h postgres \
  -U postgres \
  -d postgres \
  -f ./queries/current_user.sql \
  -C -c 10 -j 10 -t 1000 -L 100 \
  -M extended > /results/postgres-current-user.txt && \
pgbench \
  -h pgbouncer \
  -U postgres \
  -d postgres \
  -p 6432 \
  -f ./queries/current_user.sql \
  -C -c 10 -j 10 -t 1000 -L 100 \
  -M extended > /results/pgbouncer-current-user.txt && \
ghz \
  -n 10000 \
  -c 50 \
  --insecure \
  -d '{"statement":"select current_user"}' \
  --call postgres.Postgres/Query \
  postgrpc:50051 > /results/postgrpc-current-user.txt
