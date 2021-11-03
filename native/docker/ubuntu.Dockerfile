FROM ubuntu:16.04
MAINTAINER PancakeDB <inquiries@pancakedb.com>

RUN apt-get update && apt-get install -y \
  curl \
  build-essential \
  gcc-aarch64-linux-gnu \
  binutils-aarch64-linux-gnu

SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:$PATH"
RUN rustup target add aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu

COPY Cargo.toml /workdir/
COPY .cargo /workdir/.cargo
COPY src /workdir/src
COPY cross_build_linux.sh /workdir/
WORKDIR /workdir

RUN sh cross_build_linux.sh

