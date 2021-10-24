FROM ubuntu:20.04
MAINTAINER PancakeDB <inquiries@pancakedb.com>

RUN apt-get update && apt-get install -y \
  curl \
  build-essential

SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN source $HOME/.cargo/env

WORKDIR /workdir
