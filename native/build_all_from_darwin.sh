# Run from the native/ directory.

set -e

sh build_local_darwin.sh
TAG="pancake-scala-client-rust:latest"
docker build -f docker/ubuntu.Dockerfile -t "$TAG" .

copy_linux () {
  # my sneaky one-liner for copying the binaries out
  docker run --rm "$TAG" cat "/workdir/target/$1/release/libpancake_scala_client_native.so" > "../src/main/resources/native/$2/libpancake_scala_client_native.so"
}
copy_linux aarch64-unknown-linux-gnu aarch64-linux
copy_linux x86_64-unknown-linux-gnu x86_64-linux
