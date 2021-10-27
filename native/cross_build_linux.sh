# This scripts assumes the native architecture running docker is x86_64.
# Run from within native/ directory.

set -e

build_for_target () {
  echo "BUILDING FOR $1..."
  cargo build --release --target "$1"
}

# other architecture(s)
CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc build_for_target aarch64-unknown-linux-gnu

# native architecture
build_for_target x86_64-unknown-linux-gnu
