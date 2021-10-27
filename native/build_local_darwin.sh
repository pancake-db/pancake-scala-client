# Run from the native/ directory.

set -e

TARGET_NAME="x86_64-apple-darwin"
RESOURCES_DIR="../src/main/resources/native/x86_64-darwin/"

echo "BUILDING FOR $TARGET_NAME..."
cargo build --release --target "$TARGET_NAME"

echo "MOVING BINARY TO $RESOURCES_DIR"
mkdir -p "$RESOURCES_DIR"
mv "target/$TARGET_NAME/release/libpancake_scala_client_native.dylib" "$RESOURCES_DIR" || echo "(ignoring mv failure)"
