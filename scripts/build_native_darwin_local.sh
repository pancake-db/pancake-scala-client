cd native && \
  cargo build --release && \
  cd .. && \
  mkdir -p src/main/resources/native/x86_64-darwin/ && \
  cp native/target/release/*.dylib src/main/resources/native/x86_64-darwin/ && \
  cp native/target/release/*.dylib src/test/resources/native/x86_64-darwin/

