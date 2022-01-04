use jni::JNIEnv;
use jni::objects::{JClass, JString, JValue};
use jni::sys::{jbyte, jbooleanArray, jbyteArray, jobject, jsize};
use pancake_db_core::{RepLevelsAndAtoms, RepLevelsAndBytes};
use pancake_db_core::compression::ValueCodec;
use pancake_db_core::encoding::{Decoder, DecoderImpl};
use pancake_db_core::primitives::Primitive;
use q_compress::TimestampMicros;
use pancake_db_core::deletion::decompress_deletions;

fn decode_column<P: Primitive>(
  env: JNIEnv,
  nesting_depth: jbyte,
  compressed_bytes: jbyteArray,
  uncompressed_bytes: jbyteArray,
  codec: JString,
  class_name: &str,
  signature: &str,
  create_jni_array_fn: &dyn Fn(&JNIEnv, &[P::A]) -> jobject,
) -> jobject {
  // DECOMPRESS
  let bytes = env.convert_byte_array(compressed_bytes).unwrap();
  let mut atoms = Vec::new();
  let mut rep_levels = Vec::new();
  if !bytes.is_empty() {
    let codec: String = env.get_string(codec)
      .expect("unable to get codec string")
      .into();
    let decompressor = P::new_codec(&codec)
      .expect("invalid codec for data type");

    let RepLevelsAndBytes { remaining_bytes: bytes, levels } = decompressor.decompress_rep_levels(bytes)
      .expect("unable to decompress repetition levels");

    atoms.extend(decompressor.decompress_atoms(&bytes)
      .expect("unable to decompress; data may be corrupt"));
    rep_levels.extend(levels);
  };

  // DECODE
  let bytes = env.convert_byte_array(uncompressed_bytes).unwrap();
  if !bytes.is_empty() {
    let decoder = DecoderImpl::<P, RepLevelsAndAtoms<P::A>>::new(nesting_depth as u8);
    let decoded = decoder
      .decode(&bytes)
      .expect("unable to decode uncompressed bytes; data may be corrupt");
    for RepLevelsAndAtoms { levels, atoms: uncompressed_atoms } in &decoded {
      rep_levels.extend(levels);
      atoms.extend(uncompressed_atoms);
    }
  }

  let java_values = create_jni_array_fn(&env, &atoms);
  let java_rep_levels = env.byte_array_from_slice(&rep_levels)
    .expect("unable to allocate java rep levels array");
  let args = vec![
    JValue::from(java_values),
    JValue::from(java_rep_levels),
  ];
  let obj = env.new_object(class_name, signature, &args)
    .expect("could not construct java object");
  obj.into_inner()
}

#[no_mangle]
pub extern "system" fn Java_com_pancakedb_client_NativeCore_00024_decodeInt64s(
  env: JNIEnv,
  _: JClass,
  nesting_depth: jbyte,
  compressed_bytes: jbyteArray,
  uncompressed_bytes: jbyteArray,
  codec: JString,
) -> jobject {
  fn create_jni_array(env: &JNIEnv, atoms: &[i64]) -> jobject {
    let java_values = env.new_long_array(atoms.len() as jsize)
      .expect("unable to allocate java array");
    env.set_long_array_region(java_values, 0 as jsize, atoms)
      .expect("unable to assign to java array");
    java_values
  }
  decode_column::<i64>(
    env,
    nesting_depth,
    compressed_bytes,
    uncompressed_bytes,
    codec,
    "com/pancakedb/client/NativeCore$LongColumn",
    "([J[B)V",
    &create_jni_array,
  )
}

#[no_mangle]
pub extern "system" fn Java_com_pancakedb_client_NativeCore_00024_decodeBools(
  env: JNIEnv,
  _: JClass,
  nesting_depth: jbyte,
  compressed_bytes: jbyteArray,
  uncompressed_bytes: jbyteArray,
  codec: JString,
) -> jobject {
  fn create_jni_array(env: &JNIEnv, atoms: &[bool]) -> jobject {
    let java_values = env.new_boolean_array(atoms.len() as jsize)
      .expect("unable to allocate java array");
    let bools_as_bytes = atoms.iter()
      .map(|b| *b as u8)
      .collect::<Vec<u8>>();
    env.set_boolean_array_region(java_values, 0 as jsize, &bools_as_bytes)
      .expect("unable to assign to java array");
    java_values
  }
  decode_column::<bool>(
    env,
    nesting_depth,
    compressed_bytes,
    uncompressed_bytes,
    codec,
    "com/pancakedb/client/NativeCore$BooleanColumn",
    "([Z[B)V",
    &create_jni_array,
  )
}

#[no_mangle]
pub extern "system" fn Java_com_pancakedb_client_NativeCore_00024_decodeFloat32s(
  env: JNIEnv,
  _: JClass,
  nesting_depth: jbyte,
  compressed_bytes: jbyteArray,
  uncompressed_bytes: jbyteArray,
  codec: JString,
) -> jobject {
  fn create_jni_array(env: &JNIEnv, atoms: &[f32]) -> jobject {
    let java_values = env.new_float_array(atoms.len() as jsize)
      .expect("unable to allocate java array");
    env.set_float_array_region(java_values, 0 as jsize, atoms)
      .expect("unable to assign to java array");
    java_values
  }
  decode_column::<f32>(
    env,
    nesting_depth,
    compressed_bytes,
    uncompressed_bytes,
    codec,
    "com/pancakedb/client/NativeCore$FloatColumn",
    "([F[B)V",
    &create_jni_array,
  )
}
#[no_mangle]
pub extern "system" fn Java_com_pancakedb_client_NativeCore_00024_decodeFloat64s(
  env: JNIEnv,
  _: JClass,
  nesting_depth: jbyte,
  compressed_bytes: jbyteArray,
  uncompressed_bytes: jbyteArray,
  codec: JString,
) -> jobject {
  fn create_jni_array(env: &JNIEnv, atoms: &[f64]) -> jobject {
    let java_values = env.new_double_array(atoms.len() as jsize)
      .expect("unable to allocate java array");
    env.set_double_array_region(java_values, 0 as jsize, atoms)
      .expect("unable to assign to java array");
    java_values
  }
  decode_column::<f64>(
    env,
    nesting_depth,
    compressed_bytes,
    uncompressed_bytes,
    codec,
    "com/pancakedb/client/NativeCore$DoubleColumn",
    "([D[B)V",
    &create_jni_array,
  )
}

#[no_mangle]
pub extern "system" fn Java_com_pancakedb_client_NativeCore_00024_decodeTimestamps(
  env: JNIEnv,
  _: JClass,
  nesting_depth: jbyte,
  compressed_bytes: jbyteArray,
  uncompressed_bytes: jbyteArray,
  codec: JString,
) -> jobject {
  fn create_jni_array(env: &JNIEnv, atoms: &[TimestampMicros]) -> jobject {
    let java_values = env.new_long_array(atoms.len() as jsize)
      .expect("unable to allocate java array");
    // client just encodes timestamps as epoch micros.
    // If we ever do TimestampNs or other precisions, or we want the complete time range,
    // this part needs to change.
    let longs = atoms
      .iter()
      .map(|ts| ts.to_total_parts() as i64)
      .collect::<Vec<i64>>();
    env.set_long_array_region(java_values, 0 as jsize, &longs)
      .expect("unable to assign to java array");
    java_values
  }
  decode_column::<TimestampMicros>(
    env,
    nesting_depth,
    compressed_bytes,
    uncompressed_bytes,
    codec,
    "com/pancakedb/client/NativeCore$LongColumn",
    "([J[B)V",
    &create_jni_array,
  )
}

#[no_mangle]
pub extern "system" fn Java_com_pancakedb_client_NativeCore_00024_decodeStringOrBytes(
  env: JNIEnv,
  _: JClass,
  dtype: JString,
  nesting_depth: jbyte,
  compressed_bytes: jbyteArray,
  uncompressed_bytes: jbyteArray,
  codec: JString,
) -> jobject {
  fn create_jni_array(env: &JNIEnv, atoms: &[u8]) -> jobject {
    env.byte_array_from_slice(&atoms)
      .expect("unable to allocate java byte array")
  }
  let dtype: String = env.get_string(dtype)
    .expect("unable to get dtype string")
    .into();
  if dtype == "STRING" {
    decode_column::<String>(
      env,
      nesting_depth,
      compressed_bytes,
      uncompressed_bytes,
      codec,
      "com/pancakedb/client/NativeCore$StringOrBytesColumn",
      "([B[B)V",
      &create_jni_array,
    )
  } else {
    decode_column::<Vec<u8>>(
      env,
      nesting_depth,
      compressed_bytes,
      uncompressed_bytes,
      codec,
      "com/pancakedb/client/NativeCore$StringOrBytesColumn",
      "([B[B)V",
      &create_jni_array,
    )
  }
}

#[no_mangle]
pub extern "system" fn Java_com_pancakedb_client_NativeCore_00024_decodeDeletions(
  env: JNIEnv,
  _: JClass,
  data: jbyteArray,
) -> jbooleanArray {
  let bytes = env.convert_byte_array(data)
    .unwrap();

  let deletions_as_bytes: Vec<_> = decompress_deletions(bytes)
    .expect("corrupt deletion data")
    .iter()
    .map(|&b| b as u8)
    .collect();

  let res = env.new_boolean_array(deletions_as_bytes.len() as jsize)
    .expect("unable to allocate jbooleanArray for deletions");
  env.set_boolean_array_region(res, 0, &deletions_as_bytes)
    .expect("unable to assign jbooleanArray for deletions");
  res
}
