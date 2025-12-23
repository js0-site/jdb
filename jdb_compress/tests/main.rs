use jdb_compress::{Codec, dec, enc};

#[test]
fn test_none_codec() {
  let data = b"hello world";
  let compressed = enc(Codec::None, data);
  assert_eq!(compressed, data);

  let decompressed = dec(Codec::None, &compressed).unwrap();
  assert_eq!(decompressed, data);
}

#[test]
fn test_lz4_roundtrip() {
  let data = b"hello world hello world hello world";
  let compressed = enc(Codec::Lz4, data);

  // LZ4 应该压缩重复数据
  assert!(compressed.len() < data.len());

  let decompressed = dec(Codec::Lz4, &compressed).unwrap();
  assert_eq!(decompressed, data);
}

#[test]
fn test_zstd_roundtrip() {
  let data = b"hello world hello world hello world";
  let compressed = enc(Codec::Zstd, data);

  // Zstd 应该压缩重复数据
  assert!(compressed.len() < data.len());

  let decompressed = dec(Codec::Zstd, &compressed).unwrap();
  assert_eq!(decompressed, data);
}

#[test]
fn test_large_data() {
  // 1MB 重复数据
  let data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

  for codec in [Codec::Lz4, Codec::Zstd] {
    let compressed = enc(codec, &data);
    let decompressed = dec(codec, &compressed).unwrap();
    assert_eq!(decompressed, data, "failed for {codec:?}");
  }
}

#[test]
fn test_codec_from_u8() {
  assert_eq!(Codec::from_u8(0).unwrap(), Codec::None);
  assert_eq!(Codec::from_u8(1).unwrap(), Codec::Lz4);
  assert_eq!(Codec::from_u8(2).unwrap(), Codec::Zstd);
  assert!(Codec::from_u8(3).is_err());
}

#[test]
fn test_empty_data() {
  let data = b"";
  for codec in [Codec::None, Codec::Lz4, Codec::Zstd] {
    let compressed = enc(codec, data);
    let decompressed = dec(codec, &compressed).unwrap();
    assert_eq!(decompressed, data, "failed for {codec:?}");
  }
}
