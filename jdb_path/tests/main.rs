use std::path::{Path, PathBuf};

use jdb_path::{decode, encode};

#[test]
fn test_encode_decode() {
  let base = Path::new("/tmp");
  let prefix = "test";

  for id in [0, 1, 100, 1000, 10000, u64::MAX / 2] {
    let path = encode(base, prefix, id);
    let decoded = decode(&path);
    assert_eq!(decoded, Some(id), "id={id}, path={path:?}");
  }
}

#[test]
fn test_path_format() {
  let base = Path::new("/data");
  let path = encode(base, "blob", 0);
  assert_eq!(path, PathBuf::from("/data/blob/00/00/00"));

  let path = encode(base, "wal", 123456);
  // 123456 in base32 = 3rj0
  assert!(path.to_string_lossy().contains("wal/"));
}
