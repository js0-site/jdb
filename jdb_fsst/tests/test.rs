#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

use std::path::Path;

use human_size::{Byte, Megabyte, SpecificSize};
use jdb_fsst::{decode, encode};

mod fsst_ref;

const TXT_DIR: &str = "tests/txt";
const TXT_EXT: &str = "txt";
const TEST_SIZE_1MB: usize = 1024 * 1024;
const TEST_SIZE_2MB: usize = 2 * 1024 * 1024;

fn load_test_texts() -> Vec<(String, String)> {
  let txt_dir = Path::new(TXT_DIR);
  let mut texts = Vec::new();

  for lang in ["en", "zh"] {
    let lang_dir = txt_dir.join(lang);
    if !lang_dir.exists() {
      continue;
    }

    let mut files: Vec<_> = std::fs::read_dir(&lang_dir)
      .unwrap()
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == TXT_EXT))
      .collect();

    files.sort_by_key(|e| e.path());

    for entry in files {
      let path = entry.path();
      if let Ok(content) = std::fs::read_to_string(&path) {
        texts.push((content, path.to_string_lossy().into_owned()));
      }
    }
  }

  texts
}

/// Helper to convert lines into buffer and offsets
/// Convert lines to buffer and offsets
fn lines_to_buf_offsets(text: &str) -> (Vec<u8>, Vec<usize>) {
  let mut buf = Vec::new();
  let mut offsets = vec![0usize];

  for line in text.lines() {
    buf.extend_from_slice(line.as_bytes());
    offsets.push(buf.len());
  }

  (buf, offsets)
}

#[test]
fn test_fsst() {
  let test_texts = load_test_texts();

  for (text, file_path) in &test_texts {
    for test_size in [TEST_SIZE_1MB, TEST_SIZE_2MB] {
      let repeat_num = test_size / text.len();
      let test_input = text.repeat(repeat_num.max(1));
      helper(&test_input, file_path, test_size);
    }
  }
}

fn helper(test_input: &str, file_path: &str, test_size: usize) {
  let (in_buf, in_offsets) = lines_to_buf_offsets(test_input);

  // Measure my implementation
  let mut encode_output_buf: Vec<u8> = vec![0; in_buf.len()];
  let mut encode_offset_buf: Vec<usize> = vec![0; in_offsets.len()];
  let mut head = encode(
    &in_buf,
    &in_offsets,
    &mut encode_output_buf,
    &mut encode_offset_buf,
  )
  .unwrap();

  let original_size = in_buf.len();
  let compressed_size = encode_offset_buf.last().copied().unwrap_or(0);
  let compression_ratio = if original_size > 0 {
    (compressed_size as f64 / original_size as f64) * 100.0
  } else {
    0.0
  };

  // Measure fsst implementation
  let fsst_compressed = fsst_ref::compress(&in_buf, &in_offsets);
  let fsst_size = fsst_compressed.len();
  let fsst_ratio = if original_size > 0 {
    (fsst_size as f64 / original_size as f64) * 100.0
  } else {
    0.0
  };

  let original_hr = format!(
    "{:.3}",
    SpecificSize::new(original_size as f64, Byte)
      .unwrap()
      .into::<Megabyte>()
  );
  let compressed_hr = format!(
    "{:.3}",
    SpecificSize::new(compressed_size as f64, Byte)
      .unwrap()
      .into::<Megabyte>()
  );

  log::info!(
    r#"
File: {} (Target size: {} MB)
Original size: {} ({})
My Compressed size: {} ({})
My Compression ratio: {:.2}%
FSST Compressed size: {}
FSST Compression ratio: {:.2}%

文件: {} (目标大小: {} MB)
原始大小: {} ({})
我的压缩后大小: {} ({})
我的压缩比: {:.2}%
FSST 压缩后大小: {}
FSST 压缩比: {:.2}%
"#,
    file_path,
    test_size / (1024 * 1024),
    original_size,
    original_hr,
    compressed_size,
    compressed_hr,
    compression_ratio,
    fsst_size,
    fsst_ratio,
    file_path,
    test_size / (1024 * 1024),
    original_size,
    original_hr,
    compressed_size,
    compressed_hr,
    compression_ratio,
    fsst_size,
    fsst_ratio
  );

  let mut decode_output: Vec<u8> = vec![0; encode_output_buf.len() * 8];
  let mut decode_offsets: Vec<usize> = vec![0; encode_offset_buf.len()];

  decode(
    &mut head,
    &encode_output_buf,
    &encode_offset_buf,
    &mut decode_output,
    &mut decode_offsets,
  )
  .unwrap();

  for i in 1..decode_offsets.len() {
    let s = &decode_output[decode_offsets[i - 1]..decode_offsets[i]];
    let original = &in_buf[in_offsets[i - 1]..in_offsets[i]];
    assert!(
      s == original,
      "s: {:?}\n\n, original: {:?}",
      std::str::from_utf8(s),
      std::str::from_utf8(original)
    );
  }
}
