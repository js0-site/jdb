#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

use std::path::Path;

use jdb_fsst::{SYMBOL_TABLE_SIZE, compress, decompress};

fn load_test_texts() -> Vec<String> {
  let mut texts = Vec::new();
  let txt_dir = Path::new("tests/txt");

  // Load English texts
  let en_dir = txt_dir.join("en");
  if en_dir.exists() {
    let entries = std::fs::read_dir(&en_dir).unwrap();
    let mut files: Vec<_> = entries
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == "txt"))
      .collect();

    // Sort files to ensure consistent order
    files.sort_by_key(|e| e.path());

    for entry in files {
      if let Ok(content) = std::fs::read_to_string(entry.path()) {
        texts.push(content);
      }
    }
  }

  // Load Chinese texts
  let zh_dir = txt_dir.join("zh");
  if zh_dir.exists() {
    let entries = std::fs::read_dir(&zh_dir).unwrap();
    let mut files: Vec<_> = entries
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == "txt"))
      .collect();

    // Sort files to ensure consistent order
    files.sort_by_key(|e| e.path());

    for entry in files {
      if let Ok(content) = std::fs::read_to_string(entry.path()) {
        texts.push(content);
      }
    }
  }

  texts
}

/// Helper to convert lines into buffer and offsets
fn lines_to_buf_offsets(text: &str) -> (Vec<u8>, Vec<usize>) {
  let lines: Vec<&str> = text.lines().collect();
  let mut buf = Vec::new();
  let mut offsets = vec![0usize];
  for line in lines {
    buf.extend_from_slice(line.as_bytes());
    offsets.push(buf.len());
  }
  (buf, offsets)
}

#[test]
fn test_fsst() {
  let test_texts = load_test_texts();

  for text in &test_texts {
    let test_input_size = 1024 * 1024;
    let repeat_num = test_input_size / text.len();
    let test_input = text.repeat(repeat_num.max(1));
    helper(&test_input);

    let test_input_size = 2 * 1024 * 1024;
    let repeat_num = test_input_size / text.len();
    let test_input = text.repeat(repeat_num.max(1));
    helper(&test_input);
  }
}

fn helper(test_input: &str) {
  let (in_buf, in_offsets) = lines_to_buf_offsets(test_input);

  let mut compress_output_buf: Vec<u8> = vec![0; in_buf.len()];
  let mut compress_offset_buf: Vec<usize> = vec![0; in_offsets.len()];
  let mut symbol_table = [0; SYMBOL_TABLE_SIZE];

  compress(
    symbol_table.as_mut(),
    &in_buf,
    &in_offsets,
    &mut compress_output_buf,
    &mut compress_offset_buf,
  )
  .unwrap();

  let original_size = in_buf.len();
  let compressed_size = compress_offset_buf.last().copied().unwrap_or(0);
  let compression_ratio = if original_size > 0 {
    (compressed_size as f64 / original_size as f64) * 100.0
  } else {
    0.0
  };

  log::info!(
    "Original size: {} bytes, Compressed size: {} bytes, Compression ratio: {:.2}%",
    original_size,
    compressed_size,
    compression_ratio
  );

  let mut decompress_output: Vec<u8> = vec![0; compress_output_buf.len() * 8];
  let mut decompress_offsets: Vec<usize> = vec![0; compress_offset_buf.len()];

  decompress(
    &symbol_table,
    &compress_output_buf,
    &compress_offset_buf,
    &mut decompress_output,
    &mut decompress_offsets,
  )
  .unwrap();

  for i in 1..decompress_offsets.len() {
    let s = &decompress_output[decompress_offsets[i - 1]..decompress_offsets[i]];
    let original = &in_buf[in_offsets[i - 1]..in_offsets[i]];
    assert!(
      s == original,
      "s: {:?}\n\n, original: {:?}",
      std::str::from_utf8(s),
      std::str::from_utf8(original)
    );
  }
}
