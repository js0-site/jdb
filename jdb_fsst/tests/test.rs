#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
  fastrand::seed(42);
}

use std::path::Path;

use human_size::{Byte, Megabyte, SpecificSize};
use jdb_fsst::train;

// Reference implementation is now provided by the official 'fsst' crate

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
  let lines: Vec<&str> = test_input.lines().collect();

  // 1. Prepare: convert to byte slices
  // 准备：转换为字节切片
  let line_bytes: Vec<&[u8]> = lines.iter().map(|l| l.as_bytes()).collect();

  // 2. Train encoder
  // 训练编码器
  let encoder = train(&line_bytes).unwrap();
  let decoder = jdb_fsst::decode::Decode::from(&encoder);

  // 3. Encode all lines
  // 编码所有行
  let mut encode_output_buf = Vec::new();
  let mut compressed_sizes = Vec::new();

  for line_data in &line_bytes {
    let before = encode_output_buf.len();
    encoder.encode(line_data, &mut encode_output_buf);
    compressed_sizes.push(encode_output_buf.len() - before);
  }

  let original_size = test_input.len();
  let compressed_size = encode_output_buf.len();
  let compression_ratio = if original_size > 0 {
    (compressed_size as f64 / original_size as f64) * 100.0
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
文件: {} (目标大小: {} MB)
原始大小: {} ({})
压缩后大小: {} ({})
压缩比: {:.2}%
"#,
    file_path,
    test_size / (1024 * 1024),
    original_size,
    original_hr,
    compressed_size,
    compressed_hr,
    compression_ratio
  );

  // Decode and verify
  let mut decode_output = Vec::new();
  let mut pos = 0;

  for (i, &size) in compressed_sizes.iter().enumerate() {
    let compressed_line = &encode_output_buf[pos..pos + size];
    let before = decode_output.len();
    decoder.decode(compressed_line, &mut decode_output);
    let decoded_line = &decode_output[before..];

    // Verify
    assert_eq!(
      decoded_line,
      lines[i].as_bytes(),
      "Line {} mismatch: decoded={:?}, original={:?}",
      i,
      String::from_utf8_lossy(decoded_line),
      lines[i]
    );

    pos += size;
  }

  // Verify total decoded content
  let decoded_str = String::from_utf8_lossy(&decode_output);
  let original_concat = lines.join("");
  assert_eq!(
    decoded_str, original_concat,
    "Full decoded content doesn't match original"
  );
}
