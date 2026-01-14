# JDB FSST Library

A high-performance Rust implementation of the FSST (Fast Static Symbol Table) compression algorithm. This library is designed for efficient compression and decompression of string data, particularly optimized for columnar storage systems like JDB.

## Overview

FSST is a lightweight compression scheme that builds a static symbol table mapping frequently occurring byte sequences (symbols) to shorter codes (typically single bytes). This allows for excellent compression ratios while maintaining extremely fast decompression speeds.

## Key Features

- **High Performance**: Optimized for modern processors with SIMD-style processing during decompression.
- **String Optimized**: Specifically designed for string data with offset array support, similar to Apache Arrow's memory layout.
- **Random Access**: The compression format and API design support efficient random access to individual strings.
- **Automatic Fallback**: Automatically falls back to uncompressed mode (simple copy) when input data is too small or unsuitable for FSST compression.
- **Bulk Operations**: Efficiently compresses/decompresses multiple strings in a single operation.
- **Multilingual Support**: Works seamlessly with both English and Chinese text data.

## Public API

### `encode`

```rust
pub fn encode(
  symbol_table: &mut [u8],
  in_buf: &[u8],
  in_offsets_buf: &[usize],
  out_buf: &mut Vec<u8>,
  out_offsets_buf: &mut Vec<usize>,
) -> io::Result<()>
```

Compresses string data using FSST.

**Parameters:**
- `symbol_table`: Buffer to store the generated symbol table (must be `SYMBOL_TABLE_SIZE` bytes)
- `in_buf`: Concatenated byte slice of all input strings
- `in_offsets_buf`: Array of string offsets (each offset points to the start of a string in `in_buf`)
- `out_buf`: Vector to store compressed output data
- `out_offsets_buf`: Vector to store output offsets

**Behavior:**
- If input size < `LEAST_INPUT_SIZE` (32KB), data is copied without compression
- Otherwise, builds a symbol table from sampled input and compresses the data

### `decode`

```rust
pub fn decode(
  symbol_table: &[u8],
  in_buf: &[u8],
  in_offsets_buf: &[usize],
  out_buf: &mut Vec<u8>,
  out_offsets_buf: &mut Vec<usize>,
) -> io::Result<()>
```

Decompresses FSST-compressed data.

**Parameters:**
- `symbol_table`: Symbol table generated during encoding
- `in_buf`: Compressed data
- `in_offsets_buf`: Offsets for compressed data
- `out_buf`: Vector to store decompressed original data
- `out_offsets_buf`: Vector to store decompressed offsets

**Behavior:**
- Reads the header to determine if data is compressed
- If not compressed, simply copies the data
- If compressed, uses the symbol table to decode the data

## Implementation Details

### Symbol Table Construction

The algorithm samples the input data, builds byte and byte-pair frequency histograms, then iteratively constructs the symbol table. It uses heuristics (such as gain calculation) to select symbols that maximize compression ratio.

### Encoding Format

The symbol table header contains:
- Magic number (not explicitly stored, but header format is fixed)
- Encode switch flag (1 bit at position 24)
- Suffix limit (8 bits)
- Terminator byte (8 bits)
- Number of symbols (8 bits)

The compressed data stream uses escape codes (ESC = 255) to handle raw bytes not represented in the symbol table.

### Optimization Techniques

- **Symbol Reordering**: After construction, symbols are reordered by length (2,3,4,5,6,7,8,1) for better compression performance
- **Suffix Limit**: Two-byte codes are split into two sections to allow early-out optimization during encoding
- **SIMD-style Decoding**: Processes 4 bytes at a time with escape detection using bit masks
- **Short Code Lookup**: Uses a 64K lookup table for fast 2-byte symbol detection

## Constants

- `SYMBOL_TABLE_SIZE`: 2304 bytes (8 header + 256*8 symbols + 256 lengths)
- `LEAST_INPUT_SIZE`: 32KB - minimum input size to trigger encodeion
- `LEAST_INPUT_MAX_LEN`: 5 - minimum string length for FSST to be effective
- `MAX_SYMBOL_LEN`: 8 - maximum length of symbols in the table
- `CODE_BITS`: 9 - bits used for encoding (supports up to 512 symbols)
- `CODE_BASE`: 256 - first 256 codes represent single bytes

## Usage Example

```rust
use jdb_fsst::{encode, decode, SYMBOL_TABLE_SIZE};

// Prepare input data (multiple strings)
let text = "hello\nworld\nrust\nfsst\ncompression";
let lines: Vec<&str> = text.lines().collect();

// Convert to buffer and offsets
let mut in_buf = Vec::new();
let mut in_offsets = vec![0usize];
for line in lines {
    in_buf.extend_from_slice(line.as_bytes());
    in_offsets.push(in_buf.len());
}

// Compress
let mut symbol_table = [0u8; SYMBOL_TABLE_SIZE];
let mut out_buf = vec![0u8; in_buf.len()];
let mut out_offsets = vec![0usize; in_offsets.len()];

encode(
    &mut symbol_table,
    &in_buf,
    &in_offsets,
    &mut out_buf,
    &mut out_offsets,
)?;

// Calculate compression ratio
let original_size = in_buf.len();
let compressed_size = out_offsets.last().copied().unwrap_or(0);
println!("Original: {} bytes, Compressed: {} bytes, Ratio: {:.2}%",
         original_size, compressed_size,
         (compressed_size as f64 / original_size as f64) * 100.0);

// Decompress
let mut decode_buf = vec![0u8; out_buf.len() * 8];
let mut decode_offsets = vec![0usize; out_offsets.len()];

decode(
    &symbol_table,
    &out_buf,
    &out_offsets,
    &mut decode_buf,
    &mut decode_offsets,
)?;

// Verify decompression
assert_eq!(in_buf, decode_buf[..in_buf.len()]);
```

## Testing

Run the test suite with:

```bash
cargo test
```

The project includes comprehensive tests that verify:
- Correctness of encoding/decoding cycles
- Support for both English and Chinese text data
- Various input sizes (1MB and 2MB tests)
- Compression ratio verification