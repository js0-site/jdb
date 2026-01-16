#![cfg(feature = "bitcode")]

use jdb_pef::Pef;

#[test]
fn test_bitcode_serialization() {
  let data: Vec<u64> = (0..1000).map(|i| i * 10).collect();
  let original = Pef::new(&data);

  // Encode
  let encoded = bitcode::encode(&original);

  // Decode
  let decoded: Pef = bitcode::decode(&encoded).expect("Decoding failed");

  // Verify
  assert_eq!(original.num_elements, decoded.num_elements);
  // assert_eq!(original.memory_usage(), decoded.memory_usage()); // Capacity may differ

  // Check content
  for i in 0..1000 {
    assert_eq!(original.get(i), decoded.get(i));
  }

  // Check search
  assert_eq!(decoded.next_ge(500), Some(500));
}
