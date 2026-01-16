use jdb_pef::{Pef, conf::Conf};
use rand::Rng;

#[test]
fn test_rev_iter() {
    let lengths = [0, 1, 2, 10, 100, 1000];
    
    for n in lengths {
        println!("Testing n = {}", n);
        let mut data = Vec::with_capacity(n);
        let mut val = 0;
        let mut rng = rand::rng();
        for _ in 0..n {
            val += rng.random_range(1..10);
            data.push(val);
        }

        let pef = Pef::new_with_conf(&data, Conf { block_size: 100, ..Default::default() });
        
        // Test 1: Full reverse iteration
        let collected: Vec<u64> = pef.rev_iter().collect();
        let expected: Vec<u64> = data.iter().rev().cloned().collect();
        assert_eq!(collected, expected, "Reverse iteration mismatch for n={}", n);
    }
}

#[test]
fn test_rev_range() {
    let lengths = [0, 10, 100];
    for n in lengths {
        println!("Testing Range n = {}", n);
        let mut data = Vec::with_capacity(n);
        let mut val = 0;
        let mut rng = rand::rng();
        for _ in 0..n {
            val += rng.random_range(1..10);
            data.push(val);
        }
        let pef = Pef::new_with_conf(&data, Conf { block_size: 16, ..Default::default() });

        if n > 0 {
             let min = data[0];
             let max = data[data.len()-1];
             
             // Range covering everything
             let collected: Vec<u64> = pef.rev_range(min..max+1).collect();
             let expected: Vec<u64> = data.iter().rev().cloned().collect();
             assert_eq!(collected, expected, "Full range mismatch");

             // Sub-range
             let start = data[n/4];
             let end = data[3*n/4];
             // In Rust range [start, end).
             // rev_range should give elements < end, down to >= start.
             let collected_sub: Vec<u64> = pef.rev_range(start..end).collect();
             
             let expected_sub: Vec<u64> = data.iter()
                 .rev()
                 .cloned()
                 .filter(|&x| x >= start && x < end)
                 .collect();
             
             assert_eq!(collected_sub, expected_sub, "Sub range mismatch for {}..{}", start, end);
        }
    }
}
