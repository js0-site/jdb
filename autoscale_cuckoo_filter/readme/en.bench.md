## Benchmark Results

### Test Environment

| Item | Value |
|------|-------|
| OS | macOS 26.1 (arm64) |
| CPU | Apple M2 Max |
| Cores | 12 |
| Memory | 64.0 GB |
| Rust | rustc 1.94.0-nightly (21ff67df1 2025-12-15) |

Test: 100000 items, capacity=200000

### What is FPP?

**FPP (False Positive Probability)** is the probability that a filter incorrectly reports an item as present when it was never added. Lower FPP means higher accuracy but requires more memory. A typical FPP of 1% means about 1 in 100 queries for non-existent items will incorrectly return "possibly exists".

### Performance Comparison

| Library | FPP | Contains (M/s) | Add (M/s) | Remove (M/s) | Memory (KB) |
|---------|-----|----------------|-----------|--------------|-------------|
| autoscale_cuckoo_filter | 0.17% | 100.84 (1.00) | 34.08 (1.00) | 20.82 (1.00) | 353.0 |
| scalable_cuckoo_filter | 0.15% | 18.08 (0.18) | 11.28 (0.33) | 18.26 (0.88) | 353.0 |
| cuckoofilter | 0.27% | 22.01 (0.22) | 21.23 (0.62) | 14.18 (0.68) | 1024.0 |

*Ratio in parentheses: relative to autoscale_cuckoo_filter (1.00 = baseline)*