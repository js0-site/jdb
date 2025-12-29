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
| autoscale_cuckoo_filter | 0.17% | 99.97 (1.00) | 30.34 (1.00) | 49.55 (1.00) | 353.0 |
| scalable_cuckoo_filter | 0.15% | 17.99 (0.18) | 18.95 (0.62) | 18.48 (0.37) | 353.0 |
| cuckoofilter | 0.27% | 21.51 (0.22) | 22.72 (0.75) | 13.02 (0.26) | 1024.0 |

*Ratio in parentheses: relative to autoscale_cuckoo_filter (1.00 = baseline)*