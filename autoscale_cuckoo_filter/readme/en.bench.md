## Benchmark Results

Test: 100000 items, capacity=200000, target FPP≈1%

### What is FPP?

**FPP (False Positive Probability)** is the probability that a filter incorrectly reports an item as present when it was never added. Lower FPP means higher accuracy but requires more memory. A typical FPP of 1% means about 1 in 100 queries for non-existent items will incorrectly return "possibly exists".

### Performance Comparison

| Library | FPP | Contains (M/s) | Add (M/s) | Remove (M/s) | Memory (KB) |
|---------|-----|----------------|-----------|--------------|-------------|
| autoscale_cuckoo_filter | 0.17% | 43.69 (1.00) | 27.63 (1.00) | 47.37 (1.00) | 353.0 |
| scalable_cuckoo_filter | 0.15% | 16.48 (0.38) | 18.16 (0.66) | 17.62 (0.37) | 353.0 |
| cuckoofilter | 0.27% | 20.46 (0.47) | 21.30 (0.77) | 12.02 (0.25) | 1024.0 |

*Ratio in parentheses: relative to autoscale_cuckoo_filter (1.00 = baseline)*
