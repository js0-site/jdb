## PGM-Index Benchmark

Performance comparison of PGM-Index vs Binary Search with different epsilon values.

### Data Size: 1,000,000

| Algorithm | Epsilon | Mean Time | Std Dev | Throughput | Memory |
|-----------|---------|-----------|---------|------------|--------|
| jdb_pgm | 32 | 19.91ns | 66.98ns | 50.23M/s | 9.01 MB |
| jdb_pgm | 64 | 20.46ns | 71.18ns | 48.87M/s | 8.50 MB |
| jdb_pgm | 128 | 22.18ns | 72.06ns | 45.09M/s | 8.25 MB |
| pgm_index | 32 | 26.82ns | 190.03ns | 37.29M/s | 8.35 MB |
| pgm_index | 64 | 28.76ns | 83.53ns | 34.77M/s | 8.34 MB |
| pgm_index | 128 | 31.13ns | 82.36ns | 32.12M/s | 8.04 MB |
| Binary Search | null | 44.97ns | 117.01ns | 22.24M/s | - |
| HashMap | null | 46.13ns | 101.80ns | 21.68M/s | 40.00 MB |
| BTreeMap | null | 99.45ns | 155.36ns | 10.06M/s | 16.86 MB |

### Accuracy Comparison: jdb_pgm vs pgm_index

| Data Size | Epsilon | jdb_pgm (Max) | jdb_pgm (Avg) | pgm_index (Max) | pgm_index (Avg) |
|-----------|---------|---------------|---------------|-----------------|------------------|
| 1,000,000 | 128 | 128 | 46.44 | 891 | 326.85 |
| 1,000,000 | 32 | 32 | 11.29 | 891 | 326.85 |
| 1,000,000 | 64 | 64 | 22.54 | 891 | 326.85 |
### Build Time Comparison: jdb_pgm vs pgm_index

| Data Size | Epsilon | jdb_pgm (Time) | pgm_index (Time) | Speedup |
|-----------|---------|---------------------|-----------------|---------|
| 1,000,000 | 128 | 2.10ms | 1.21ms | 0.57x |
| 1,000,000 | 32 | 2.04ms | 1.18ms | 0.58x |
| 1,000,000 | 64 | 2.06ms | 1.28ms | 0.62x |
### Configuration
Query Count: 1500000
Data Sizes: 10,000, 100,000, 1,000,000
Epsilon Values: 32, 64, 128



---

### Epsilon (ε) Explained

*Epsilon (ε) controls the accuracy-speed trade-off:*

*Mathematical definition: ε defines the maximum absolute error between the predicted position and the actual position in the data array. When calling `load(data, epsilon, ...)`, ε guarantees |pred - actual| ≤ ε, where positions are indices within the data array of length `data.len()`.*

*Example: For 1M elements with ε=32, if the actual key is at position 1000:*
- ε=32 predicts position between 968-1032, then checks up to 64 elements
- ε=128 predicts position between 872-1128, then checks up to 256 elements


### Notes
#### What is PGM-Index?
PGM-Index (Piecewise Geometric Model Index) is a learned index structure that approximates the distribution of keys with piecewise linear models.
It provides O(log ε) search time with guaranteed error bounds, where ε controls the trade-off between memory and speed.

#### Why Compare with Binary Search?
Binary search is the baseline for sorted array lookup. PGM-Index aims to:
- Match or exceed binary search performance
- Reduce memory overhead compared to traditional indexes
- Provide better cache locality for large datasets

#### Environment
- OS: macOS 26.1 (arm64)
- CPU: Apple M2 Max
- Cores: 12
- Memory: 64.0GB
- Rust: rustc 1.94.0-nightly (8d670b93d 2025-12-31)

#### References
- [PGM-Index Paper](https://doi.org/10.1145/3373718.3394764)
- [Official PGM-Index Site](https://pgm.di.unipi.it/)
- [Learned Indexes](https://arxiv.org/abs/1712.01208)
