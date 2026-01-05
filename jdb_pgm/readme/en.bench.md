## Pgm-Index Benchmark

Performance comparison of Pgm-Index vs Binary Search with different epsilon values.

### Data Size: 1,000,000

| Algorithm | Epsilon | Mean Time | Std Dev | Throughput | Memory |
|-----------|---------|-----------|---------|------------|--------|
| jdb_pgm | 64 | 15.84ns | 41.83ns | 63.14M/s | 8.50 MB |
| jdb_pgm | 32 | 16.59ns | 37.89ns | 60.28M/s | 9.01 MB |
| jdb_pgm | 128 | 17.47ns | 43.31ns | 57.25M/s | 8.25 MB |
| pgm_index | 32 | 19.77ns | 49.60ns | 50.57M/s | 8.35 MB |
| pgm_index | 64 | 22.11ns | 49.08ns | 45.22M/s | 8.34 MB |
| pgm_index | 128 | 25.86ns | 119.46ns | 38.67M/s | 8.04 MB |
| HashMap | null | 29.44ns | 57.93ns | 33.97M/s | 40.00 MB |
| Binary Search | null | 39.09ns | 69.91ns | 25.58M/s | - |
| BTreeMap | null | 79.62ns | 91.66ns | 12.56M/s | 16.86 MB |

### Accuracy Comparison: jdb_pgm vs pgm_index

| Data Size | Epsilon | jdb_pgm (Max) | jdb_pgm (Avg) | pgm_index (Max) | pgm_index (Avg) |
|-----------|---------|---------------|---------------|-----------------|------------------|
| 1,000,000 | 128 | 128 | 46.80 | 1024 | 511.28 |
| 1,000,000 | 32 | 32 | 11.35 | 256 | 127.48 |
| 1,000,000 | 64 | 64 | 22.59 | 512 | 255.39 |
### Build Time Comparison: jdb_pgm vs pgm_index

| Data Size | Epsilon | jdb_pgm (Time) | pgm_index (Time) | Speedup |
|-----------|---------|---------------------|-----------------|---------|
| 1,000,000 | 128 | 1.99ms | 1.18ms | 0.59x |
| 1,000,000 | 32 | 2.56ms | 1.81ms | 0.71x |
| 1,000,000 | 64 | 2.17ms | 1.34ms | 0.62x |
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
#### What is Pgm-Index?
Pgm-Index (Piecewise Geometric Model Index) is a learned index structure that approximates the distribution of keys with piecewise linear models.
It provides O(log ε) search time with guaranteed error bounds, where ε controls the trade-off between memory and speed.

#### Why Compare with Binary Search?
Binary search is the baseline for sorted array lookup. Pgm-Index aims to:
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
- [Pgm-Index Paper](https://doi.org/10.1145/3373718.3394764)
- [Official Pgm-Index Site](https://pgm.di.unipi.it/)
- [Learned Indexes](https://arxiv.org/abs/1712.01208)
