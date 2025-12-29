# size_lru : Intelligent Size-Aware Cache Library

High-performance, size-aware cache library implementing intelligent eviction strategies for optimal memory usage.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [API Reference](#api-reference)
- [Design](#design)
- [Technology Stack](#technology-stack)
- [Directory Structure](#directory-structure)
- [History](#history)

## Features

- **Size Awareness**: Optimizes storage based on actual object size rather than count.
- **Intelligent Eviction**: Implements LHD (Least Hit Density) algorithm to maximize hit rate.
- **Constant Complexity**: Guarantees O(1) time access for get, set, and remove operations.
- **Adaptive Tuning**: Automatically adjusts internal parameters to match workload patterns.
- **Zero Overhead**: Provides `NoCache` implementation for performance baselining.

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
size_lru = { version = "0.1.0", features = ["lhd"] }
```

## Usage

Demonstration code based on `tests/main.rs`.

### Basic Operations

```rust
use size_lru::Lhd;

fn main() {
  // Initialize cache with capacity
  let mut cache: Lhd<&str, &str> = Lhd::new(1024);

  // Set values with explicit size
  cache.set("k1", "v1", 10);
  cache.set("k2", "v2", 20);

  // Retrieve value
  assert_eq!(cache.get(&"k1"), Some(&"v1"));

  // Check state
  assert_eq!(cache.len(), 2);
  assert_eq!(cache.size(), 30);

  // Remove value
  cache.rm(&"k2");
  assert_eq!(cache.get(&"k2"), None);
}
```

### Generic Trait Usage

```rust
use size_lru::{SizeLru, Lhd};

fn cache_op<K, V>(cache: &mut impl SizeLru<K, V>, key: K, val: V, size: u32) {
  cache.set(key, val, size);
}
```

## API Reference

### `trait SizeLru<K, V>`

Core interface for cache implementations.

- `fn get(&mut self, key: &K) -> Option<&V>`: Retrieve reference to value. Updates hit statistics.
- `fn set(&mut self, key: K, val: V, size: u32)`: Insert or update value. Triggers eviction if capacity exceeded.
- `fn rm(&mut self, key: &K)`: Remove value by key.

### `struct Lhd<K, V>`

LHD algorithm implementation.

- `fn new(max: usize) -> Self`: Create new instance with maximum byte capacity.
- `fn size(&self) -> usize`: Return total size of stored items in bytes.
- `fn len(&self) -> usize`: Return count of stored items.
- `fn is_empty(&self) -> bool`: Return true if cache contains no items.

## Design

### Architecture

```mermaid
graph TD
  User[User Code] --> Trait[SizeLru Trait]
  Trait --> |impl| Lhd[Lhd Struct]
  Trait --> |impl| No[NoCache Struct]
  
  subgraph Lhd_Internals [Lhd Implementation]
    Lhd --> Index[HashMap Index]
    Lhd --> Entries[Vec Entry]
    Lhd --> Stats[Class Stats]
    
    Entries --> EntryData[Key, Val, Size, TS]
    Stats --> Hits[Hit Counters]
    Stats --> Evicts[Eviction Counters]
  end
```

### Eviction Logic

```mermaid
graph TD
  Start[Set Operation] --> CheckExist{Key Exists?}
  CheckExist --Yes--> Update[Update Value & Size]
  CheckExist --No--> CheckCap{Over Capacity?}
  
  CheckCap --No--> Insert[Insert New Entry]
  CheckCap --Yes--> EvictStart[Start Eviction]
  
  subgraph Eviction_Process [LHD Eviction]
    EvictStart --> Sample[Sample N Candidates]
    Sample --> Calc[Calculate Hit Density]
    Calc --> Select["Select Victim (Min Density)"]
    Select --> Remove[Remove Victim]
    Remove --> CheckCap
  end
  
  Update --> End[Done]
  Insert --> End
```

### Generational Mechanism

```mermaid
graph TD
  Access[Access Entry] --> AgeCalc[Calculate Age: current_ts - entry_ts]
  AgeCalc --> Coarsen[Age Coarsening: age >> shift]
  Coarsen --> AgeBucket["Age Bucket: min(coarsened_age, MAX_AGE-1)"]
  
  subgraph ClassMapping [Class Mapping]
    AgeBucket --> Sum[last_age + prev_age]
    Sum --> LogScale["Log Mapping: class_id = 32 - leading_zeros(sum) - 19"]
    LogScale --> ClassSelect["Select Class: min(log_result, AGE_CLASSES-1)"]
  end
  
  ClassSelect --> UpdateStats[Update Class Statistics]
  
  subgraph AgeClasses [Age Class Structure]
    ClassSelect --> Class0[Class 0: New Access]
    ClassSelect --> Class1[Class 1: Occasional Access]
    ClassSelect --> Class2[Class 2: Medium Frequency]
    ClassSelect --> ClassN["Class N: High Frequency (N=15)"]
    
    Class0 --> Buckets0[4096 Age Buckets]
    Class1 --> Buckets1[4096 Age Buckets]
    Class2 --> Buckets2[4096 Age Buckets]
    ClassN --> BucketsN[4096 Age Buckets]
  end
```

### Hit Rate Calculation Mechanism

```mermaid
graph TD
  Reconfig[Reconfiguration Trigger] --> Decay[Apply EWMA Decay]
  Decay --> Iterate[Iterate Age Buckets Backwards]
  
  subgraph DensityCalc [Density Calculation]
    Iterate --> Init[Initialize: events=0, hits=0, life=0]
    Init --> Loop["Loop from MAX_AGE-1 to 0"]
    Loop --> AddHits["hits += hits[age]"]
    Loop --> AddEvents["events += hits[age] + evicts[age]"]
    Loop --> AddLife["life += events"]
    Loop --> CalcDensity["density[age] = hits / life"]
  end
  
  CalcDensity --> NextAge{More Buckets?}
  NextAge --Yes--> Loop
  NextAge --No--> Complete[Density Calculation Complete]
  
  subgraph HitStats [Hit Statistics Update]
    AccessEntry[Entry Accessed] --> GetClass[Get Class ID]
    GetClass --> GetAge[Get Age Bucket]
    GetAge --> Increment["hits[class][age] += 1.0"]
  end
```

### Density Calculation and Eviction Flow

```mermaid
graph TD
  EvictStart[Start Eviction] --> Sample[Sample N Candidates]
  Sample --> CalcDensity[Calculate Hit Density for Each Candidate]
  
  subgraph DensityFormula [Density Calculation Formula]
    CalcDensity --> GetEntry[Get Entry Information]
    GetEntry --> CalcAge["Calculate Age: (ts - entry_ts) >> shift"]
    CalcAge --> GetClass["Get Class: class_id(last_age + prev_age)"]
    GetClass --> Lookup["Lookup: density = classes[class].density[age]"]
    Lookup --> Normalize["Normalize: density / size"]
  end
  
  Normalize --> Compare[Compare Density Values]
  Compare --> Select["Select Victim (Min Density)"]
  Select --> Remove[Remove Victim]
  Remove --> UpdateEvictStats["Update Eviction Stats: evicts[class][age] += 1.0"]
  UpdateEvictStats --> CheckCapacity{Still Over Capacity?}
  CheckCapacity --Yes--> Sample
  CheckCapacity --No--> End[Eviction Complete]
```

## Technology Stack

- **Rust**: Systems programming language.
- **gxhash**: High-performance, non-cryptographic hashing.
- **fastrand**: Efficient pseudo-random number generation.

## Directory Structure

```
src/
  lib.rs    # Trait definitions and module exports
  lhd.rs    # LHD algorithm implementation
  no.rs     # No-op implementation
tests/
  main.rs   # Integration tests and demos
readme/
  en.md     # English documentation
  zh.md     # Chinese documentation
```

## History

The **LHD (Least Hit Density)** algorithm originates from the NSDI '18 paper "LHD: Improving Cache Hit Rate by Maximizing Hit Density". The authors (Beckmann et al.) proposed replacing complex heuristics with a probabilistic framework. Instead of asking "which item was least recently used?", LHD asks "which item has the least expected hits per unit of space?". By estimating the probability of future hits based on object age and size, LHD maximizes the total hit rate of the cache. This implementation brings these theoretical gains to a practical Rust library.

### References

- **Paper**: [LHD: Improving Cache Hit Rate by Maximizing Hit Density](https://www.usenix.org/conference/nsdi18/presentation/beckmann) (NSDI '18)
- **Implementation**: [Official Simulation Code](https://github.com/beckmann/cache_replacement)
- **PDF**: [Download Paper](https://www.usenix.org/system/files/conference/nsdi18/nsdi18-beckmann.pdf)