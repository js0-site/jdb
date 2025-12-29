# size_lru : 智能大小感知缓存库

具备智能淘汰策略的高性能大小感知缓存库，优化内存使用。

## 目录

- [功能特性](#功能特性)
- [安装指南](#安装指南)
- [使用演示](#使用演示)
- [接口参考](#接口参考)
- [设计架构](#设计架构)
- [技术堆栈](#技术堆栈)
- [目录结构](#目录结构)
- [历史背景](#历史背景)

## 功能特性

- **大小感知**：基于对象实际大小而非数量优化存储。
- **智能淘汰**：实现 LHD (最低命中密度) 算法以最大化命中率。
- **常数复杂度**：确保获取、设置和删除操作的 O(1) 时间复杂度。
- **自适应调整**：自动调整内部参数以匹配工作负载模式。
- **零开销**：提供 `NoCache` 实现用于性能基准测试。

## 安装指南

在 `Cargo.toml` 中添加：

```toml
[dependencies]
size_lru = { version = "0.1.0", features = ["lhd"] }
```

## 使用演示

演示代码基于 `tests/main.rs`。

### 基础操作

```rust
use size_lru::Lhd;

fn main() {
  // 初始化指定容量的缓存
  let mut cache: Lhd<&str, &str> = Lhd::new(1024);

  // 设置带有明确大小的值
  cache.set("k1", "v1", 10);
  cache.set("k2", "v2", 20);

  // 获取值
  assert_eq!(cache.get(&"k1"), Some(&"v1"));

  // 检查状态
  assert_eq!(cache.len(), 2);
  assert_eq!(cache.size(), 30);

  // 删除值
  cache.rm(&"k2");
  assert_eq!(cache.get(&"k2"), None);
}
```

### 通用 Trait 用法

```rust
use size_lru::{SizeLru, Lhd};

fn cache_op<K, V>(cache: &mut impl SizeLru<K, V>, key: K, val: V, size: u32) {
  cache.set(key, val, size);
}
```

## 接口参考

### `trait SizeLru<K, V>`

缓存实现的各项核心接口。

- `fn get(&mut self, key: &K) -> Option<&V>`: 获取值引用。更新命中统计信息。
- `fn set(&mut self, key: K, val: V, size: u32)`: 插入或更新值。若超出容量将触发淘汰。
- `fn rm(&mut self, key: &K)`: 按键删除值。

### `struct Lhd<K, V>`

LHD 算法实现。

- `fn new(max: usize) -> Self`: 创建具有最大字节容量的新实例。
- `fn size(&self) -> usize`: 返回存储项目的总大小（字节）。
- `fn len(&self) -> usize`: 返回存储项目的数量。
- `fn is_empty(&self) -> bool`: 如果缓存不包含任何项目，返回真。

## 设计架构

### 架构图

```mermaid
graph TD
  User[用户代码] --> Trait[SizeLru Trait]
  Trait --> |impl| Lhd[Lhd 结构体]
  Trait --> |impl| No[NoCache 结构体]
  
  subgraph Lhd_Internals [Lhd 实现]
    Lhd --> Index[HashMap 索引]
    Lhd --> Entries[Vec 条目]
    Lhd --> Stats[分类统计]
    
    Entries --> EntryData[键, 值, 大小, 时间戳]
    Stats --> Hits[命中计数]
    Stats --> Evicts[淘汰计数]
  end
```

### 淘汰逻辑

```mermaid
graph TD
  Start[设置操作] --> CheckExist{键是否存在?}
  CheckExist --是--> Update[更新值和大小]
  CheckExist --否--> CheckCap{超出容量?}
  
  CheckCap --否--> Insert[插入新条目]
  CheckCap --是--> EvictStart[开始淘汰]
  
  subgraph Eviction_Process [LHD 淘汰]
    EvictStart --> Sample[采样 N 个候选]
    Sample --> Calc[计算命中密度]
    Calc --> Select["选择牺牲者 (最小密度)"]
    Select --> Remove[移除牺牲者]
    Remove --> CheckCap
  end
  
  Update --> End[完成]
  Insert --> End
```

### 分代机制详解

```mermaid
graph TD
  Access[访问条目] --> AgeCalc[计算年龄: current_ts - entry_ts]
  AgeCalc --> Coarsen[年龄粗化: age >> shift]
  Coarsen --> AgeBucket["年龄桶: min(coarsened_age, MAX_AGE-1)"]
  
  subgraph ClassMapping [类别映射]
    AgeBucket --> Sum[last_age + prev_age]
    Sum --> LogScale["对数映射: class_id = 32 - leading_zeros(sum) - 19"]
    LogScale --> ClassSelect["选择类别: min(log_result, AGE_CLASSES-1)"]
  end
  
  ClassSelect --> UpdateStats[更新类别统计]
  
  subgraph AgeClasses [年龄类别结构]
    ClassSelect --> Class0[类别 0: 新访问]
    ClassSelect --> Class1[类别 1: 偶尔访问]
    ClassSelect --> Class2[类别 2: 中等频率]
    ClassSelect --> ClassN["类别 N: 高频率 (N=15)"]
    
    Class0 --> Buckets0[4096 个年龄桶]
    Class1 --> Buckets1[4096 个年龄桶]
    Class2 --> Buckets2[4096 个年龄桶]
    ClassN --> BucketsN[4096 个年龄桶]
  end
```

### 命中率计算机制

```mermaid
graph TD
  Reconfig[重新配置触发] --> Decay[应用 EWMA 衰减]
  Decay --> Iterate[反向迭代年龄桶]
  
  subgraph DensityCalc [密度计算]
    Iterate --> Init[初始化: events=0, hits=0, life=0]
    Init --> Loop["从 MAX_AGE-1 到 0 循环"]
    Loop --> AddHits["hits += hits[age]"]
    Loop --> AddEvents["events += hits[age] + evicts[age]"]
    Loop --> AddLife["life += events"]
    Loop --> CalcDensity["density[age] = hits / life"]
  end
  
  CalcDensity --> NextAge{还有年龄桶?}
  NextAge --是--> Loop
  NextAge --否--> Complete[密度计算完成]
  
  subgraph HitStats [命中统计更新]
    AccessEntry[条目被访问] --> GetClass[获取类别 ID]
    GetClass --> GetAge[获取年龄桶]
    GetAge --> Increment["hits[class][age] += 1.0"]
  end
```

### 密度计算与淘汰流程

```mermaid
graph TD
  EvictStart[开始淘汰] --> Sample[采样 N 个候选]
  Sample --> CalcDensity[计算每个候选的命中密度]
  
  subgraph DensityFormula [密度计算公式]
    CalcDensity --> GetEntry[获取条目信息]
    GetEntry --> CalcAge["计算年龄: (ts - entry_ts) >> shift"]
    CalcAge --> GetClass["获取类别: class_id(last_age + prev_age)"]
    GetClass --> Lookup["查表: density = classes[class].density[age]"]
    Lookup --> Normalize["归一化: density / size"]
  end
  
  Normalize --> Compare[比较密度值]
  Compare --> Select["选择牺牲者 (最小密度)"]
  Select --> Remove[移除牺牲者]
  Remove --> UpdateEvictStats["更新淘汰统计: evicts[class][age] += 1.0"]
  UpdateEvictStats --> CheckCapacity{仍超容量?}
  CheckCapacity --是--> Sample
  CheckCapacity --否--> End[淘汰完成]
```

## 技术堆栈

- **Rust**: 系统编程语言。
- **gxhash**: 高性能非加密哈希。
- **fastrand**: 高效伪随机数生成。

## 目录结构

```
src/
  lib.rs    # Trait 定义和模块导出
  lhd.rs    # LHD 算法实现
  no.rs     # 空操作实现
tests/
  main.rs   # 集成测试和演示
readme/
  en.md     # 英文文档
  zh.md     # 中文文档
```

## 历史背景

**LHD (最低命中密度)** 算法源自 NSDI '18 论文《LHD: Improving Cache Hit Rate by Maximizing Hit Density》。作者 (Beckmann 等人) 提议用概率框架替代复杂的启发式算法。LHD 不问“哪一项最近最少使用？”，而是问“哪一项单位空间的预期命中率最低？”。通过根据对象年龄和大小估算未来命中的概率，LHD 最大化了缓存的总命中率。本实现将这些理论成果转化为实用的 Rust 库。

### 参考文献

- **论文**: [LHD: Improving Cache Hit Rate by Maximizing Hit Density](https://www.usenix.org/conference/nsdi18/presentation/beckmann) (NSDI '18)
- **实现**: [官方模拟代码](https://github.com/beckmann/cache_replacement)
- **PDF**: [下载论文](https://www.usenix.org/system/files/conference/nsdi18/nsdi18-beckmann.pdf)