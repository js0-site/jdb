# jdb_compress - 压缩算法模块

定位: 封装 LZ4/Zstd 压缩算法。

## 依赖

```toml
lz4_flex = "0.12"
zstd = "0.13"
thiserror = "2"
```

## 模块结构

```
jdb_compress/src/
└── lib.rs    # 压缩/解压函数
```

## 算法对比

| 算法 | 压缩率 | 压缩速度 | 解压速度 | 场景 |
|-----|-------|---------|---------|-----|
| LZ4 | 2-3x | 800 MB/s | 4 GB/s | 热数据 |
| Zstd | 3-5x | 400 MB/s | 1 GB/s | 冷数据 |

## 核心类型

```rust
/// 压缩算法 Compression codec
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Codec {
  #[default]
  None = 0,
  Lz4 = 1,
  Zstd = 2,
}

/// 压缩 Compress
pub fn enc(codec: Codec, src: &[u8]) -> Vec<u8>;

/// 解压 Decompress
pub fn dec(codec: Codec, src: &[u8]) -> Result<Vec<u8>, Error>;
```

## 设计要点

1. **极简接口**: enc/dec 两个函数
2. **LZ4 带长度前缀**: 解压时自动获取原始大小
3. **Zstd 默认级别 3**: 平衡压缩率和速度
