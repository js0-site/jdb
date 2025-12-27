# Implementation Plan: WAL End Marker

## Overview

为 WAL 添加尾部标记机制，实现快速恢复。使用 Rust 实现，遵循项目现有代码风格。

## Tasks

- [x] 1. 创建 End Marker 模块
  - [x] 1.1 创建 `src/wal/end.rs` 实现 `build_end` 和 `parse_end`
    - 定义 `END_SIZE = 12` 和 `END_MAGIC = 0xED_ED_ED_ED`
    - `build_end(head_offset: u64) -> [u8; 12]`
    - `parse_end(buf: &[u8]) -> Option<u64>`
    - _Requirements: 1.1, 1.2, 1.3_
  - [x] 1.2 在 `src/wal/mod.rs` 中导出 end 模块
    - _Requirements: 1.1_
  - [x] 1.3 编写 Property 1 测试: End Marker round-trip
    - **Property 1: End Marker Round-Trip**
    - **Validates: Requirements 1.1, 1.2**

- [x] 2. 修改写入流程
  - [x] 2.1 修改 `src/wal/write.rs` 的 `write_head` 方法
    - 检查空间时包含 END_SIZE
    - 写入 Head 和 infile 数据后追加 End Marker
    - 更新 cur_pos 包含 END_SIZE
    - _Requirements: 2.1, 2.2, 2.3_
  - [x] 2.2 编写 Property 2 测试: Write produces valid End Marker
    - **Property 2: Write Produces Valid End Marker**
    - **Validates: Requirements 2.1, 2.2, 2.3**

- [x] 3. Checkpoint - 确保写入测试通过
  - 运行 `./test.sh` 确保编译通过
  - 确保所有测试通过，有问题请询问用户

- [x] 4. 实现快速恢复
  - [x] 4.1 在 `src/wal/open.rs` 添加 `try_fast_recover` 函数
    - 读取文件末尾 12 字节
    - 验证 magic 和 Head CRC
    - 成功返回 `Some(file_len)`，失败返回 `None`
    - _Requirements: 3.1, 3.2, 3.3, 3.4_
  - [x] 4.2 修改 `find_newest` 使用快速恢复
    - 先尝试 `try_fast_recover`
    - 失败则回退到 `recover_scan_with_skip`
    - _Requirements: 3.1, 4.1, 4.2_
  - [x] 4.3 编写 Property 3 测试: Fast recovery correctness
    - **Property 3: Fast Recovery Correctness**
    - **Validates: Requirements 3.2, 3.3**

- [x] 5. 实现带跳过的扫描恢复
  - [x] 5.1 添加 `search_magic` 函数搜索魔数
    - 从指定位置向前搜索 `0xEDEDEDED`
    - 返回找到的位置或 None
    - _Requirements: 4.3_
  - [x] 5.2 修改 `recover_scan` 为 `recover_scan_with_skip`
    - 遇到损坏条目时搜索 magic 跳过
    - 记录损坏条目日志（位置和长度）
    - 返回最后有效条目结尾位置
    - _Requirements: 4.3, 4.4, 4.5, 5.1, 5.2, 5.3_
  - [x] 5.3 编写 Property 4 测试: Scan recovery position
    - **Property 4: Scan Recovery Position**
    - **Validates: Requirements 4.1, 4.2**
  - [x] 5.4 编写 Property 5 测试: Corrupted entry skip
    - **Property 5: Corrupted Entry Skip**
    - **Validates: Requirements 4.3, 4.4, 4.5**

- [x] 6. 更新 scan 方法
  - [x] 6.1 修改 `read.rs` 中的 `scan` 方法适配新布局
    - 条目长度计算包含 END_SIZE
    - _Requirements: 2.1_

- [x] 7. Final Checkpoint - 确保所有测试通过
  - 运行 `./test.sh` 确保编译通过
  - 确保所有测试通过，有问题请询问用户

## Notes

- 使用 `proptest` 进行属性测试
- 遵循项目代码风格：简洁命名、双语注释、高性能库
