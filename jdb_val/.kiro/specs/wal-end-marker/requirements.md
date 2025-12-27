# Requirements Document

## Introduction

为 WAL 文件添加尾部标记（End Marker），用于快速定位最后有效条目位置，避免启动时全文件扫描。当尾部标记损坏时，回退到正向扫描恢复。

## Glossary

- **End_Marker**: 数据条目尾部标记，包含 head_offset (u64) + magic (4字节)
- **WAL**: Write-Ahead Log，预写日志
- **Head**: 64字节的数据条目头部结构
- **Magic**: 4字节魔数，标识数据结束 `0xED_ED_ED_ED`
- **Corrupted_Entry**: CRC 校验失败或长度不匹配的损坏条目

## Requirements

### Requirement 1: End Marker 结构

**User Story:** As a developer, I want a compact end marker structure, so that I can quickly locate the last valid entry.

#### Acceptance Criteria

1. THE End_Marker SHALL consist of head_offset (u64, 8 bytes) followed by magic (4 bytes), totaling 12 bytes
2. THE End_Marker SHALL store head_offset as little-endian u64
3. THE End_Marker SHALL use magic value `0xED_ED_ED_ED` to identify data end

### Requirement 2: End Marker 写入

**User Story:** As a developer, I want end markers written after each entry, so that recovery can quickly find the last valid position.

#### Acceptance Criteria

1. WHEN a Head is written, THE Wal SHALL append an End_Marker immediately after the entry data
2. THE End_Marker head_offset SHALL point to the start position of the corresponding Head
3. WHEN writing End_Marker, THE Wal SHALL update cur_pos to include the 12-byte marker

### Requirement 3: 快速恢复

**User Story:** As a developer, I want fast recovery using end markers, so that startup time is minimized.

#### Acceptance Criteria

1. WHEN opening a WAL file, THE Wal SHALL first attempt to read the last 12 bytes as End_Marker
2. IF the last 12 bytes contain valid magic, THE Wal SHALL use head_offset to locate the last Head
3. IF the located Head has valid CRC, THE Wal SHALL set cur_pos to head_offset + entry_length + 12
4. IF fast recovery succeeds, THE Wal SHALL skip full file scan

### Requirement 4: 回退扫描恢复

**User Story:** As a developer, I want fallback to full scan when end marker is invalid, so that data integrity is maintained.

#### Acceptance Criteria

1. IF the last 12 bytes do not contain valid magic, THE Wal SHALL perform forward scan recovery
2. IF the head_offset points to invalid Head (CRC mismatch), THE Wal SHALL perform forward scan recovery
3. WHEN forward scan encounters a corrupted entry (CRC mismatch), THE Wal SHALL search forward for next magic marker
4. WHEN magic marker is found after corrupted entry, THE Wal SHALL skip the corrupted entry and log warning with entry length
5. THE Wal SHALL continue scanning from the position after the found magic marker

### Requirement 5: 损坏条目日志

**User Story:** As a developer, I want corrupted entries logged, so that I can diagnose data issues.

#### Acceptance Criteria

1. WHEN a corrupted entry is detected, THE Wal SHALL log a warning message
2. THE warning message SHALL include the corrupted entry's offset position
3. THE warning message SHALL include the corrupted entry's expected length (if determinable)
