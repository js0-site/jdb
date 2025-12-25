//! Table configuration / 表配置

/// History keep policy / 历史保留策略
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Keep {
  /// Keep only current value / 只保留当前值
  #[default]
  Current,
  /// Keep N versions / 保留 N 个版本
  Versions(usize),
  /// Keep for duration (ms) / 保留指定时长（毫秒）
  Duration(u64),
  /// Keep all history / 保留全部历史
  All,
}

impl Keep {
  /// Check if should keep version / 检查是否保留版本
  /// idx: 0 = current, 1 = prev, ...
  #[inline]
  pub fn should_keep(&self, idx: usize, age_ms: Option<u64>) -> bool {
    match self {
      Self::Current => idx == 0,
      Self::Versions(n) => idx < *n,
      Self::Duration(max_ms) => age_ms.is_none_or(|age| age <= *max_ms),
      Self::All => true,
    }
  }

  /// Max versions to keep (None = unlimited) / 最大保留版本数
  #[inline]
  pub fn max_versions(&self) -> Option<usize> {
    match self {
      Self::Current => Some(1),
      Self::Versions(n) => Some(*n),
      Self::Duration(_) | Self::All => None,
    }
  }

  /// Max age in ms (None = unlimited) / 最大保留时长
  #[inline]
  pub fn max_age_ms(&self) -> Option<u64> {
    match self {
      Self::Duration(ms) => Some(*ms),
      _ => None,
    }
  }
}

/// Table configuration / 表配置
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conf {
  /// Set keep policy / 设置保留策略
  Keep(Keep),
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn keep_current() {
    let k = Keep::Current;
    assert!(k.should_keep(0, None));
    assert!(!k.should_keep(1, None));
    assert_eq!(k.max_versions(), Some(1));
  }

  #[test]
  fn keep_versions() {
    let k = Keep::Versions(3);
    assert!(k.should_keep(0, None));
    assert!(k.should_keep(2, None));
    assert!(!k.should_keep(3, None));
    assert_eq!(k.max_versions(), Some(3));
  }

  #[test]
  fn keep_duration() {
    let k = Keep::Duration(1000);
    assert!(k.should_keep(0, Some(500)));
    assert!(k.should_keep(0, Some(1000)));
    assert!(!k.should_keep(0, Some(1001)));
    assert!(k.should_keep(100, None)); // no timestamp = keep
    assert_eq!(k.max_age_ms(), Some(1000));
  }

  #[test]
  fn keep_all() {
    let k = Keep::All;
    assert!(k.should_keep(1000, Some(u64::MAX)));
    assert_eq!(k.max_versions(), None);
    assert_eq!(k.max_age_ms(), None);
  }
}
