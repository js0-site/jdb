//! Database configuration / 数据库配置

/// History retention policy / 历史保留策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retention {
  /// Keep only current value / 只保留当前值
  Current,
  /// Keep N versions / 保留 N 个版本
  Versions(usize),
  /// Keep for duration (ms) / 保留指定时长（毫秒）
  Duration(u64),
  /// Keep N versions or duration, whichever is less / 版本数和时长取较少
  VersionsOrDuration(usize, u64),
  /// Keep all history / 保留全部历史
  All,
}

impl Retention {
  /// Check if should keep version / 检查是否保留版本
  /// idx: 0 = current, 1 = prev, ...
  #[inline]
  pub fn should_keep(&self, idx: usize, age_ms: Option<u64>) -> bool {
    match self {
      Self::Current => idx == 0,
      Self::Versions(n) => idx < *n,
      Self::Duration(max_ms) => age_ms.is_none_or(|age| age <= *max_ms),
      Self::VersionsOrDuration(n, max_ms) => {
        idx < *n && age_ms.is_none_or(|age| age <= *max_ms)
      }
      Self::All => true,
    }
  }

  /// Max versions to keep (None = unlimited) / 最大保留版本数
  #[inline]
  pub fn max_versions(&self) -> Option<usize> {
    match self {
      Self::Current => Some(1),
      Self::Versions(n) | Self::VersionsOrDuration(n, _) => Some(*n),
      Self::Duration(_) | Self::All => None,
    }
  }

  /// Max age in ms (None = unlimited) / 最大保留时长
  #[inline]
  pub fn max_age_ms(&self) -> Option<u64> {
    match self {
      Self::Duration(ms) | Self::VersionsOrDuration(_, ms) => Some(*ms),
      _ => None,
    }
  }
}

impl Default for Retention {
  fn default() -> Self {
    Self::Current
  }
}

/// Database configuration / 数据库配置
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbConf {
  /// Set retention policy / 设置保留策略
  Retention(Retention),
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn retention_current() {
    let r = Retention::Current;
    assert!(r.should_keep(0, None));
    assert!(!r.should_keep(1, None));
    assert_eq!(r.max_versions(), Some(1));
  }

  #[test]
  fn retention_versions() {
    let r = Retention::Versions(3);
    assert!(r.should_keep(0, None));
    assert!(r.should_keep(2, None));
    assert!(!r.should_keep(3, None));
    assert_eq!(r.max_versions(), Some(3));
  }

  #[test]
  fn retention_duration() {
    let r = Retention::Duration(1000);
    assert!(r.should_keep(0, Some(500)));
    assert!(r.should_keep(0, Some(1000)));
    assert!(!r.should_keep(0, Some(1001)));
    assert!(r.should_keep(100, None)); // no timestamp = keep
    assert_eq!(r.max_age_ms(), Some(1000));
  }

  #[test]
  fn retention_versions_or_duration() {
    let r = Retention::VersionsOrDuration(3, 1000);
    // idx < 3 AND age <= 1000
    assert!(r.should_keep(0, Some(500)));
    assert!(r.should_keep(2, Some(1000)));
    assert!(!r.should_keep(3, Some(500))); // idx >= 3
    assert!(!r.should_keep(0, Some(1001))); // age > 1000
  }

  #[test]
  fn retention_all() {
    let r = Retention::All;
    assert!(r.should_keep(1000, Some(u64::MAX)));
    assert_eq!(r.max_versions(), None);
    assert_eq!(r.max_age_ms(), None);
  }
}
