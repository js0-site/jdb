//! Property tests for GC state machine / GC 状态机属性测试
//!
//! Feature: gc, Property 1: State Machine Transitions
//! Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5

use jdb_gc::{GcState, GcWorker};
use proptest::prelude::*;

/// Actions that can be performed on GcWorker / 可在 GcWorker 上执行的操作
#[derive(Debug, Clone)]
enum Action {
  Start,
  StartSweep,
  Finish,
  Reset,
  UpdateMarking(usize),
  UpdateSweeping(usize),
}

/// Generate arbitrary action / 生成任意操作
fn arb_action() -> impl Strategy<Value = Action> {
  prop_oneof![
    Just(Action::Start),
    Just(Action::StartSweep),
    Just(Action::Finish),
    Just(Action::Reset),
    (0usize..10).prop_map(Action::UpdateMarking),
    (0usize..10).prop_map(Action::UpdateSweeping),
  ]
}

/// Check if state is Marking / 检查状态是否为 Marking
fn is_marking(state: &GcState) -> bool {
  matches!(state, GcState::Marking { .. })
}

/// Check if state is Sweeping / 检查状态是否为 Sweeping
fn is_sweeping(state: &GcState) -> bool {
  matches!(state, GcState::Sweeping { .. })
}

/// Check if state is Done / 检查状态是否为 Done
fn is_done(state: &GcState) -> bool {
  matches!(state, GcState::Done)
}

proptest! {
  #![proptest_config(ProptestConfig::with_cases(100))]

  /// Property 1: State Machine Transitions
  /// For any GcWorker, the state transitions must follow:
  /// Idle → Marking → Sweeping → Done, and reset() returns to Idle from any state.
  /// Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5
  #[test]
  fn prop_state_machine_transitions(actions in prop::collection::vec(arb_action(), 0..50)) {
    let mut worker = GcWorker::new();

    // Requirement 4.1: Initial state is Idle / 初始状态为 Idle
    prop_assert!(worker.is_idle(), "Initial state should be Idle");

    for action in actions {
      let prev_state = worker.state().clone();

      match action {
        Action::Start => {
          worker.start();
          // Requirement 4.2: start() transitions to Marking / start() 转换到 Marking
          prop_assert!(is_marking(worker.state()), "start() should transition to Marking");
        }
        Action::StartSweep => {
          // Only valid from Marking state / 只在 Marking 状态有效
          if is_marking(&prev_state) {
            worker.start_sweep();
            // Requirement 4.3: start_sweep() transitions to Sweeping / start_sweep() 转换到 Sweeping
            prop_assert!(is_sweeping(worker.state()), "start_sweep() should transition to Sweeping");
          }
        }
        Action::Finish => {
          // Only valid from Sweeping state / 只在 Sweeping 状态有效
          if is_sweeping(&prev_state) {
            worker.finish();
            // Requirement 4.4: finish() transitions to Done / finish() 转换到 Done
            prop_assert!(is_done(worker.state()), "finish() should transition to Done");
          }
        }
        Action::Reset => {
          worker.reset();
          // Requirement 4.5: reset() returns to Idle from any state / reset() 从任何状态返回 Idle
          prop_assert!(worker.is_idle(), "reset() should return to Idle from any state");
        }
        Action::UpdateMarking(idx) => {
          if is_marking(&prev_state) {
            worker.update_marking(idx, None);
            prop_assert!(is_marking(worker.state()), "update_marking should stay in Marking");
          }
        }
        Action::UpdateSweeping(idx) => {
          if is_sweeping(&prev_state) {
            worker.update_sweeping(idx);
            prop_assert!(is_sweeping(worker.state()), "update_sweeping should stay in Sweeping");
          }
        }
      }
    }
  }

  /// Test valid transition sequence: Idle → Marking → Sweeping → Done
  /// 测试有效转换序列：Idle → Marking → Sweeping → Done
  #[test]
  fn prop_valid_transition_sequence(_seed in any::<u64>()) {
    let mut worker = GcWorker::new();

    // Idle state / Idle 状态
    prop_assert!(worker.is_idle());
    prop_assert!(!worker.is_done());

    // Idle → Marking / Idle → Marking
    worker.start();
    prop_assert!(!worker.is_idle());
    prop_assert!(is_marking(worker.state()));

    // Marking → Sweeping / Marking → Sweeping
    worker.start_sweep();
    prop_assert!(is_sweeping(worker.state()));

    // Sweeping → Done / Sweeping → Done
    worker.finish();
    prop_assert!(worker.is_done());

    // Done → Idle via reset / Done → Idle 通过 reset
    worker.reset();
    prop_assert!(worker.is_idle());
  }

  /// Test reset from any state / 测试从任何状态重置
  #[test]
  fn prop_reset_from_any_state(actions in prop::collection::vec(arb_action(), 0..20)) {
    let mut worker = GcWorker::new();

    // Apply some actions to get to a random state / 应用一些操作到达随机状态
    for action in actions {
      match action {
        Action::Start => worker.start(),
        Action::StartSweep if is_marking(worker.state()) => worker.start_sweep(),
        Action::Finish if is_sweeping(worker.state()) => worker.finish(),
        Action::UpdateMarking(idx) if is_marking(worker.state()) => worker.update_marking(idx, None),
        Action::UpdateSweeping(idx) if is_sweeping(worker.state()) => worker.update_sweeping(idx),
        _ => {}
      }
    }

    // Reset should always return to Idle / reset 应该总是返回 Idle
    worker.reset();
    prop_assert!(worker.is_idle(), "reset() should always return to Idle");
  }

  /// Test that start() clears state / 测试 start() 清除状态
  #[test]
  fn prop_start_clears_state(_seed in any::<u64>()) {
    let mut worker = GcWorker::new();

    // Add some data / 添加一些数据
    worker.start();
    worker.inc_keys(100);
    worker.inc_tables();

    // Start again should reset stats / 再次 start 应该重置统计
    worker.start();
    prop_assert_eq!(worker.stats().keys_scanned, 0);
    prop_assert_eq!(worker.stats().tables_scanned, 0);
    prop_assert!(is_marking(worker.state()));
  }
}
