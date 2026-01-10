//! Flush logic for Mems
//! Mems 的刷盘逻辑

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use event_listener::Event;
use jdb_base::sst::{Flush, Meta, OnFlush};

use crate::{FlushErr, Mem};

/// Shared inner state
/// 共享内部状态
pub(crate) struct Inner<F, N> {
  pub frozen: BTreeMap<u64, Mem>,
  pub flusher: Option<F>,
  pub notify: N,
  pub flushing: bool,
  pub event: Event,
}

/// Execute a single flush task
/// 执行单个刷盘任务
async fn one<F: Flush>(flusher: &mut F, mem: Mem) -> Result<Meta, FlushErr<F::Error>> {
  flusher
    .flush(mem.data.iter())
    .await
    .map_err(|_| FlushErr::Recv)?
    .map_err(FlushErr::Flush)
}

/// Handle flush result, returns true if should continue
/// 处理刷盘结果，返回 true 表示应继续
fn handle<F, N: OnFlush>(
  inner: &RefCell<Inner<F, N>>,
  id: u64,
  res: Result<Meta, impl std::fmt::Debug>,
) -> bool {
  match res {
    Ok(meta) => {
      let mut inner_mut = inner.borrow_mut();
      inner_mut.frozen.remove(&id);
      inner_mut.notify.on_flush(meta);
      true
    }
    Err(e) => {
      log::error!("mem flush {id} failed: {e:?}");
      false
    }
  }
}

/// Get next frozen to flush
/// 获取下一个待刷盘的 frozen
fn next<F, N>(inner: &RefCell<Inner<F, N>>) -> Option<(u64, Mem)> {
  inner
    .borrow()
    .frozen
    .first_key_value()
    .map(|(&id, m)| (id, m.clone()))
}

/// Mark flush done and notify waiters
/// 标记刷盘完成并通知等待者
fn done<F, N>(inner: &RefCell<Inner<F, N>>) {
  inner.borrow_mut().flushing = false;
  inner.borrow().event.notify(usize::MAX);
}

/// Flush loop
/// 刷盘循环
async fn flush_loop<F: Flush, N: OnFlush>(inner: &RefCell<Inner<F, N>>) {
  while let Some((id, mem)) = next(inner) {
    // Take flusher out to avoid holding RefCell across await
    // 取出 flusher 避免跨 await 持有 RefCell
    let mut flusher = match inner.borrow_mut().flusher.take() {
      Some(f) => f,
      None => break,
    };
    let res = one(&mut flusher, mem).await;
    inner.borrow_mut().flusher = Some(flusher);
    if !handle(inner, id, res) {
      break;
    }
  }
}

/// Flush all frozen memtables, wait if already flushing
/// 刷新所有冻结的内存表，如果正在刷盘则等待
pub(crate) async fn all<F: Flush, N: OnFlush>(inner: &Rc<RefCell<Inner<F, N>>>) {
  // Wait if already flushing
  // 如果正在刷盘则等待
  while inner.borrow().flushing {
    let listener = inner.borrow().event.listen();
    if !inner.borrow().flushing {
      break;
    }
    listener.await;
  }

  inner.borrow_mut().flushing = true;
  flush_loop(inner).await;
  done(inner);
}

/// Spawn background flush task
/// 启动后台刷盘任务
pub(crate) fn spawn<F: Flush, N: OnFlush>(inner: Rc<RefCell<Inner<F, N>>>) {
  if inner.borrow().flushing {
    return;
  }

  inner.borrow_mut().flushing = true;

  compio::runtime::spawn(async move {
    flush_loop(&inner).await;
    done(&inner);
  })
  .detach();
}
