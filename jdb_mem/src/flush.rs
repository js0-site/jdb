//! Flush logic for Mems
//! Mems 的刷盘逻辑

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use event_listener::Event;
use jdb_base::sst::{Flush, Meta, OnFlush};
use log::error;

use crate::{Mem, MemInner};

/// Shared inner state
/// 共享内部状态
pub(crate) struct Inner<F, N> {
  pub frozen: BTreeMap<u64, Mem>,
  pub flusher: Option<F>,
  pub notify: N,
  pub flushing: bool,
  pub event: Event,
}

/// Flush result type
/// 刷盘结果类型
type FlushResult<F> = Result<
  (
    Result<Result<Meta, <F as Flush>::Error>, oneshot::RecvError>,
    F,
  ),
  oneshot::RecvError,
>;

/// Execute a single flush task in cpu_bind thread
/// 在 cpu_bind 线程中执行单个刷盘任务
async fn one<F: Flush>(flusher: F, id: u64, mem: Mem) -> FlushResult<F> {
  let (tx, rx) = oneshot::channel();
  let ptr = &*mem as *const MemInner as usize;
  // Move mem to closure to keep it alive
  // 将 mem 移动到闭包以保持存活
  let _keep_alive = mem;

  cpu_bind::spawn(move |_| {
    // Safety: ptr valid because mem is alive until rx.await
    // 安全：ptr 有效，因为 mem 在 rx.await 前存活
    let mem_ref = unsafe { &*(ptr as *const MemInner) };
    let mut flusher = flusher;
    let result_rx = flusher.flush(id, mem_ref.data.iter());
    let result = result_rx.recv();
    let _ = tx.send((result, flusher));
  });

  rx.await
}

/// Restore flusher to inner
/// 归还 flusher 到 inner
#[inline]
fn restore_flusher<F, N>(inner: &Rc<RefCell<Inner<F, N>>>, flusher: F) {
  inner.borrow_mut().flusher = Some(flusher);
}

/// Handle flush result, returns true if should continue
/// 处理刷盘结果，返回 true 表示应继续
fn handle<F: Flush, N: OnFlush>(
  inner: &Rc<RefCell<Inner<F, N>>>,
  id: u64,
  res: FlushResult<F>,
) -> bool {
  match res {
    Ok((Ok(Ok(meta)), flusher)) => {
      restore_flusher(inner, flusher);
      let mut inner_mut = inner.borrow_mut();
      inner_mut.frozen.remove(&id);
      inner_mut.notify.on_flush(meta);
      true
    }
    Ok((Ok(Err(e)), flusher)) => {
      error!("mem flush {id} failed: {e:?}");
      restore_flusher(inner, flusher);
      false
    }
    Ok((Err(e), flusher)) => {
      error!("mem flush {id} recv failed: {e:?}");
      restore_flusher(inner, flusher);
      false
    }
    Err(e) => {
      error!("mem flush {id} critical: channel dropped: {e:?}");
      false
    }
  }
}

/// Get next frozen to flush
/// 获取下一个待刷盘的 frozen
fn next<F, N>(inner: &Rc<RefCell<Inner<F, N>>>) -> Option<(u64, Mem)> {
  inner
    .borrow()
    .frozen
    .first_key_value()
    .map(|(&id, m)| (id, m.clone()))
}

/// Take flusher out temporarily
/// 临时取出 flusher
fn take<F, N>(inner: &Rc<RefCell<Inner<F, N>>>) -> F {
  inner.borrow_mut().flusher.take().expect("flusher missing")
}

/// Mark flush done and notify waiters
/// 标记刷盘完成并通知等待者
fn done<F, N>(inner: &Rc<RefCell<Inner<F, N>>>) {
  inner.borrow_mut().flushing = false;
  inner.borrow().event.notify(usize::MAX);
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

  while let Some((id, mem)) = next(inner) {
    let flusher = take(inner);
    if !handle(inner, id, one(flusher, id, mem).await) {
      break;
    }
  }

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
    while let Some((id, mem)) = next(&inner) {
      let flusher = take(&inner);
      if !handle(&inner, id, one(flusher, id, mem).await) {
        break;
      }
    }
    done(&inner);
  })
  .detach();
}
