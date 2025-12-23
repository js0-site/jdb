//! Worker thread implementation Worker 线程实现

use crate::request::{Request, Response, VNodeAssign};
use compio::runtime::Runtime as CompioRuntime;
use core_affinity::{get_core_ids, set_for_current};
use jdb_comm::JdbError;
use jdb_tablet::Tablet;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::thread::JoinHandle;

/// Worker handle Worker 句柄
pub struct Worker {
  pub id: usize,
  pub tx: Sender<Request>,
  handle: Option<JoinHandle<()>>,
}

impl Worker {
  /// Spawn worker thread 启动 worker 线程
  pub fn spawn(id: usize, core_id: Option<usize>, assigns: Vec<VNodeAssign>) -> Self {
    let (tx, rx) = std::sync::mpsc::channel::<Request>();

    let handle = std::thread::spawn(move || {
      // Bind to core 绑定 CPU 核心
      if let Some(cid) = core_id {
        if let Some(cores) = get_core_ids() {
          if let Some(core) = cores.get(cid) {
            set_for_current(*core);
          }
        }
      }

      if let Ok(rt) = CompioRuntime::new() {
        rt.block_on(worker_loop(id, rx, assigns));
      }
    });

    Self {
      id,
      tx,
      handle: Some(handle),
    }
  }

  pub fn send(&self, req: Request) -> crate::Result<()> {
    self.tx.send(req).map_err(|_| crate::RuntimeError::SendFailed)
  }

  pub fn shutdown(&mut self) {
    let _ = self.tx.send(Request::Shutdown);
    if let Some(h) = self.handle.take() {
      let _ = h.join();
    }
  }
}

/// Worker main loop Worker 主循环
async fn worker_loop(id: usize, rx: Receiver<Request>, assigns: Vec<VNodeAssign>) {
  let mut tablets: HashMap<u16, RefCell<Tablet>> = HashMap::new();

  for assign in assigns {
    match Tablet::create(&assign.dir, assign.vnode).await {
      Ok(t) => {
        tablets.insert(assign.vnode.0, RefCell::new(t));
      }
      Err(e) => {
        eprintln!("worker {id}: tablet {} failed: {e}", assign.vnode.0);
      }
    }
  }

  let no_tablet = || JdbError::NoTablet;

  loop {
    let req = match rx.recv() {
      Ok(r) => r,
      Err(_) => break,
    };

    match req {
      Request::Put { table, key, val, tx } => {
        let resp = match tablets.values().next() {
          Some(cell) => match cell.borrow_mut().put(table, key, val).await {
            Ok(()) => Response::Ok,
            Err(e) => Response::Err(e),
          },
          None => Response::Err(no_tablet()),
        };
        let _ = tx.send(resp);
      }

      Request::Get { key, tx } => {
        let val = tablets.values().next().map(|c| c.borrow().get(&key)).flatten();
        let _ = tx.send(Response::Value(val));
      }

      Request::Delete { table, key, tx } => {
        let resp = match tablets.values().next() {
          Some(cell) => match cell.borrow_mut().delete(table, &key).await {
            Ok(d) => Response::Deleted(d),
            Err(e) => Response::Err(e),
          },
          None => Response::Err(no_tablet()),
        };
        let _ = tx.send(resp);
      }

      Request::Range { start, end, tx } => {
        let result = tablets
          .values()
          .next()
          .map(|c| c.borrow().range(&start, &end))
          .unwrap_or_default();
        let _ = tx.send(Response::Range(result));
      }

      Request::Flush { tx } => {
        let mut err = None;
        for cell in tablets.values() {
          if let Err(e) = cell.borrow_mut().flush().await {
            err = Some(e);
            break;
          }
        }
        let _ = tx.send(match err {
          Some(e) => Response::Err(e),
          None => Response::Ok,
        });
      }

      Request::Shutdown => break,
    }
  }
}
