//! Runtime dispatcher 运行时调度器

use crate::error::{Result, RuntimeError};
use crate::request::{Request, Response, VNodeAssign};
use crate::worker::Worker;
use jdb_comm::{TableID, VNodeID};
use std::path::PathBuf;
use tokio::sync::oneshot;

/// Runtime configuration 运行时配置
pub struct RuntimeConfig {
  pub workers: usize,
  pub bind_cores: bool,
  pub data_dir: PathBuf,
}

impl Default for RuntimeConfig {
  fn default() -> Self {
    Self {
      workers: 1,
      bind_cores: false,
      data_dir: PathBuf::from("/tmp/jdb"),
    }
  }
}

/// JDB Runtime JDB 运行时
pub struct Runtime {
  workers: Vec<Worker>,
  started: bool,
}

impl Runtime {
  pub fn new() -> Self {
    Self {
      workers: Vec::new(),
      started: false,
    }
  }

  pub fn start(&mut self, cfg: RuntimeConfig) -> Result<()> {
    if self.started {
      return Err(RuntimeError::AlreadyStarted);
    }

    std::fs::create_dir_all(&cfg.data_dir).ok();

    for i in 0..cfg.workers {
      let core_id = if cfg.bind_cores { Some(i) } else { None };
      let vnode = VNodeID::new(i as u16);
      let dir = cfg.data_dir.join(format!("vnode_{}", vnode.0));

      let worker = Worker::spawn(i, core_id, vec![VNodeAssign { vnode, dir }]);
      self.workers.push(worker);
    }

    self.started = true;
    Ok(())
  }

  fn worker(&self) -> Result<&Worker> {
    self.workers.first().ok_or(RuntimeError::NotStarted)
  }

  pub async fn put(&self, table: TableID, key: Vec<u8>, val: Vec<u8>) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    self.worker()?.send(Request::Put { table, key, val, tx })?;

    match rx.await {
      Ok(Response::Ok) => Ok(()),
      Ok(Response::Err(e)) => Err(RuntimeError::Tablet(e)),
      _ => Err(RuntimeError::RecvFailed),
    }
  }

  pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let (tx, rx) = oneshot::channel();
    self.worker()?.send(Request::Get { key: key.to_vec(), tx })?;

    match rx.await {
      Ok(Response::Value(v)) => Ok(v),
      _ => Err(RuntimeError::RecvFailed),
    }
  }

  pub async fn delete(&self, table: TableID, key: &[u8]) -> Result<bool> {
    let (tx, rx) = oneshot::channel();
    self.worker()?.send(Request::Delete {
      table,
      key: key.to_vec(),
      tx,
    })?;

    match rx.await {
      Ok(Response::Deleted(d)) => Ok(d),
      Ok(Response::Err(e)) => Err(RuntimeError::Tablet(e)),
      _ => Err(RuntimeError::RecvFailed),
    }
  }

  pub async fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let (tx, rx) = oneshot::channel();
    self.worker()?.send(Request::Range {
      start: start.to_vec(),
      end: end.to_vec(),
      tx,
    })?;

    match rx.await {
      Ok(Response::Range(r)) => Ok(r),
      _ => Err(RuntimeError::RecvFailed),
    }
  }

  pub async fn flush(&self) -> Result<()> {
    for worker in &self.workers {
      let (tx, rx) = oneshot::channel();
      worker.send(Request::Flush { tx })?;

      match rx.await {
        Ok(Response::Ok) => {}
        Ok(Response::Err(e)) => return Err(RuntimeError::Tablet(e)),
        _ => return Err(RuntimeError::RecvFailed),
      }
    }
    Ok(())
  }

  pub fn shutdown(&mut self) {
    for worker in &mut self.workers {
      worker.shutdown();
    }
    self.workers.clear();
    self.started = false;
  }
}

impl Default for Runtime {
  fn default() -> Self {
    Self::new()
  }
}

impl Drop for Runtime {
  fn drop(&mut self) {
    self.shutdown();
  }
}
