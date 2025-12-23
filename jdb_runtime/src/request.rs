//! Request/Response types 请求/响应类型

use jdb_comm::{JdbError, TableID, VNodeID};
use std::path::PathBuf;
use tokio::sync::oneshot;

/// Request to worker 发送给 worker 的请求
pub enum Request {
  Put {
    table: TableID,
    key: Vec<u8>,
    val: Vec<u8>,
    tx: oneshot::Sender<Response>,
  },
  Get {
    key: Vec<u8>,
    tx: oneshot::Sender<Response>,
  },
  Delete {
    table: TableID,
    key: Vec<u8>,
    tx: oneshot::Sender<Response>,
  },
  Range {
    start: Vec<u8>,
    end: Vec<u8>,
    tx: oneshot::Sender<Response>,
  },
  Flush {
    tx: oneshot::Sender<Response>,
  },
  Shutdown,
}

/// Response from worker worker 的响应
pub enum Response {
  Ok,
  Value(Option<Vec<u8>>),
  Range(Vec<(Vec<u8>, Vec<u8>)>),
  Deleted(bool),
  Err(JdbError),
}

/// VNode assignment VNode 分配信息
pub struct VNodeAssign {
  pub vnode: VNodeID,
  pub dir: PathBuf,
}
