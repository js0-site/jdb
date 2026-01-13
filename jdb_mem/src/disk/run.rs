use std::rc::Rc;

use futures::channel::oneshot::{Receiver, channel};
use jdb_base::{Discard, sst::Sst};

use super::{Disk, FlushRes};
use crate::{Map, log_err};

/// Start background flush operations
/// 开始后台刷盘操作
pub fn run<S, D>(mut disk: Disk<S, D>, map: Rc<Map>) -> Receiver<FlushRes<S, D>>
where
  S: Sst,
  D: Discard,
{
  let (tx, rx) = channel();
  compio::runtime::spawn(async move {
    let res = {
      let Disk { sst, discard } = &mut disk;

      // Process discarded entries before flushing
      // 在刷盘前处理丢弃的条目
      for (k, p) in &map.discards {
        discard.discard(k, p);
      }
      futures::join!(sst.write(map.iter()), discard.flush())
    };

    let (sst_res, discard_res) = res;

    if let Err(err) = discard_res {
      log_err("discard flush error", err);
    }

    // sst.push is done by caller synchronously with old.remove to ensure atomicity
    // sst.push 由调用方与 old.remove 同步执行以确保原子性

    let res = tx.send((disk, sst_res));
    if let Err((_, Err(err))) = res {
      log_err("mem send sst write error (receiver dropped)", err);
    }
  })
  .detach();
  rx
}
