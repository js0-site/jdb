use crate::Map;
use jdb_base::{sst::Flush, Discard};

use std::rc::Rc;

pub struct Disk<F, D> {
  pub flush: F,
  pub discard: D,
}

impl<F: Flush, D: Discard> Disk<F, D> {
  pub const fn new(flush: F, discard: D) -> Self {
    Self { flush, discard }
  }

  pub async fn run(mut self, id: u64, map: Rc<Map>) -> Self {
    let _ = futures::join!(self.flush.flush(id, map.iter()), self.discard.flush());
    self
  }
}

use oneshot::{channel, Receiver, TryRecvError};

pub enum State<F, D> {
  Idle(Disk<F, D>),
  Running(Receiver<Disk<F, D>>),
}

impl<F, D> State<F, D>
where
  F: Flush,
  D: Discard + 'static,
{
  pub fn flush(&mut self, old: &mut Vec<Rc<Map>>) {
    loop {
      match self {
        State::Running(rx) => match rx.try_recv() {
          Ok(disk) => {
            *self = State::Idle(disk);
            if !old.is_empty() {
              old.remove(0);
            }
          }
          Err(TryRecvError::Empty) => return,
          Err(TryRecvError::Disconnected) => {
            panic!("Flush task disconnected unexpectedly");
          }
        },
        State::Idle(_) => {
          if old.is_empty() {
            return;
          }

          let disk = if let State::Idle(disk) =
            std::mem::replace(self, State::Running(channel().1))
          {
            disk
          } else {
            unreachable!("Already matched Idle");
          };

          let map = old[0].clone();
          let (tx, rx) = channel();
          let id = ider::id();

          compio::runtime::spawn(async move {
            let disk = disk.run(id, map).await;
            let _ = tx.send(disk);
          })
          .detach();

          *self = State::Running(rx);
        }
      }
    }
  }
}
