#![cfg_attr(docsrs, feature(doc_cfg))]

pub trait Conf {
  const BUF_WRITER_SIZE: usize = 512 * 1024;
}

pub struct CONF;
impl Conf for CONF {}
