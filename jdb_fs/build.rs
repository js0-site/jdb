use std::{env, fs, path::Path};

use anyhow::{Context, Result};

fn get(key: &str, default: usize) -> Result<(String, String)> {
  println!("cargo:rerun-if-env-changed={}", key);
  match env::var(key) {
    Ok(val) => {
      val
        .parse::<usize>()
        .with_context(|| format!("{} must be a number, get: {}", key, val))?;
      Ok((key.to_string(), val))
    }
    Err(_) => Ok((key.to_string(), default.to_string())),
  }
}

fn save(filename: &str, configs: &[(String, String)]) -> Result<()> {
  let out_dir = env::var_os("OUT_DIR").context("OUT_DIR not found")?;
  let dest_path = Path::new(&out_dir).join(filename);

  let content = configs
    .iter()
    .map(|(k, v)| format!("pub const {}: usize = {};", k, v))
    .collect::<Vec<_>>()
    .join("\n");

  if let Ok(current) = fs::read_to_string(&dest_path)
    && current == content
  {
    return Ok(());
  }

  fs::write(&dest_path, content).with_context(|| format!("Failed to write {}", filename))?;
  Ok(())
}

macro_rules! bind {
    ( $( $module:ident : { $( $key:ident : $val:expr ),* $(,)? } ),* $(,)? ) => {
        $(
            let configs = vec![
                $(
                    get(stringify!($key), $val)?,
                )*
            ];
            save(concat!(stringify!($module), ".rs"), &configs)?;
        )*
    };
}

fn main() -> Result<()> {
  let mem_base: usize = get("JDB_MEM_BASE", 1024)?.1.parse()?;

  bind!(
      consts: {
          AUTO_COMPACT_OPS_INTERVAL: 65536,
          BUF_WRITER_SIZE: 512 * mem_base,
          BUF_READ_SIZE: 512 * mem_base,
      },
      file_consts: {
          MAX_BUF_SIZE: 4 * 1024 * mem_base,
          MAX_WRITE_SIZE: 128 * 1024 * mem_base,
      }
  );

  Ok(())
}
