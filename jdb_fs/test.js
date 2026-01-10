#!/usr/bin/env zx

import { $, fs, which } from "zx";
import { join } from "path";

const LINUX = "x86_64-unknown-linux-gnu",
  WIN = "x86_64-pc-windows-msvc",
  MAC = "x86_64-apple-darwin",
  TARGETS = [LINUX, WIN, MAC],
  ARGS = ["test", "--all-features", "--release", "--", "--nocapture"];

// Get package name, target dir and current host from cargo metadata
// 从 cargo metadata 获取包名、目标目录和当前主机
const META = await (async () => {
  const meta = JSON.parse(
    (await $`cargo metadata --format-version 1 --no-deps`).stdout,
  );
  const pkg = meta.packages[0].name;
  const targetDir = meta.target_directory;
  const host = (await $`rustc -vV`).stdout.match(/host: (.+)/)[1];
  return { pkg, targetDir, host };
})();

$.verbose = 1;

const ensureCmd = async (cmd, install) => {
  if (!(await which(cmd, { nothrow: true }))) await install();
};

const ensureBinstall = () =>
  ensureCmd("cargo-binstall", () => $`cargo install cargo-binstall`);

const ensureZigbuild = async () => {
  await ensureBinstall();
  await ensureCmd("cargo-zigbuild", () => $`cargo binstall cargo-zigbuild -y`);
};

const ensureXwin = async () => {
  await ensureBinstall();
  await ensureCmd("cargo-xwin", () => $`cargo binstall cargo-xwin -y`);
};

// Ensure stable toolchain and targets are installed (exclude WIN, xwin handles it)
// 确保 stable 工具链和目标已安装（排除 WIN，xwin 自动处理）
const ensureStable = async (targets) => {
  const zigTargets = targets.filter((t) => t !== WIN);
  if (zigTargets.length === 0) return;
  const installed = (await $`rustup target list --toolchain stable --installed`)
    .stdout;
  for (const t of zigTargets) {
    if (!installed.includes(t)) {
      await $`rustup target add ${t} --toolchain stable`;
    }
  }
};

// Find test binaries in deps dir
// 在 deps 目录中查找测试二进制
const findTestBins = async (pkg, targetDir, target, ext = "") => {
  const dir = join(targetDir, target, "release", "deps");
  const files = await fs.readdir(dir);
  return files
    .filter((f) => f.startsWith(pkg) && !f.endsWith(".d") && f.endsWith(ext))
    .map((f) => join(dir, f));
};

// Run tests in docker for Linux target
// 用 docker 运行 Linux 目标的测试
const dockerTest = async (pkg, targetDir, target) => {
  const bins = await findTestBins(pkg, targetDir, target);
  for (const bin of bins) {
    await $`docker run --rm -v $(pwd):/app -w /app debian:bookworm ${bin} --nocapture`;
  }
};

// Run tests with wine for Windows target
// 用 wine 运行 Windows 目标的测试
const wineTest = async (pkg, targetDir, target) => {
  const bins = await findTestBins(pkg, targetDir, target, ".exe");
  for (const bin of bins) {
    await $`wine64 ${bin} --nocapture`;
  }
};

const main = async () => {
  const { pkg, targetDir, host } = META;
  const others = TARGETS.filter((t) => t !== host);

  await $`cargo ${ARGS}`;
  console.log(`✓ ${host} passed (nightly)`);

  if (others.length === 0) return;

  await ensureZigbuild();
  await ensureXwin();
  await ensureStable(others);

  // Build and test other platforms
  // 构建和测试其他平台
  for (const t of others) {
    if (t === WIN) {
      // Windows msvc use xwin
      // Windows msvc 用 xwin
      await $`rustup run stable cargo xwin build --tests --all-features --release --target ${t}`;
    } else {
      // Linux/Mac use zigbuild
      // Linux/Mac 用 zigbuild
      await $`rustup run stable cargo zigbuild --tests --all-features --release --target ${t}`;
    }
    console.log(`✓ ${t} built (stable)`);

    if (t === LINUX) {
      await dockerTest(pkg, targetDir, t);
      console.log(`✓ ${t} tested (docker)`);
    } else if (t === WIN) {
      await wineTest(pkg, targetDir, t);
      console.log(`✓ ${t} tested (wine)`);
    } else if (t === MAC && host.includes("apple-darwin")) {
      // x86_64 mac can run on arm64 mac via Rosetta
      // x86_64 mac 可以通过 Rosetta 在 arm64 mac 上运行
      const bins = await findTestBins(pkg, targetDir, t);
      for (const bin of bins) {
        await $`${bin} --nocapture`;
      }
      console.log(`✓ ${t} tested (rosetta)`);
    }
  }
};

main();
