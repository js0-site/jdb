#!/usr/bin/env bun

// Read bench.json and generate markdown tables
// 读取 bench.json 并生成 markdown 表格

import { readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { execSync } from "child_process";
import os from "os";
import Table from "cli-table3";

const ROOT = import.meta.dirname;
const JSON_PATH = join(ROOT, "bench.json");
const EN_MD = join(ROOT, "readme/en.bench.md");
const ZH_MD = join(ROOT, "readme/zh.bench.md");

const data = JSON.parse(readFileSync(JSON_PATH, "utf8"));

const sysInfo = getSystemInfo();

// Console output / 控制台输出
for (const cat of data.categories) {
  console.log(`\n=== ${cat.name} (${cat.capacity_mb}MB, ${cat.items} items) ===\n`);
  printConsoleTable(cat);
}

// Generate markdown / 生成 markdown
writeFileSync(EN_MD, genMdEn(data, sysInfo));
writeFileSync(ZH_MD, genMdZh(data, sysInfo));
console.log(`\nSaved: ${EN_MD}, ${ZH_MD}`);

function printConsoleTable(cat) {
  const table = new Table({
    head: ["Library", "Get (M/s)", "Set (M/s)", "Hit Rate", "Memory (KB)"],
    style: { head: ["cyan"] },
  });

  const base = cat.results[0];
  for (const r of cat.results) {
    const gr = (r.get_mops / base.get_mops).toFixed(2);
    const sr = (r.set_mops / base.set_mops).toFixed(2);
    table.push([
      r.lib,
      `${r.get_mops.toFixed(2)} (${gr})`,
      `${r.set_mops.toFixed(2)} (${sr})`,
      `${r.hit_rate.toFixed(1)}%`,
      r.memory_kb.toFixed(1),
    ]);
  }
  console.log(table.toString());
}

function getSystemInfo() {
  const cpus = os.cpus();
  const cpu = cpus[0]?.model || "Unknown";
  const cores = cpus.length;
  const mem = (os.totalmem() / 1024 / 1024 / 1024).toFixed(1);
  const platform = os.platform();
  const arch = os.arch();
  const release = os.release();

  let rustVer = "Unknown";
  try {
    rustVer = execSync("rustc --version", { encoding: "utf8" }).trim();
  } catch {}

  let osName = `${platform} ${release}`;
  if (platform === "darwin") {
    try {
      const ver = execSync("sw_vers -productVersion", { encoding: "utf8" }).trim();
      osName = `macOS ${ver}`;
    } catch {}
  }

  return { cpu, cores, mem, osName, arch, rustVer };
}

function genMdEn(data, sys) {
  const lines = [];
  lines.push("## Benchmark Results\n");
  lines.push("### Test Environment\n");
  lines.push(`| Item | Value |`);
  lines.push(`|------|-------|`);
  lines.push(`| OS | ${sys.osName} (${sys.arch}) |`);
  lines.push(`| CPU | ${sys.cpu} |`);
  lines.push(`| Cores | ${sys.cores} |`);
  lines.push(`| Memory | ${sys.mem} GB |`);
  lines.push(`| Rust | ${sys.rustVer} |`);
  lines.push("");

  for (const cat of data.categories) {
    lines.push(`### ${cat.name} (${cat.capacity_mb}MB cache, ${cat.items} items)\n`);
    lines.push("| Library | Get (M/s) | Set (M/s) | Hit Rate | Memory (KB) |");
    lines.push("|---------|-----------|-----------|----------|-------------|");

    const base = cat.results[0];
    for (const r of cat.results) {
      const gr = (r.get_mops / base.get_mops).toFixed(2);
      const sr = (r.set_mops / base.set_mops).toFixed(2);
      lines.push(
        `| ${r.lib} | ${r.get_mops.toFixed(2)} (${gr}) | ${r.set_mops.toFixed(2)} (${sr}) | ${r.hit_rate.toFixed(1)}% | ${r.memory_kb.toFixed(1)} |`
      );
    }
    lines.push("\n*Ratio in parentheses: relative to size_lru (1.00 = baseline)*\n");
  }

  return lines.join("\n");
}

function genMdZh(data, sys) {
  const lines = [];
  lines.push("## 性能测试结果\n");
  lines.push("### 测试环境\n");
  lines.push(`| 项目 | 值 |`);
  lines.push(`|------|-------|`);
  lines.push(`| 操作系统 | ${sys.osName} (${sys.arch}) |`);
  lines.push(`| CPU | ${sys.cpu} |`);
  lines.push(`| 核心数 | ${sys.cores} |`);
  lines.push(`| 内存 | ${sys.mem} GB |`);
  lines.push(`| Rust | ${sys.rustVer} |`);
  lines.push("");

  for (const cat of data.categories) {
    lines.push(`### ${cat.name} (${cat.capacity_mb}MB 缓存, ${cat.items} 条数据)\n`);
    lines.push("| 库 | 读取 (百万/秒) | 写入 (百万/秒) | 命中率 | 内存 (KB) |");
    lines.push("|---------|-----------|-----------|----------|-------------|");

    const base = cat.results[0];
    for (const r of cat.results) {
      const gr = (r.get_mops / base.get_mops).toFixed(2);
      const sr = (r.set_mops / base.set_mops).toFixed(2);
      lines.push(
        `| ${r.lib} | ${r.get_mops.toFixed(2)} (${gr}) | ${r.set_mops.toFixed(2)} (${sr}) | ${r.hit_rate.toFixed(1)}% | ${r.memory_kb.toFixed(1)} |`
      );
    }
    lines.push("\n*括号内为相对性能：以 size_lru 为基准（1.00 = 基准值）*\n");
  }

  return lines.join("\n");
}
