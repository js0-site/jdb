#!/usr/bin/env bun

import { readFileSync, readdirSync, existsSync } from "fs";
import { join } from "path";
import Table from "cli-table3";

const ROOT = import.meta.dirname;
const BASE_ENGINE = "jdb_val";

function formatOps(ops) {
  if (ops >= 1000000) return (ops / 1000000).toFixed(2) + "M";
  if (ops >= 1000) return (ops / 1000).toFixed(1) + "K";
  return Math.floor(ops).toString();
}

function formatMB(mb) {
  if (mb >= 1000) return (mb / 1000).toFixed(1) + "GB";
  return mb.toFixed(0) + "MB";
}

function formatSpeed(mb) {
  if (mb >= 1000) return (mb / 1000).toFixed(1) + "G/s";
  if (mb >= 1) return mb.toFixed(0) + "M/s";
  return (mb * 1000).toFixed(0) + "K/s";
}

function formatRatio(val, base) {
  if (!val || val === 0 || !base || base === 0) return "-";
  const pct = (val / base) * 100;
  return pct.toFixed(0) + "%";
}

function generateReport() {
  const reportDir = join(ROOT, "report");

  console.log();
  console.log("==========================================");
  console.log("           BENCHMARK SUMMARY");
  console.log("==========================================");
  console.log();

  if (!existsSync(reportDir)) {
    console.log("No benchmark reports found.");
    return;
  }

  const files = readdirSync(reportDir).filter((f) => f.endsWith(".json"));
  if (files.length === 0) {
    console.log("No benchmark reports found.");
    return;
  }

  // Load all data / 加载所有数据
  const engines = {};
  for (const file of files) {
    const filePath = join(reportDir, file);
    const data = JSON.parse(readFileSync(filePath, "utf8"));
    const cats = {};
    for (const [name, result] of data.categories) {
      cats[name] = result;
    }
    engines[data.engine] = cats;
  }

  const base = engines[BASE_ENGINE];
  if (!base) {
    console.log(`Base engine '${BASE_ENGINE}' not found.`);
    return;
  }

  // === Speed Tables / 速度表 ===
  const writeSpeedTable = new Table({
    head: ["Engine", "Large", "vs jdb", "Medium", "vs jdb", "Small", "vs jdb"],
    colWidths: [10, 10, 10, 10, 10, 10, 10],
    colAligns: ["left", "right", "right", "right", "right", "right", "right"],
  });

  const readSpeedTable = new Table({
    head: ["Engine", "Large", "vs jdb", "Medium", "vs jdb", "Small", "vs jdb"],
    colWidths: [10, 10, 10, 10, 10, 10, 10],
    colAligns: ["left", "right", "right", "right", "right", "right", "right"],
  });

  // === Throughput Tables / 吞吐量表 ===
  const writeThroughputTable = new Table({
    head: ["Engine", "Large", "vs jdb", "Medium", "vs jdb", "Small", "vs jdb"],
    colWidths: [10, 10, 10, 10, 10, 10, 10],
    colAligns: ["left", "right", "right", "right", "right", "right", "right"],
  });

  const readThroughputTable = new Table({
    head: ["Engine", "Large", "vs jdb", "Medium", "vs jdb", "Small", "vs jdb"],
    colWidths: [10, 10, 10, 10, 10, 10, 10],
    colAligns: ["left", "right", "right", "right", "right", "right", "right"],
  });

  // === Disk Tables / 磁盘表 ===
  const writeDiskTable = new Table({
    head: ["Engine", "Large", "vs jdb", "Medium", "vs jdb", "Small", "vs jdb"],
    colWidths: [10, 10, 10, 10, 10, 10, 10],
    colAligns: ["left", "right", "right", "right", "right", "right", "right"],
  });

  // === Memory Table / 内存表 ===
  const memTable = new Table({
    head: ["Engine", "Large", "vs jdb", "Medium", "vs jdb", "Small", "vs jdb"],
    colWidths: [10, 10, 10, 10, 10, 10, 10],
    colAligns: ["left", "right", "right", "right", "right", "right", "right"],
  });

  for (const [engine, cats] of Object.entries(engines)) {
    const large = cats["Large"] || {};
    const medium = cats["Medium"] || {};
    const small = cats["Small"] || {};

    const baseLarge = base["Large"] || {};
    const baseMedium = base["Medium"] || {};
    const baseSmall = base["Small"] || {};

    // Speed - Write (ops/s) / 速度 - 写入
    writeSpeedTable.push([
      engine,
      formatOps(large.write?.ops_per_sec || 0),
      formatRatio(large.write?.ops_per_sec, baseLarge.write?.ops_per_sec),
      formatOps(medium.write?.ops_per_sec || 0),
      formatRatio(medium.write?.ops_per_sec, baseMedium.write?.ops_per_sec),
      formatOps(small.write?.ops_per_sec || 0),
      formatRatio(small.write?.ops_per_sec, baseSmall.write?.ops_per_sec),
    ]);

    // Speed - Read (ops/s) / 速度 - 读取
    readSpeedTable.push([
      engine,
      formatOps(large.read?.ops_per_sec || 0),
      formatRatio(large.read?.ops_per_sec, baseLarge.read?.ops_per_sec),
      formatOps(medium.read?.ops_per_sec || 0),
      formatRatio(medium.read?.ops_per_sec, baseMedium.read?.ops_per_sec),
      formatOps(small.read?.ops_per_sec || 0),
      formatRatio(small.read?.ops_per_sec, baseSmall.read?.ops_per_sec),
    ]);

    // Throughput - Write (MB/s) / 吞吐量 - 写入
    writeThroughputTable.push([
      engine,
      formatSpeed(large.write?.mb_per_sec || 0),
      formatRatio(large.write?.mb_per_sec, baseLarge.write?.mb_per_sec),
      formatSpeed(medium.write?.mb_per_sec || 0),
      formatRatio(medium.write?.mb_per_sec, baseMedium.write?.mb_per_sec),
      formatSpeed(small.write?.mb_per_sec || 0),
      formatRatio(small.write?.mb_per_sec, baseSmall.write?.mb_per_sec),
    ]);

    // Throughput - Read (MB/s) / 吞吐量 - 读取
    readThroughputTable.push([
      engine,
      formatSpeed(large.read?.mb_per_sec || 0),
      formatRatio(large.read?.mb_per_sec, baseLarge.read?.mb_per_sec),
      formatSpeed(medium.read?.mb_per_sec || 0),
      formatRatio(medium.read?.mb_per_sec, baseMedium.read?.mb_per_sec),
      formatSpeed(small.read?.mb_per_sec || 0),
      formatRatio(small.read?.mb_per_sec, baseSmall.read?.mb_per_sec),
    ]);

    // Disk - Write (amplification) / 磁盘 - 写入（写放大）
    // Lower is better, so ratio is inverted / 越低越好，所以比率反转
    writeDiskTable.push([
      engine,
      (large.write_amp || 0).toFixed(2) + "x",
      formatRatio(baseLarge.write_amp, large.write_amp),
      (medium.write_amp || 0).toFixed(2) + "x",
      formatRatio(baseMedium.write_amp, medium.write_amp),
      (small.write_amp || 0).toFixed(2) + "x",
      formatRatio(baseSmall.write_amp, small.write_amp),
    ]);

    // Memory / 内存
    // Lower is better / 越低越好
    memTable.push([
      engine,
      formatMB(large.mem_mb || 0),
      formatRatio(baseLarge.mem_mb, large.mem_mb),
      formatMB(medium.mem_mb || 0),
      formatRatio(baseMedium.mem_mb, medium.mem_mb),
      formatMB(small.mem_mb || 0),
      formatRatio(baseSmall.mem_mb, small.mem_mb),
    ]);
  }

  console.log("=== Speed (ops/s) / 速度 ===");
  console.log();
  console.log("Write:");
  console.log(writeSpeedTable.toString());
  console.log();
  console.log("Read:");
  console.log(readSpeedTable.toString());
  console.log();

  console.log("=== Throughput (MB/s) / 吞吐量 ===");
  console.log();
  console.log("Write:");
  console.log(writeThroughputTable.toString());
  console.log();
  console.log("Read:");
  console.log(readThroughputTable.toString());
  console.log();

  console.log("=== Disk (write amplification) / 磁盘（写放大） ===");
  console.log();
  console.log(writeDiskTable.toString());
  console.log();

  console.log("=== Memory / 内存 ===");
  console.log();
  console.log(memTable.toString());

  console.log();
  console.log(`Reports saved in: ${reportDir}/`);
}

if (import.meta.main) {
  generateReport();
}
