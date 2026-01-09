#!/usr/bin/env bun

import { readFileSync, readdirSync, existsSync } from "fs";
import { join } from "path";
import Table from "cli-table3";

const ROOT = import.meta.dirname;
const BASE_ENGINE = "wlog";

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
      formatOps(large.write?.ops || 0),
      formatRatio(large.write?.ops, baseLarge.write?.ops),
      formatOps(medium.write?.ops || 0),
      formatRatio(medium.write?.ops, baseMedium.write?.ops),
      formatOps(small.write?.ops || 0),
      formatRatio(small.write?.ops, baseSmall.write?.ops),
    ]);

    // Speed - Read (ops/s) / 速度 - 读取
    readSpeedTable.push([
      engine,
      formatOps(large.read?.ops || 0),
      formatRatio(large.read?.ops, baseLarge.read?.ops),
      formatOps(medium.read?.ops || 0),
      formatRatio(medium.read?.ops, baseMedium.read?.ops),
      formatOps(small.read?.ops || 0),
      formatRatio(small.read?.ops, baseSmall.read?.ops),
    ]);

    // Throughput - Write (MB/s) / 吞吐量 - 写入
    writeThroughputTable.push([
      engine,
      formatSpeed(large.write?.mbs || 0),
      formatRatio(large.write?.mbs, baseLarge.write?.mbs),
      formatSpeed(medium.write?.mbs || 0),
      formatRatio(medium.write?.mbs, baseMedium.write?.mbs),
      formatSpeed(small.write?.mbs || 0),
      formatRatio(small.write?.mbs, baseSmall.write?.mbs),
    ]);

    // Throughput - Read (MB/s) / 吞吐量 - 读取
    readThroughputTable.push([
      engine,
      formatSpeed(large.read?.mbs || 0),
      formatRatio(large.read?.mbs, baseLarge.read?.mbs),
      formatSpeed(medium.read?.mbs || 0),
      formatRatio(medium.read?.mbs, baseMedium.read?.mbs),
      formatSpeed(small.read?.mbs || 0),
      formatRatio(small.read?.mbs, baseSmall.read?.mbs),
    ]);

    // Disk - Write (size + amplification) / 磁盘 - 写入（大小 + 写放大）
    // Lower is better / 越低越好
    writeDiskTable.push([
      engine,
      formatMB(large.disk_mb || 0),
      formatRatio(baseLarge.disk_mb, large.disk_mb),
      formatMB(medium.disk_mb || 0),
      formatRatio(baseMedium.disk_mb, medium.disk_mb),
      formatMB(small.disk_mb || 0),
      formatRatio(baseSmall.disk_mb, small.disk_mb),
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

  console.log("=== Disk / 磁盘 ===");
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
