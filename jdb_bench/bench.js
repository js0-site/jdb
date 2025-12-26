#!/usr/bin/env bun

import { readFileSync, readdirSync, existsSync } from "fs";
import { join } from "path";
import Table from "cli-table3";

const ROOT = import.meta.dirname;

function formatBytes(bytes) {
  if (bytes === 0) return "0B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + sizes[i];
}

function formatOps(ops) {
  if (ops >= 1000000) return (ops / 1000000).toFixed(2) + "M";
  if (ops >= 1000) return (ops / 1000).toFixed(1) + "K";
  return Math.floor(ops).toString();
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

  // Create table for write ops
  const writeTable = new Table({
    head: ["Engine", "Large W", "Medium W", "Small W"],
    colWidths: [12, 12, 12, 12],
    colAligns: ["left", "right", "right", "right"],
  });

  // Create table for read ops
  const readTable = new Table({
    head: ["Engine", "Large R", "Medium R", "Small R"],
    colWidths: [12, 12, 12, 12],
    colAligns: ["left", "right", "right", "right"],
  });

  // Create table for disk/mem
  const diskTable = new Table({
    head: ["Engine", "Large B/rec", "Medium B/rec", "Small B/rec", "Mem"],
    colWidths: [12, 14, 14, 14, 10],
    colAligns: ["left", "right", "right", "right", "right"],
  });

  for (const file of files) {
    const filePath = join(reportDir, file);
    const data = JSON.parse(readFileSync(filePath, "utf8"));

    const engine = data.engine;
    const cats = {};
    for (const [name, result] of data.categories) {
      cats[name] = result;
    }

    const large = cats["Large"] || {};
    const medium = cats["Medium"] || {};
    const small = cats["Small"] || {};

    writeTable.push([
      engine,
      formatOps(large.write?.ops_per_sec || 0),
      formatOps(medium.write?.ops_per_sec || 0),
      formatOps(small.write?.ops_per_sec || 0),
    ]);

    readTable.push([
      engine,
      formatOps(large.read?.ops_per_sec || 0),
      formatOps(medium.read?.ops_per_sec || 0),
      formatOps(small.read?.ops_per_sec || 0),
    ]);

    diskTable.push([
      engine,
      formatBytes(large.bytes_per_rec || 0),
      formatBytes(medium.bytes_per_rec || 0),
      formatBytes(small.bytes_per_rec || 0),
      (small.mem_mb || 0).toFixed(0) + "MB",
    ]);
  }

  console.log("Write Performance (ops/s):");
  console.log(writeTable.toString());
  console.log();

  console.log("Read Performance (ops/s):");
  console.log(readTable.toString());
  console.log();

  console.log("Disk Usage (bytes/record):");
  console.log(diskTable.toString());

  console.log();
  console.log(`Reports saved in: ${reportDir}/`);
}

// 如果直接运行此脚本，生成报告
if (import.meta.main) {
  generateReport();
}
