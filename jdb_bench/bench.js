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

  // Create a new table
  const table = new Table({
    head: [
      "Engine",
      "Large ops/s",
      "Medium ops/s",
      "Small ops/s",
      "Disk",
      "Memory",
    ],
    colWidths: [12, 14, 15, 14, 12, 12],
    colAligns: ["left", "right", "right", "right", "right", "right"],
  });

  for (const file of files) {
    const filePath = join(reportDir, file);
    const data = JSON.parse(readFileSync(filePath, "utf8"));

    const engine = data.engine;
    const large = Math.floor(data.large.ops_per_sec);
    const medium = Math.floor(data.medium.ops_per_sec);
    const small = Math.floor(data.small.ops_per_sec);
    const disk = formatBytes(data.disk_bytes);
    const mem = formatBytes(data.mem_bytes);

    table.push([engine, large, medium, small, disk, mem]);
  }

  console.log(table.toString());

  console.log();
  console.log(`Reports saved in: ${reportDir}/`);
}

// 如果直接运行此脚本，生成报告
if (import.meta.main) {
  generateReport();
}
