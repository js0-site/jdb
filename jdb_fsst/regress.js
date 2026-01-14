#!/usr/bin/env bun

import { readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";
import { execSync } from "child_process";

const ROOT = import.meta.dirname;
const REGRESS_JSON = join(ROOT, "benches/regress.json");
const REGRESS_HTML = join(ROOT, "benches/regress.html");
const MAX_HISTORY = 128;

const BENCHMARKS = [
  { name: "my_ratio", label: "我的压缩率", unit: "%", lowerBetter: true },
  { name: "my_throughput", label: "我的吞吐", unit: "MB/s", lowerBetter: false },
  { name: "ref_ratio", label: "参考压缩率", unit: "%", lowerBetter: true },
  { name: "ref_throughput", label: "参考吞吐", unit: "MB/s", lowerBetter: false },
  { name: "speedup", label: "加速倍数", unit: "x", lowerBetter: false },
  { name: "ratio_speedup", label: "压缩率提升", unit: "x", lowerBetter: false },
];

function formatValue(val, bench) {
  if (val === null || val === undefined) return "N/A";
  return `${val.toFixed(2)}${bench.unit}`;
}

function formatDiff(curr, prev, bench) {
  if (!prev || prev === 0 || !curr) return "";
  const diff = ((curr - prev) / prev) * 100;
  const sign = diff >= 0 ? "+" : "";

  // For ratio, lower is better. For throughput, higher is better.
  let isGood = bench.lowerBetter ? diff <= -1 : diff >= 1;
  let isBad = bench.lowerBetter ? diff >= 1 : diff <= -1;

  const color = isGood ? "32" : isBad ? "31" : "33";
  return `\x1b[${color}m${sign}${diff.toFixed(1)}%\x1b[0m`;
}

function genHtml(entry, history) {
  const latestMetrics = BENCHMARKS.map((b) => {
    const val = entry[b.name];
    const prev = history.length >= 2 ? history[history.length - 2][b.name] : null;
    const diff = prev && val ? (((val - prev) / prev) * 100).toFixed(1) : null;

    let diffClass = "";
    if (diff !== null) {
      const d = parseFloat(diff);
      const isGood = b.lowerBetter ? d <= -1 : d >= 1;
      const isBad = b.lowerBetter ? d >= 1 : d <= -1;
      diffClass = isGood ? "improved" : isBad ? "regressed" : "neutral";
    }

    return `
      <div class="metric">
        <div class="label">${b.label}</div>
        <div class="value">${formatValue(val, b)}</div>
        ${diff !== null ? `<div class="diff ${diffClass}">${parseFloat(diff) >= 0 ? "+" : ""}${diff}%</div>` : ""}
      </div>`;
  }).join("");

  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>jdb_fsst Performance Regression</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body { font-family: -apple-system, sans-serif; margin: 20px; background: #1a1a2e; color: #eee; }
    h1 { color: #00d4ff; margin-bottom: 5px; }
    .subtitle { color: #888; margin-bottom: 20px; }
    .latest { background: #16213e; padding: 15px; border-radius: 8px; margin: 20px 0; }
    .latest h3 { margin-top: 0; color: #00d4ff; }
    .metrics { display: flex; flex-wrap: wrap; gap: 15px; }
    .metric { background: #0f3460; padding: 12px 16px; border-radius: 6px; min-width: 140px; }
    .metric .label { font-size: 11px; color: #888; margin-bottom: 4px; }
    .metric .value { font-size: 18px; font-weight: bold; color: #fff; }
    .metric .diff { font-size: 12px; margin-top: 4px; }
    .metric .diff.improved { color: #0f0; }
    .metric .diff.regressed { color: #f66; }
    .metric .diff.neutral { color: #ff0; }
    .chart-container { width: 100%; max-width: 1200px; margin: 30px 0; }
  </style>
</head>
<body>
  <h1>jdb_fsst Performance Regression</h1>
  <div class="subtitle">Commit: ${entry.commit} (${entry.branch}) | ${entry.date}</div>
  <div class="latest">
    <h3>Latest Results</h3>
    <div class="metrics">${latestMetrics}</div>
  </div>
  <div class="chart-container"><canvas id="chart_throughput"></canvas></div>
  <div class="chart-container"><canvas id="chart_ratio"></canvas></div>
  <script>
    const data = ${JSON.stringify(history)};
    const labels = data.map(d => d.commit);
    
    new Chart(document.getElementById("chart_throughput"), {
      type: "line",
      data: {
        labels: labels,
        datasets: [
          { label: "My Throughput (MB/s)", data: data.map(d => d.my_throughput), borderColor: "#00d4ff", tension: 0.3 },
          { label: "Ref Throughput (MB/s)", data: data.map(d => d.ref_throughput), borderColor: "#ff6b6b", tension: 0.3 }
        ]
      },
      options: { plugins: { title: { display: true, text: "Throughput (Higher is better)", color: "#eee" } } }
    });

    new Chart(document.getElementById("chart_ratio"), {
      type: "line",
      data: {
        labels: labels,
        datasets: [
          { label: "My Ratio (%)", data: data.map(d => d.my_ratio), borderColor: "#0f0", tension: 0.3 },
          { label: "Ref Ratio (%)", data: data.map(d => d.ref_ratio), borderColor: "#ff0", tension: 0.3 }
        ]
      },
      options: { plugins: { title: { display: true, text: "Compression Ratio (Lower is better)", color: "#eee" } } }
    });
  </script>
</body>
</html>`;
}

// ============ Main ============
console.log("🚀 Running jdb_fsst benchmarks...\n");

const commit = execSync(
  "git rev-parse --short HEAD 2>/dev/null || echo unknown",
  { encoding: "utf8" },
).trim();
const branch = execSync(
  "git branch --show-current 2>/dev/null || echo unknown",
  { encoding: "utf8" },
).trim();
const date = new Date().toISOString().replace("T", " ").slice(0, 19);

const entry = { date, commit, branch };

try {
  const output = execSync("cargo bench --bench bench --features bench_all -- --json", {
    cwd: ROOT,
    encoding: "utf8",
    timeout: 300000,
  });

  // Find the JSON line in the output
  const lines = output.split("\n");
  const jsonLine = lines.find(l => l.trim().startsWith("{") && l.trim().endsWith("}"));
  if (!jsonLine) {
    throw new Error("Could not find JSON output from benchmark");
  }
  const results = JSON.parse(jsonLine);
  for (const b of BENCHMARKS) {
    entry[b.name] = results[b.name] ?? null;
  }
} catch (e) {
  console.error("⚠ Benchmarks failed:", e.message);
  process.exit(1);
}

// 加载历史 / load history
let history = existsSync(REGRESS_JSON)
  ? JSON.parse(readFileSync(REGRESS_JSON, "utf8"))
  : [];
const prev = history.length > 0 ? history[history.length - 1] : null;

// 保存 / save
history.push(entry);
if (history.length > MAX_HISTORY) history = history.slice(-MAX_HISTORY);
writeFileSync(REGRESS_JSON, JSON.stringify(history, null, 2));
writeFileSync(REGRESS_HTML, genHtml(entry, history));

// 输出结果 / print results
console.log("=".repeat(60));
console.log(`📊 jdb_fsst Performance Report`);
console.log(`   Commit: ${commit} (${branch})`);
console.log(`   Date:   ${date}`);
console.log("=".repeat(60));

console.log("\n📈 Aggregated Results (Current vs Previous):\n");

const colMetric = 20;
const colVal = 12;
const colChange = 12;

console.log(`${"Metric".padEnd(colMetric)}   ${"Value".padStart(colVal)}   ${"Change".padStart(colChange)}`);
console.log("-".repeat(colMetric + colVal + colChange + 6));

for (const b of BENCHMARKS) {
  const curr = entry[b.name];
  const prevVal = prev ? prev[b.name] : null;
  const valStr = formatValue(curr, b);
  const diffStr = formatDiff(curr, prevVal, b);
  console.log(`${b.label.padEnd(colMetric)}   ${valStr.padStart(colVal)}   ${diffStr.padStart(colChange)}`);
}

if (entry.my_ratio && entry.ref_ratio) {
  console.log("\n⚖️  Implementation Comparison (Mine vs Reference):\n");
  const ratioImprovement = (entry.ref_ratio - entry.my_ratio).toFixed(2);
  const throughputSpeedup = (entry.my_throughput / entry.ref_throughput).toFixed(2);

  const ratioColor = parseFloat(ratioImprovement) >= 0 ? "32" : "31";
  const speedupColor = parseFloat(throughputSpeedup) >= 1 ? "32" : "31";

  console.log(`- 压缩率领先: \x1b[${ratioColor}m${ratioImprovement}%\x1b[0m ( 参考: ${entry.ref_ratio.toFixed(2)}% / 我的: ${entry.my_ratio.toFixed(2)}%)`);
  console.log(`- 吞吐倍数:   \x1b[${speedupColor}m${throughputSpeedup}x\x1b[0m ( 参考: ${entry.ref_throughput.toFixed(2)} MB/s / 我的: ${entry.my_throughput.toFixed(2)} MB/s)`);
}

console.log(`\n📄 JSON: ${REGRESS_JSON}`);
console.log(`📄 HTML: ${REGRESS_HTML}`);
console.log("✅ Done.\n");
