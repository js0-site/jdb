#!/usr/bin/env bun

import { readFileSync, writeFileSync, mkdirSync } from "fs";
import { optimize } from "svgo";

const data = JSON.parse(readFileSync("benches/regress.json", "utf-8"));

const LIB_COLORS = {
  jdb_fsst: "#f97316",
  fsst: "#3b82f6",
};
const DEFAULT_COLOR = "#8b5cf6";
const MAIN_TITLE_SIZE = 32;

const getColor = (lib) => LIB_COLORS[lib] || DEFAULT_COLOR;

const lighten = (hex, amount = 0.2) => {
  const num = parseInt(hex.slice(1), 16);
  const r = Math.min(
    255,
    ((num >> 16) & 0xff) + (255 - ((num >> 16) & 0xff)) * amount,
  );
  const g = Math.min(
    255,
    ((num >> 8) & 0xff) + (255 - ((num >> 8) & 0xff)) * amount,
  );
  const b = Math.min(255, (num & 0xff) + (255 - (num & 0xff)) * amount);
  return `rgb(${Math.round(r)},${Math.round(g)},${Math.round(b)})`;
};

const darken = (hex, amount = 0.25) => {
  const num = parseInt(hex.slice(1), 16);
  const r = Math.max(0, ((num >> 16) & 0xff) * (1 - amount));
  const g = Math.max(0, ((num >> 8) & 0xff) * (1 - amount));
  const b = Math.max(0, (num & 0xff) * (1 - amount));
  return `rgb(${Math.round(r)},${Math.round(g)},${Math.round(b)})`;
};

const bar3d = (x, y, w, h, color, depth = 10) => {
  const light = lighten(color, 0.3);
  const dark = darken(color, 0.3);

  return `<rect x="${x}" y="${y}" width="${w}" height="${h}" fill="${color}"/><rect x="${x}" y="${y}" width="${w * 0.15}" height="${h}" fill="${light}" opacity="0.4"/><polygon points="${x},${y} ${x + depth},${y - depth} ${x + w + depth},${y - depth} ${x + w},${y}" fill="${light}"/><polygon points="${x + w},${y} ${x + w + depth},${y - depth} ${x + w + depth},${y + h - depth} ${x + w},${y + h}" fill="${dark}"/>`;
};

const STAR_RIGHT_SPACING = 3;
const STAR_VERTICAL_OFFSET = -3;

const createValueText = (x, y, text, isBest = false) => {
  const textWidth = text.length * 7.8;
  const starOffset = isBest ? -textWidth / 2 - STAR_RIGHT_SPACING - 6 : 0;
  return isBest
    ? `<polygon points="${x + starOffset},${y - 11} ${x + starOffset + 2},${y - 7} ${x + starOffset + 6},${y - 7} ${x + starOffset + 3},${y - 4} ${x + starOffset + 4},${y} ${x + starOffset},${y - 2} ${x + starOffset - 4},${y} ${x + starOffset - 3},${y - 4} ${x + starOffset - 6},${y - 7} ${x + starOffset - 2},${y - 7}" fill="none" stroke="#ef4444" stroke-width="1.2"/><text x="${x}" y="${y}" text-anchor="middle" font-size="13" font-weight="700" fill="#000000">${text}</text>`
    : `<text x="${x}" y="${y}" text-anchor="middle" font-size="13" font-weight="400" fill="#374151">${text}</text>`;
};

const createTitleText = (
  x,
  y,
  text,
  size = 16,
  weight = 600,
  color = "#374151",
) =>
  `<text x="${x}" y="${y}" text-anchor="middle" font-size="${size}" font-weight="${weight}" fill="${color}">${text}</text>`;

const barChart = (
  results,
  m,
  yOffset,
  getValue,
  formatLabel,
  lowerIsBetter = false,
) => {
  const cH = m.chartH - m.t - m.b;
  const depth = 10;
  let maxVal = 0;
  for (const r of results) {
    maxVal = Math.max(maxVal, getValue(r));
  }

  const bW = Math.min(65, (m.cW - 40) / results.length - 25);
  const gap = (m.cW - results.length * bW) / (results.length + 1);

  let svg = "";

  for (let i = 0; i <= 4; i++) {
    const y = yOffset + m.t + cH - (cH * i) / 4;
    const label = formatLabel((maxVal * i) / 4);
    svg += `<line x1="${m.l}" y1="${y}" x2="${m.l + m.cW}" y2="${y}" stroke="#e5e7eb" stroke-dasharray="${i === 0 ? "" : "4,4"}"/><text x="${m.l - 12}" y="${y + 5}" text-anchor="end" font-size="12" fill="#9ca3af">${label}</text>`;
  }

  const bestVal = lowerIsBetter
    ? Math.min(...results.map(getValue))
    : Math.max(...results.map(getValue));

  results.forEach((r, i) => {
    const x = m.l + gap + i * (bW + gap);
    const v = getValue(r);
    const h = (v / maxVal) * cH;
    const y = yOffset + m.t + cH - h;
    const isBest = v === bestVal;
    const color = getColor(r.lib);
    const labelY = yOffset + m.chartH - m.b + 22;

    svg += bar3d(x, y, bW, h, color, depth);
    svg += createValueText(
      x + bW / 2 + depth / 2,
      y - depth - 8,
      formatLabel(v),
      isBest,
    );
    svg += createTitleText(x + bW / 2, labelY, r.lib, 13, 500, "#4b5563");
  });

  return svg;
};

const combine = (data, lang) => {
  const { tp, ratio, ref_ratio } = data;

  const m = { t: 70, r: 50, b: 55, l: 70, cW: 400, chartH: 280 };
  const W = m.l + m.cW + m.r;
  const chartGap = 0;
  const titleH = 90;
  const chartTitleOffset = 20;

  const mainTitle =
    lang === "en" ? "jdb_fsst vs fsst Benchmark" : "jdb_fsst vs fsst 性能评测";
  const subTitle = lang === "en" ? `Performance Comparison` : `性能对比`;

  const titles =
    lang === "en"
      ? [
          "Encode Throughput (MB/s)",
          "Decode Throughput (MB/s)",
          "Compression Ratio (%) (lower is better)",
        ]
      : ["编码吞吐量 (MB/s)", "解码吞吐量 (MB/s)", "压缩率 (%) (越小越好)"];

  const results = Object.entries(tp).map(([lib, metrics]) => ({
    lib,
    encode_throughput: metrics.enc,
    decode_throughput: metrics.dec,
    compression_ratio: lib === "jdb_fsst" ? ratio : ref_ratio,
  }));

  const sortedByRatio = [...results].sort(
    (a, b) => a.compression_ratio - b.compression_ratio,
  );
  const sortedByEncode = [...results].sort(
    (a, b) => b.encode_throughput - a.encode_throughput,
  );
  const sortedByDecode = [...results].sort(
    (a, b) => b.decode_throughput - a.decode_throughput,
  );

  const charts = [
    {
      data: sortedByEncode,
      getValue: (r) => r.encode_throughput,
      format: (v) => v.toFixed(1),
      lowerIsBetter: false,
    },
    {
      data: sortedByDecode,
      getValue: (r) => r.decode_throughput,
      format: (v) => v.toFixed(1),
      lowerIsBetter: false,
    },
    {
      data: sortedByRatio,
      getValue: (r) => r.compression_ratio,
      format: (v) => v.toFixed(2),
      lowerIsBetter: true,
    },
  ];

  let svg = "";
  let yOffset = titleH;

  svg += createTitleText(W / 2, 42, mainTitle, MAIN_TITLE_SIZE, 700, "#111827");
  svg += createTitleText(W / 2, 68, subTitle, 14, 400, "#6b7280");

  charts.forEach((chart, idx) => {
    svg += createTitleText(
      W / 2,
      yOffset + chartTitleOffset,
      titles[idx],
      16,
      600,
      "#374151",
    );
    svg += barChart(
      chart.data,
      m,
      yOffset,
      chart.getValue,
      chart.format,
      chart.lowerIsBetter,
    );
    yOffset += m.chartH + chartGap;
  });

  const H = yOffset - 10;

  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">\n${svg}\n</svg>`;
};

try {
  mkdirSync("readme/en", { recursive: true });
  mkdirSync("readme/zh", { recursive: true });
} catch {}

const svgoConfig = { plugins: ["preset-default"] };
const compressAndWrite = (filename, content) => {
  const result = optimize(content, svgoConfig);
  writeFileSync(filename, result.data);
};

compressAndWrite("readme/en/bench.svg", combine(data, "en"));
compressAndWrite("readme/zh/bench.svg", combine(data, "zh"));
console.log("Generated: readme/en/bench.svg, readme/zh/bench.svg");
