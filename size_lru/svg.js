#!/usr/bin/env bun

import { readFileSync, writeFileSync, mkdirSync } from 'fs';
import { optimize } from 'svgo';

const data = JSON.parse(readFileSync('bench.json', 'utf-8'));

const COLORS = ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452'];
const DEPTH = 10, ANGLE = 0.5;

const lighten = (hex, p) => {
  const n = parseInt(hex.slice(1), 16);
  const r = Math.min(255, (n >> 16) + (2.55 * p) | 0);
  const g = Math.min(255, ((n >> 8) & 0xFF) + (2.55 * p) | 0);
  const b = Math.min(255, (n & 0xFF) + (2.55 * p) | 0);
  return `#${(r << 16 | g << 8 | b).toString(16).padStart(6, '0')}`;
};

const darken = (hex, p) => {
  const n = parseInt(hex.slice(1), 16);
  const r = Math.max(0, (n >> 16) - (2.55 * p) | 0);
  const g = Math.max(0, ((n >> 8) & 0xFF) - (2.55 * p) | 0);
  const b = Math.max(0, (n & 0xFF) - (2.55 * p) | 0);
  return `#${(r << 16 | g << 8 | b).toString(16).padStart(6, '0')}`;
};

const bar3d = (x, y, w, h, c) => `<g>
  <path d="M${x+w},${y} L${x+w+DEPTH*ANGLE},${y-DEPTH} L${x+w+DEPTH*ANGLE},${y+h-DEPTH} L${x+w},${y+h} Z" fill="${darken(c,20)}"/>
  <path d="M${x},${y} L${x+DEPTH*ANGLE},${y-DEPTH} L${x+w+DEPTH*ANGLE},${y-DEPTH} L${x+w},${y} Z" fill="${lighten(c,25)}"/>
  <rect x="${x}" y="${y}" width="${w}" height="${h}" fill="${c}" rx="1"/>
</g>`;

// Performance chart for a category / 单个类别的性能图
const perfChart = (cat, lang) => {
  const ops = lang === 'en' ? ['Get', 'Set'] : ['读取', '写入'];
  const title = lang === 'en' 
    ? `${cat.name} Performance (M ops/s)` 
    : `${cat.name} 性能 (百万次/秒)`;
  const W = 400, H = 220, m = { t: 40, r: 20, b: 65, l: 50 };
  const cW = W - m.l - m.r, cH = H - m.t - m.b;
  
  let max = 0;
  for (const r of cat.results) max = Math.max(max, r.get_mops, r.set_mops);
  max = Math.ceil(max / 5) * 5 || 5;
  
  const libs = cat.results.map(r => r.lib);
  const gW = cW / 2, bW = Math.min(25, (gW - 15) / libs.length), gap = 3;
  
  let svg = `<text x="${W/2}" y="24" text-anchor="middle" font-size="12" font-weight="bold" fill="#333">${title}</text>`;
  
  for (let i = 0; i <= 4; i++) {
    const y = m.t + cH - cH * i / 4;
    svg += `<line x1="${m.l}" y1="${y}" x2="${W-m.r}" y2="${y}" stroke="#e0e0e0"/>`;
    svg += `<text x="${m.l-6}" y="${y+3}" text-anchor="end" font-size="9" fill="#666">${max*i/4|0}</text>`;
  }
  svg += `<line x1="${m.l}" y1="${m.t+cH}" x2="${W-m.r}" y2="${m.t+cH}" stroke="#888"/>`;
  
  const keys = ['get_mops', 'set_mops'];
  ops.forEach((op, oi) => {
    const gX = m.l + gW * oi + (gW - libs.length * (bW + gap)) / 2;
    svg += `<text x="${m.l + gW * oi + gW/2}" y="${H-m.b+15}" text-anchor="middle" font-size="10" fill="#333">${op}</text>`;
    cat.results.forEach((r, li) => {
      const v = r[keys[oi]], h = (v / max) * cH;
      const x = gX + li * (bW + gap), y = m.t + cH - h;
      svg += bar3d(x, y, bW, h, COLORS[li % COLORS.length]);
    });
  });
  
  return svg;
};

// Hit rate chart / 命中率图
const hitChart = (cat, lang) => {
  const title = lang === 'en' ? `${cat.name} Hit Rate (%)` : `${cat.name} 命中率 (%)`;
  const W = 200, H = 180, m = { t: 35, r: 15, b: 55, l: 40 };
  const cW = W - m.l - m.r, cH = H - m.t - m.b;
  
  const max = 100;
  const libs = cat.results.map(r => r.lib);
  const bW = Math.min(30, (cW - 10) / libs.length), gap = 6;
  const total = libs.length * bW + (libs.length - 1) * gap;
  const startX = m.l + (cW - total) / 2;
  
  let svg = `<text x="${W/2}" y="20" text-anchor="middle" font-size="11" font-weight="bold" fill="#333">${title}</text>`;
  
  for (let i = 0; i <= 4; i++) {
    const y = m.t + cH - cH * i / 4;
    svg += `<line x1="${m.l}" y1="${y}" x2="${W-m.r}" y2="${y}" stroke="#e0e0e0"/>`;
    svg += `<text x="${m.l-5}" y="${y+3}" text-anchor="end" font-size="8" fill="#666">${25*i}</text>`;
  }
  svg += `<line x1="${m.l}" y1="${m.t+cH}" x2="${W-m.r}" y2="${m.t+cH}" stroke="#888"/>`;
  
  cat.results.forEach((r, i) => {
    const h = (r.hit_rate / max) * cH;
    const x = startX + i * (bW + gap), y = m.t + cH - h;
    svg += bar3d(x, y, bW, h, COLORS[i % COLORS.length]);
    svg += `<text x="${x+bW/2}" y="${y-DEPTH-2}" text-anchor="middle" font-size="8" fill="#333">${r.hit_rate.toFixed(0)}</text>`;
  });
  
  return svg;
};

// Memory chart / 内存图
const memChart = (cat, lang) => {
  const title = lang === 'en' ? `${cat.name} Memory (KB)` : `${cat.name} 内存 (KB)`;
  const W = 200, H = 180, m = { t: 35, r: 15, b: 55, l: 45 };
  const cW = W - m.l - m.r, cH = H - m.t - m.b;
  
  let max = Math.max(...cat.results.map(r => r.memory_kb));
  max = Math.ceil(max / 1000) * 1000 || 1000;
  
  const libs = cat.results.map(r => r.lib);
  const bW = Math.min(30, (cW - 10) / libs.length), gap = 6;
  const total = libs.length * bW + (libs.length - 1) * gap;
  const startX = m.l + (cW - total) / 2;
  
  let svg = `<text x="${W/2}" y="20" text-anchor="middle" font-size="11" font-weight="bold" fill="#333">${title}</text>`;
  
  for (let i = 0; i <= 4; i++) {
    const y = m.t + cH - cH * i / 4;
    svg += `<line x1="${m.l}" y1="${y}" x2="${W-m.r}" y2="${y}" stroke="#e0e0e0"/>`;
    svg += `<text x="${m.l-5}" y="${y+3}" text-anchor="end" font-size="8" fill="#666">${(max*i/4/1000).toFixed(0)}k</text>`;
  }
  svg += `<line x1="${m.l}" y1="${m.t+cH}" x2="${W-m.r}" y2="${m.t+cH}" stroke="#888"/>`;
  
  cat.results.forEach((r, i) => {
    const h = (r.memory_kb / max) * cH;
    const x = startX + i * (bW + gap), y = m.t + cH - h;
    svg += bar3d(x, y, bW, h, COLORS[i % COLORS.length]);
  });
  
  return svg;
};


// Combine all charts / 组合所有图表
const combine = (data, lang) => {
  const cats = data.categories;
  const W = 820, H = 600;
  const libs = cats[0].results.map(r => r.lib);
  
  // Legend / 图例
  let legend = '';
  libs.forEach((lib, i) => {
    const x = 30 + i * 115;
    legend += `<rect x="${x}" y="0" width="10" height="10" fill="${COLORS[i%COLORS.length]}" rx="2"/>`;
    legend += `<text x="${x+14}" y="9" font-size="9" fill="#333">${lib}</text>`;
  });
  
  let svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">
<rect width="${W}" height="${H}" fill="#fff"/>
<g transform="translate(0,15)">${legend}</g>`;
  
  // Row 1: Performance charts / 第一行：性能图
  cats.forEach((cat, i) => {
    const x = i * 270 + 5;
    svg += `<g transform="translate(${x},35)">${perfChart(cat, lang)}</g>`;
  });
  
  // Row 2: Hit rate and memory / 第二行：命中率和内存
  cats.forEach((cat, i) => {
    const x = i * 270 + 5;
    svg += `<g transform="translate(${x},270)">${hitChart(cat, lang)}</g>`;
    svg += `<g transform="translate(${x + 205},270)">${memChart(cat, lang)}</g>`;
  });
  
  svg += '</svg>';
  return svg;
};

// Ensure readme dir exists / 确保 readme 目录存在
try { mkdirSync('readme'); } catch {}

const svgoConfig = {
  plugins: ['preset-default', { name: 'removeViewBox', active: false }]
};

const compressAndWrite = (filename, content) => {
  const result = optimize(content, svgoConfig);
  writeFileSync(filename, result.data);
};

compressAndWrite('readme/en.svg', combine(data, 'en'));
compressAndWrite('readme/zh.svg', combine(data, 'zh'));
console.log('Done: readme/en.svg, readme/zh.svg');
