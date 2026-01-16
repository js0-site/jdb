#!/usr/bin/env bun

import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { $ } from "zx";
import Table from "cli-table3";
import { render } from "pug";
import { conv } from "./benches/conv.js";

const DIR = import.meta.dirname,
  PK_HIST = join(DIR, "benches/regress_history.json"),
  PK_HTML = join(DIR, "benches/regress.html"),
  MAX_H = 50,
  // [键名, 标签, 单位, 是否越小越好]
  BENCHS = [
    ["ratio", "压缩率", "%", true],
    ["tp_enc", "编码吞吐", "MB/s", false],
    ["tp_dec", "批量解码", "MB/s", false],
    ["tp_ran", "随机解码", "MB/s", false],
  ];

const fmtV = (v, u) =>
  v === null || v === undefined
    ? "无数据"
    : `${v.toFixed(3)}${u === "%" ? "" : " "}${u}`;

const fmtD = (v, p, low) => {
  if (p === undefined || p === null || v === undefined || v === null || p === 0)
    return "—";
  const d = ((v - p) / p) * 100,
    s = d >= 0 ? "+" : "";
  const cl = (low ? d <= -0.01 : d >= 0.01)
    ? "32"
    : (low ? d >= 0.01 : d <= -0.01)
      ? "31"
      : "33";
  return `\x1b[${cl}m${s}${d.toFixed(2)}%\x1b[0m`;
};

const genHtml = (et, hs) => {
  const tpl = `
doctype html
html
  head
    meta(charset="utf-8")
    title 性能回归报告
    script(src="https://cdn.jsdelivr.net/npm/chart.js")
    style.
      body{font-family:-apple-system,sans-serif;margin:20px;background:#1a1a2e;color:#eee}
      h1{color:#00d4ff} .subtitle{color:#888;margin-bottom:20px}
      .latest{background:#16213e;padding:15px;border-radius:8px;margin:20px 0}
      .metrics{display:flex;flex-wrap:wrap;gap:15px}
      .metric{background:#0f3460;padding:12px 16px;border-radius:6px;min-width:140px}
      .metric .label{font-size:11px;color:#888} .metric .value{font-size:18px;font-weight:bold}
      .metric .diff{font-size:12px;margin-top:4px}
      .metric .diff.improved{color:#0f0} .metric .diff.regressed{color:#f66}
      .chart-container{width:100%;max-width:1200px;margin:30px 0}
  body
    h1 jdb_fsst 性能回归
    .subtitle 提交: #{et.commit} (#{et.branch}) | 时间: #{et.date}
    .latest
      .metrics
        each b in BENCHS
          - const [k, lb, u, low] = b, v = et[k], p = hs.length >= 2 ? hs[hs.length - 2][k] : null
          - let diff = (p && v && p !== 0) ? (((v - p) / p) * 100).toFixed(2) : null, cl = ""
          if diff !== null
            - const dv = parseFloat(diff), ok = low ? dv <= -0.01 : dv >= 0.01, bad = low ? dv >= 0.01 : dv <= -0.01
            - cl = ok ? "improved" : bad ? "regressed" : "neutral"
          .metric
            .label #{lb}
            .value #{fmt(v, u)}
            if diff !== null
              .diff(class=cl) #{parseFloat(diff) >= 0 ? "+" : ""}#{diff}%
    each b in BENCHS
      .chart-container: canvas(id="c_"+b[0])
    script.
      const d = !{JSON.stringify(hs)}, ls = d.map(x => x.commit);
      const cf = (id, t, label, data, color) => new Chart(document.getElementById(id), {
        type: "line", data: { labels: ls, datasets: [{label, data, borderColor: color}] },
        options: { plugins: { title: { display: true, text: t, color: "#eee" } } }
      });
      cf("c_ratio", "压缩率回归曲线 (越低越好)", "压缩率 (%)", d.map(x=>x.ratio), "#00d4ff");
      cf("c_tp_enc", "编码吞吐回归曲线 (越高越好)", "吞吐量 (MB/s)", d.map(x=>x.tp_enc), "#0f0");
      cf("c_tp_dec", "批量解码回归曲线 (越高越好)", "吞吐量 (MB/s)", d.map(x=>x.tp_dec), "#f0f");
      cf("c_tp_ran", "随机解码回归曲线 (越高越好)", "吞吐量 (MB/s)", d.map(x=>x.tp_ran), "#ff0");
    `;
  return render(tpl, { et, hs, BENCHS, fmt: fmtV });
};

const run = async () => {
  const commit = (await $`git rev-parse --short HEAD`.quiet()).text().trim(),
    branch = (await $`git branch --show-current`.quiet()).text().trim(),
    date = new Date().toLocaleString("zh-CN", { hour12: false }),
    et = { date, commit, branch };

  console.log(`启动回归测试 (提交: ${commit})...`);

  // 运行当前版本的基准测试
  await $`cargo bench --bench bench --features bench_jdb_fsst -- --quiet --nocapture`.quiet();

  const res = conv();
  et.ratio = res.ratio ?? null;
  // Extract throughput from tp.jdb_fsst
  // 从 tp.jdb_fsst 提取吞吐量
  et.tp_enc = res.tp?.jdb_fsst?.enc ?? null;
  et.tp_dec = res.tp?.jdb_fsst?.dec ?? null;
  et.tp_ran = res.tp?.jdb_fsst?.ran ?? null;

  let hs = existsSync(PK_HIST) ? JSON.parse(readFileSync(PK_HIST, "utf8")) : [];
  const prev = hs.length > 0 ? hs[hs.length - 1] : null;

  hs.push(et);
  if (hs.length > MAX_H) hs = hs.slice(-MAX_H);
  writeFileSync(PK_HIST, JSON.stringify(hs, null, 2));
  writeFileSync(PK_HTML, genHtml(et, hs));

  const tb = new Table({
    head: ["回归指标", "当前值", "变动幅度"],
    chars: {
      top: "",
      "top-mid": "",
      "top-left": "",
      "top-right": "",
      bottom: "",
      "bottom-mid": "",
      "bottom-left": "",
      "bottom-right": "",
      left: "",
      "left-mid": "",
      mid: "",
      "mid-mid": "",
      right: "",
      "right-mid": "",
      middle: "  ",
    },
    style: { head: [], "padding-left": 0, "padding-right": 0 },
  });
  BENCHS.forEach(([k, lb, u, low]) =>
    tb.push([lb, fmtV(et[k], u), fmtD(et[k], prev?.[k], low)]),
  );

  console.log(`\n${tb.toString()}\n\n${PK_HTML}`);
};

export default run;
run();

