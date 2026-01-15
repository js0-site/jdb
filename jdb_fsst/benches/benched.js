#!/usr/bin/env bun

import { writeFileSync } from "node:fs";
import { join } from "node:path";
import Table from "cli-table3";
import { conv } from "./conv.js";

const DIR = import.meta.dirname,
  RATIO_PK = join(DIR, "regress_ratio.json"),
  OUT_PK = join(DIR, "regress.json");

const TB_CHARS = {
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
};

const TB_STYLE = { head: [], "padding-left": 0, "padding-right": 0 };

const showSum = (st) => {
  const { impls, tp } = st;
  if (impls.length === 0) {
    console.log("\n无基准测试数据");
    return;
  }

  // Build header: 指标 + each impl
  // 构建表头
  const head = ["指标", ...impls.map((i) => i)];

  // Find baseline (prefer 'fsst', fallback to first)
  // 找基准（优先 fsst，否则第一个）
  const base = impls.includes("fsst") ? "fsst" : impls[0];

  const tb = new Table({ head, chars: TB_CHARS, style: TB_STYLE });

  // Encode row
  // 编码行
  const enc_row = ["编码 (MB/s)"];
  for (const impl of impls) {
    const v = tp[impl]?.enc || 0;
    const base_v = tp[base]?.enc || 0;
    const ratio = base_v > 0 ? (v / base_v).toFixed(2) + "x" : "-";
    enc_row.push(impl === base ? v.toFixed(2) : `${v.toFixed(2)} (${ratio})`);
  }
  tb.push(enc_row);

  // Decode row
  // 批量解码行
  const dec_row = ["批量解码 (MB/s)"];
  for (const impl of impls) {
    const v = tp[impl]?.dec || 0;
    const base_v = tp[base]?.dec || 0;
    const ratio = base_v > 0 ? (v / base_v).toFixed(2) + "x" : "-";
    dec_row.push(impl === base ? v.toFixed(2) : `${v.toFixed(2)} (${ratio})`);
  }
  tb.push(dec_row);

  // Random Decode row
  // 随机解码行
  const ran_row = ["随机解码 (MB/s)"];
  for (const impl of impls) {
    const v = tp[impl]?.ran || 0;
    const base_v = tp[base]?.ran || 0;
    const ratio = base_v > 0 ? (v / base_v).toFixed(2) + "x" : "-";
    ran_row.push(impl === base ? v.toFixed(2) : `${v.toFixed(2)} (${ratio})`);
  }
  tb.push(ran_row);

  console.log(`\n性能对比 (基准: ${base})\n${tb.toString()}`);

  // Summary for jdb_fsst vs fsst if both exist
  // 如果 jdb_fsst 和 fsst 都存在，显示摘要
  if (tp.jdb_fsst && tp.fsst) {
    const ld = st.ref_ratio - st.ratio,
      sp_enc = tp.fsst.enc > 0 ? tp.jdb_fsst.enc / tp.fsst.enc : 0,
      sp_dec = tp.fsst.dec > 0 ? tp.jdb_fsst.dec / tp.fsst.dec : 0,
      sp_ran = tp.fsst.ran > 0 ? tp.jdb_fsst.ran / tp.fsst.ran : 0;

    console.log(`\n总结 (jdb_fsst vs fsst):
- 压缩率领先: ${ld.toFixed(2)}%
- 编码速度:   ${sp_enc.toFixed(2)}x (${sp_enc >= 1 ? "更快" : "较慢"})
- 批量解码:   ${sp_dec.toFixed(2)}x (${sp_dec >= 1 ? "更快" : "较慢"})
- 随机解码:   ${sp_ran.toFixed(2)}x (${sp_ran >= 1 ? "更快" : "较慢"})
`);
  }
};

const run = async () => {
  const st = conv();

  // Show ratio table
  // 显示压缩率表
  const r_info = st._r_info;
  const tb = new Table({
    head: ["文件", "大小", "jdb_fsst", "fsst", "领先"],
    chars: TB_CHARS,
    style: TB_STYLE,
  });
  r_info.table.forEach((r) => tb.push(r));
  console.log(`\n计算压缩率...\n${tb.toString()}`);

  writeFileSync(
    RATIO_PK,
    JSON.stringify({ ratio: st.ratio, ref_ratio: st.ref_ratio }, null, 2),
  );
  writeFileSync(OUT_PK, JSON.stringify(st, null, 2));
  showSum(st);
  console.log(`结果已保存至 ${OUT_PK}\n`);
};

export default run;
run();
