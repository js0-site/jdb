#!/usr/bin/env bun

import { writeFileSync } from "node:fs";
import { join } from "node:path";
import Table from "cli-table3";
import { conv } from "./conv.js";

const DIR = import.meta.dirname,
  RATIO_PK = join(DIR, "regress_ratio.json"),
  OUT_PK = join(DIR, "regress.json");

const showSum = (st) => {
  const ld = st.ref_ratio - st.ratio,
    sp_enc = st.ref_tp_enc > 0 ? st.tp_enc / st.ref_tp_enc : 0,
    sp_dec = st.ref_tp_dec > 0 ? st.tp_dec / st.ref_tp_dec : 0,
    ev =
      st.ref_tp_enc > 0
        ? ld > 0 && sp_enc >= 0.8
          ? "优秀"
          : sp_enc >= 1
            ? "速度优先"
            : "压缩优先"
        : "数据不足",
    tb = new Table({
      head: ["指标", "我的实现", "参考实现", "对比"],
      chars: {
        top: "", "top-mid": "", "top-left": "", "top-right": "",
        bottom: "", "bottom-mid": "", "bottom-left": "", "bottom-right": "",
        left: "", "left-mid": "", mid: "", "mid-mid": "",
        right: "", "right-mid": "", middle: "  "
      },
      style: { head: [], "padding-left": 0, "padding-right": 0 },
    });

  tb.push(
    [
      "压缩率",
      `${st.ratio.toFixed(2)}%`,
      `${st.ref_ratio.toFixed(2)}%`,
      `${st.ratio < st.ref_ratio ? "领先" : "落后"} ${Math.abs(ld).toFixed(2)}%`,
    ],
    [
      "编码吞吐 (MB/s)",
      st.tp_enc.toFixed(2),
      st.ref_tp_enc.toFixed(2),
      `${sp_enc.toFixed(2)}x`,
    ],
    [
      "解码吞吐 (MB/s)",
      st.tp_dec.toFixed(2),
      st.ref_tp_dec.toFixed(2),
      `${sp_dec.toFixed(2)}x`,
    ],
  );

  console.log(`\n性能对比总结\n${tb.toString()}\n\n总结:
- 压缩率领先: ${ld.toFixed(2)}%
- 编码速度:   ${sp_enc.toFixed(2)}x (${sp_enc >= 1 ? "更快" : "较慢"})
- 解码速度:   ${sp_dec.toFixed(2)}x (${sp_dec >= 1 ? "更快" : "较慢"})
- 综合评价:   ${ev}`);
};

const run = async () => {
  const st = conv();

  // Show ratio table
  const r_info = st._r_info;
  const tb = new Table({
    head: ["文件", "大小", "我的", "参考", "领先"],
    chars: {
      top: "", "top-mid": "", "top-left": "", "top-right": "",
      bottom: "", "bottom-mid": "", "bottom-left": "", "bottom-right": "",
      left: "", "left-mid": "", mid: "", "mid-mid": "",
      right: "", "right-mid": "", middle: "  "
    },
    style: { head: [], "padding-left": 0, "padding-right": 0 },
  });
  r_info.table.forEach((r) => tb.push(r));
  console.log(`\n计算压缩率...\n${tb.toString()}`);

  writeFileSync(RATIO_PK, JSON.stringify({ ratio: st.ratio, ref_ratio: st.ref_ratio }, null, 2));
  writeFileSync(OUT_PK, JSON.stringify(st, null, 2));
  showSum(st);
  console.log(`结果已保存至 ${OUT_PK}\n`);
};

export default run;
run();

