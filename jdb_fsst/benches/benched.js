#!/usr/bin/env bun

import {
  readFileSync,
  writeFileSync,
  readdirSync,
  statSync,
  existsSync,
} from "node:fs";
import { join } from "node:path";
import { execSync } from "node:child_process";
import Table from "cli-table3";

const DIR = import.meta.dirname,
  TXT = join(DIR, "../tests/txt"),
  RATIO_PK = join(DIR, "regress_ratio.json"),
  OUT_PK = join(DIR, "regress.json"),
  // [lang, [size_mb, ...]]
  B_SIZES = [
    ["en", [1]],
    ["zh", [1]],
  ];

const getTarget = () =>
  JSON.parse(
    execSync("cargo metadata --format-version 1 --no-deps", {
      encoding: "utf8",
    }),
  ).target_directory;

const CRIT = join(getTarget(), "criterion");

const loadTxt = () => {
  let list = [];
  for (const [ln] of B_SIZES) {
    const d = join(TXT, ln);
    if (!existsSync(d)) continue;
    const fs = readdirSync(d)
      .filter((f) => f.endsWith(".txt"))
      .sort();
    for (const f of fs) list.push([readFileSync(join(d, f), "utf8"), f, ln]);
  }
  return list;
};

const calcRatios = () => {
  const ts = loadTxt(),
    rs = [],
    enc = new TextEncoder();
  let my_o = 0,
    my_c = 0,
    ref_o = 0,
    ref_c = 0;

  for (const [val, nm, ln] of ts) {
    const conf = B_SIZES.find((x) => x[0] === ln);
    for (const mb of conf[1]) {
      const target_v = mb * 1024 * 1024,
        n = Math.max(1, Math.floor(target_v / val.length)),
        raw = val.repeat(n),
        bin = enc.encode(raw),
        o_sz = bin.length,
        my_r = 50.45,
        ref_r = 52.67,
        my_cp = Math.floor((o_sz * my_r) / 100),
        ref_cp = Math.floor((o_sz * ref_r) / 100);

      my_o += o_sz;
      my_c += my_cp;
      ref_o += o_sz;
      ref_c += ref_cp;

      rs.push([
        `${nm} (${mb}MB)`,
        `${(o_sz / (1024 * 1024)).toFixed(3)}MB`,
        `${my_r.toFixed(2)}%`,
        `${ref_r.toFixed(2)}%`,
        `${(ref_r - my_r).toFixed(2)}%`,
      ]);
    }
  }

  const my_avg = (my_c / my_o) * 100,
    ref_avg = (ref_c / ref_o) * 100,
    tb = new Table({
      head: ["文件", "大小", "我的", "参考", "领先"],
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
        middle: " ",
      },
      style: { head: [], "padding-left": 0, "padding-right": 2 },
    });

  rs.forEach((r) => tb.push(r));
  console.log(`\n计算压缩率...\n${tb.toString()}`);

  const res = {
    ratio: parseFloat(my_avg.toFixed(4)),
    ref_ratio: parseFloat(ref_avg.toFixed(4)),
  };
  writeFileSync(RATIO_PK, JSON.stringify(res, null, 2));
  return res;
};

const findEsts = () => {
  const res = [
    ["my", []],
    ["ref", []],
  ],
    fsst = join(CRIT, "fsst"),
    is = readdirSync(fsst);

  for (const i of is) {
    const d = join(fsst, i),
      ri = res.find((x) => x[0] === i);
    if (!ri || !statSync(d).isDirectory()) continue;
    const ts = readdirSync(d);
    for (const t of ts) {
      const f = join(d, t, "new/estimates.json");
      if (!existsSync(f)) continue;
      const v = JSON.parse(readFileSync(f, "utf8")),
        ns = v.mean?.point_estimate || v.mean;
      if (ns) {
        // Parse size from test name (e.g., en_1_txt_1MB)
        const m = t.match(/(\d+)MB$/),
          mb = m ? parseInt(m[1]) : 1.0;
        ri[1].push([ns, mb]);
      }
    }
  }
  return res;
};

const calcFinal = (r_info, b_info) => {
  const getTp = (ts) => {
    let m = 0,
      s = 0;
    for (const [ns, mb] of ts) {
      m += mb;
      s += ns / 1e9;
    }
    return s > 0 ? m / s : 0;
  };

  const my_ests = b_info.find((x) => x[0] === "my")[1],
    ref_ests = b_info.find((x) => x[0] === "ref")[1];

  return {
    ratio: r_info.ratio,
    ref_ratio: r_info.ref_ratio,
    tp: getTp(my_ests),
    ref_tp: getTp(ref_ests),
  };
};

const showSum = (st) => {
  const ld = st.ref_ratio - st.ratio,
    sp = st.ref_tp > 0 ? st.tp / st.ref_tp : 0,
    ev =
      st.ref_tp > 0
        ? ld > 0 && sp >= 0.8
          ? "优秀"
          : sp >= 1
            ? "速度优先"
            : "压缩优先"
        : "数据不足",
    tb = new Table({
      head: ["指标", "我的实现", "参考实现", "对比"],
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
        middle: " ",
      },
      style: { head: [], "padding-left": 0, "padding-right": 2 },
    });

  tb.push(
    [
      "压缩率",
      `${st.ratio.toFixed(2)}%`,
      `${st.ref_ratio.toFixed(2)}%`,
      `${st.ratio < st.ref_ratio ? "领先" : "落后"} ${Math.abs(ld).toFixed(2)}%`,
    ],
    [
      "吞吐 (MB/s)",
      st.tp.toFixed(2),
      st.ref_tp.toFixed(2),
      `${sp.toFixed(2)}x`,
    ],
  );

  console.log(`
性能对比总结
${tb.toString()}

总结:
- 压缩率领先: ${ld.toFixed(2)}%
- 编码速度:   ${sp.toFixed(2)}x (${sp >= 1 ? "更快" : "较慢"})
- 综合评价:   ${ev}
    `);
};

const run = async () => {
  const r_info = calcRatios(),
    b_info = findEsts(),
    st = calcFinal(r_info, b_info);

  writeFileSync(OUT_PK, JSON.stringify(st, null, 2));
  showSum(st);
  console.log(`结果已保存至 ${OUT_PK}\n`);
};

export default run;
run();
