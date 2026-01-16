#!/usr/bin/env node
import { writeFileSync, mkdirSync, readFileSync } from "fs";
import { getTargetDir, parseCriterion, formatTime } from "./lib.js";
import Table from "cli-table3";
import CURRENT_LABELS, { RESOURCES } from "./i18n/index.js";

const main = async () => {
  const target_dir = await getTargetDir();
  const results = parseCriterion(target_dir);

  // Read stats
  let stats = {};
  try {
    stats = JSON.parse(readFileSync("benches/stats.json", "utf8"));
  } catch (e) {
    console.warn("Could not read benches/stats.json");
  }
  const N = parseInt(stats["params.n"] || "50000");

  const libs = Object.keys(results).sort();

  if (libs.length === 0) {
    console.log("No benchmark results found.");
    return;
  }

  const opKeys = [
    { group: "Random Access", name: "get", key: "op_get", default: "Random Get", complexity: 1 },
    { group: "Search", name: "next_ge", key: "op_next_ge", default: "Next GE", complexity: 1 },
    { group: "Sequential", name: "iter", key: "op_iter", default: "Iterate", complexity: N },
    { group: "Sequential", name: "rev_iter", key: "op_rev_iter", default: "Rev Iterate", complexity: N },
    { group: "Range", name: "range_iter", key: "op_range", default: "Range", complexity: N / 2 },
    { group: "Range", name: "rev_range_iter", key: "op_rev_range", default: "Rev Range", complexity: N / 2 },
  ];

  mkdirSync("readme", { recursive: true });

  const fmtOps = (ns, complexity) => {
    if (!ns) return "-";
    // Ops/s = complexity * 1e9 / ns
    // Million = Ops/s / 1e6
    const ops = (complexity * 1e9) / ns;
    const million = ops / 1000000;
    return million.toFixed(3);
  };

  const getCompressionPct = (lib) => {
    const v = stats[`${lib}.bpe`];
    if (!v) return "-";
    const bpe = parseFloat(v);
    const pct = (bpe / 64.0) * 100.0;
    return pct.toFixed(2) + "%";
  };

  for (const [langCode, labels] of Object.entries(RESOURCES)) {
    // Columns: Op, Lib1, Lib2...

    let md = `| ${labels.metric || "Operation"} | ` + libs.join(" | ") + " |\n";
    md += "| --- | " + libs.map(() => "---").join(" | ") + " |\n";

    // Compression Row
    const compLabel = labels.compression_ratio || "Compression Ratio (%)";
    let rowComp = [compLabel];

    for (const lib of libs) {
      rowComp.push(getCompressionPct(lib));
    }
    md += "| " + rowComp.join(" | ") + " |\n";

    for (const op of opKeys) {
      const label = labels[op.key] || op.default;

      let rowHasData = false;
      const mdRow = [label];

      for (const lib of libs) {
        const res = results[lib]?.[op.group]?.[op.name];
        if (res) {
          mdRow.push(fmtOps(res.mean_ns, op.complexity));
          rowHasData = true;
        } else {
          mdRow.push("-");
        }
      }
      // Always show row if structure dictates
      md += "| " + mdRow.join(" | ") + " |\n";
    }

    const filename = `readme/${langCode}.bench.md`;
    let title = labels.table_title_perf || "Benchmark Results";
    if (labels.unit_label) title += ` (${labels.unit_label})`;

    writeFileSync(filename, `# ${title}\n\n` + md);
    console.log(`Generated ${filename}`);
  }

  // Console Output
  const consoleTable = new Table({
    head: [CURRENT_LABELS.metric || "Operation", ...libs],
  });

  // Compression Row
  const consoleCompRow = [CURRENT_LABELS.compression_ratio || "Compression Ratio (%)"];
  for (const lib of libs) {
    consoleCompRow.push(getCompressionPct(lib));
  }
  consoleTable.push(consoleCompRow);

  for (const op of opKeys) {
    const label = CURRENT_LABELS[op.key] || op.default;
    const row = [label];

    for (const lib of libs) {
      const res = results[lib]?.[op.group]?.[op.name];
      if (res) {
        row.push(fmtOps(res.mean_ns, op.complexity));
      } else {
        row.push("-");
      }
    }
    consoleTable.push(row);
  }

  const title = CURRENT_LABELS.table_title_perf || "Benchmark Results";
  const unit = CURRENT_LABELS.unit_label || "Million Ops/s";
  console.log(`\n${title} (${unit}):`);
  console.log(consoleTable.toString());
};
const lang = process.env.LANG || "";
const isZh = lang.includes("zh") || lang.includes("CN");

main();
