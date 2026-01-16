import { readFileSync, readdirSync, existsSync, statSync } from "fs";
import { join } from "path";
import { $ } from "zx";

export const getTargetDir = async () => {
  const metadata = await $`cargo metadata --format-version 1`.quiet();
  const json = JSON.parse(metadata.stdout);
  return json.target_directory;
};

export const parseCriterion = (target_dir) => {
  const results = {};
  const criterion_dir = `${target_dir}/criterion`;

  if (!existsSync(criterion_dir)) {
    console.log(`Warning: Criterion directory not found: ${criterion_dir}`);
    return results;
  }

  // New benchmark groups
  const groups = ["Random Access", "Search", "Sequential", "Range"];

  for (const group of groups) {
    const group_dir = join(criterion_dir, group);
    if (!existsSync(group_dir)) continue;

    // Inside group dir, we expect "Op/Lib" or just "Op" folders?
    // Criterion maps "get/jdb_pef" to "get/jdb_pef" folder structure.
    // So group_dir contains "get" folder (for Random Access) or "next_ge" (for Search), or "iter" (for Sequential).

    // Actually, distinct bench functions usually strictly map.
    // Let's iterate all subdirectories recursively or just know the names.
    // Known Ops:
    // Random Access -> "get"
    // Search -> "next_ge"
    // Sequential -> "iter"

    let ops = [];
    if (group === "Random Access") ops = ["get"];
    if (group === "Search") ops = ["next_ge"];
    if (group === "Sequential") ops = ["iter", "rev_iter"];
    if (group === "Range") ops = ["range_iter", "rev_range_iter"];

    for (const op of ops) {
      const op_dir = join(group_dir, op);
      if (!existsSync(op_dir)) continue;

      const libs = readdirSync(op_dir);
      for (const lib of libs) {
        const lib_dir = join(op_dir, lib);
        if (!statSync(lib_dir).isDirectory()) continue;

        const estimates_file = join(lib_dir, "base", "estimates.json");
        if (!existsSync(estimates_file)) continue;

        const estimates = JSON.parse(readFileSync(estimates_file, "utf8"));
        const mean_ns = estimates.mean.point_estimate;

        if (!results[lib]) results[lib] = {};
        if (!results[lib][group]) results[lib][group] = {};

        results[lib][group][op] = { mean_ns };
      }
    }
  }
  return results;
};

export const formatBytes = (bytes) => {
  if (bytes === 0) return "0 B";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
};

export const formatTime = (ns) => {
  if (ns < 1000) return `${ns.toFixed(2)} ns`;
  if (ns < 1000000) return `${(ns / 1000).toFixed(2)} μs`;
  if (ns < 1000000000) return `${(ns / 1000000).toFixed(2)} ms`;
  return `${(ns / 1000000000).toFixed(2)} s`;
};
export const normalizeFilter = (name) => name.replace("BinaryFuse", "Bf");

export const filterOrder = {
  Bf8: 0,
  BinaryFuse8: 0,
  Bf16: 1,
  BinaryFuse16: 1,
  Bf32: 2,
  BinaryFuse32: 2,
};

export const getFilterScore = (filterName) => {
  return filterOrder[filterName] ?? 999;
};

export const compareFilters = (a, b) => {
  const orderA = getFilterScore(a);
  const orderB = getFilterScore(b);
  return orderA - orderB;
};
