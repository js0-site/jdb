//! Read bench.json and print table.
//! 读取 bench.json 并打印表格

use serde::Deserialize;
use std::io::Write;
use tabled::settings::Style;
use tabled::{Table, Tabled};

const JSON_PATH: &str = "bench.json";
const EN_MD: &str = "readme/en.bench.md";
const ZH_MD: &str = "readme/zh.bench.md";

#[derive(Deserialize)]
struct BenchResult {
    lib: String,
    add_mops: f64,
    contains_mops: f64,
    remove_mops: f64,
    memory_kb: f64,
    fpp: f64,
}

#[derive(Deserialize)]
struct BenchOutput {
    items: usize,
    capacity: usize,
    target_fpp: f64,
    results: Vec<BenchResult>,
}

#[derive(Tabled)]
struct Row {
    #[tabled(rename = "Library")]
    lib: String,
    #[tabled(rename = "FPP")]
    fpp: String,
    #[tabled(rename = "Contains (M/s)")]
    contains: String,
    #[tabled(rename = "Add (M/s)")]
    add: String,
    #[tabled(rename = "Remove (M/s)")]
    remove: String,
    #[tabled(rename = "Memory (KB)")]
    memory: String,
}

fn main() {
    let json = std::fs::read_to_string(JSON_PATH).expect("Failed to read bench.json");
    let data: BenchOutput = sonic_rs::from_str(&json).expect("Failed to parse JSON");

    let base = &data.results[0];

    // Console table
    // 控制台表格
    let rows: Vec<Row> = data
        .results
        .iter()
        .map(|r| {
            let ar = r.add_mops / base.add_mops;
            let cr = r.contains_mops / base.contains_mops;
            let rr = r.remove_mops / base.remove_mops;
            Row {
                lib: r.lib.clone(),
                fpp: format!("{:.2}%", r.fpp * 100.0),
                contains: format!("{:.2} ({:.2})", r.contains_mops, cr),
                add: format!("{:.2} ({:.2})", r.add_mops, ar),
                remove: format!("{:.2} ({:.2})", r.remove_mops, rr),
                memory: format!("{:.1}", r.memory_kb),
            }
        })
        .collect();

    let table = Table::new(rows).with(Style::ascii()).to_string();
    println!(
        "\n=== Performance ({} items, capacity={}, FPP≈{:.0}%) ===\n",
        data.items,
        data.capacity,
        data.target_fpp * 100.0
    );
    println!("{table}\n");

    // English markdown
    // 英文 markdown
    let en = gen_md_en(&data);
    std::fs::File::create(EN_MD)
        .unwrap()
        .write_all(en.as_bytes())
        .unwrap();

    // Chinese markdown
    // 中文 markdown
    let zh = gen_md_zh(&data);
    std::fs::File::create(ZH_MD)
        .unwrap()
        .write_all(zh.as_bytes())
        .unwrap();

    println!("Saved to {EN_MD} and {ZH_MD}");
}

fn gen_md_en(data: &BenchOutput) -> String {
    let base = &data.results[0];
    let mut s = String::new();

    s.push_str("## Benchmark Results\n\n");
    s.push_str(&format!(
        "Test: {} items, capacity={}, target FPP≈{:.0}%\n\n",
        data.items,
        data.capacity,
        data.target_fpp * 100.0
    ));

    // FPP explanation
    // FPP 说明
    s.push_str("### What is FPP?\n\n");
    s.push_str("**FPP (False Positive Probability)** is the probability that a filter incorrectly reports an item as present when it was never added. ");
    s.push_str("Lower FPP means higher accuracy but requires more memory. ");
    s.push_str("A typical FPP of 1% means about 1 in 100 queries for non-existent items will incorrectly return \"possibly exists\".\n\n");

    // Combined table
    // 综合表格
    s.push_str("### Performance Comparison\n\n");
    s.push_str("| Library | FPP | Contains (M/s) | Add (M/s) | Remove (M/s) | Memory (KB) |\n");
    s.push_str("|---------|-----|----------------|-----------|--------------|-------------|\n");
    for r in &data.results {
        let ar = r.add_mops / base.add_mops;
        let cr = r.contains_mops / base.contains_mops;
        let rr = r.remove_mops / base.remove_mops;
        s.push_str(&format!(
            "| {} | {:.2}% | {:.2} ({:.2}) | {:.2} ({:.2}) | {:.2} ({:.2}) | {:.1} |\n",
            r.lib,
            r.fpp * 100.0,
            r.contains_mops, cr,
            r.add_mops, ar,
            r.remove_mops, rr,
            r.memory_kb
        ));
    }

    s.push_str("\n*Ratio in parentheses: relative to autoscale_cuckoo_filter (1.00 = baseline)*\n");

    s
}

fn gen_md_zh(data: &BenchOutput) -> String {
    let base = &data.results[0];
    let mut s = String::new();

    s.push_str("## 性能测试结果\n\n");
    s.push_str(&format!(
        "测试：{} 条数据，容量={}，目标误判率≈{:.0}%\n\n",
        data.items,
        data.capacity,
        data.target_fpp * 100.0
    ));

    // FPP explanation
    // 误判率说明
    s.push_str("### 什么是误判率（FPP）？\n\n");
    s.push_str("**误判率（False Positive Probability，FPP）** 是指过滤器错误地报告某个元素存在的概率，即该元素实际上从未被添加过。");
    s.push_str("误判率越低，准确性越高，但需要更多内存。");
    s.push_str("典型的 1% 误判率意味着大约每 100 次查询不存在的元素，会有 1 次错误地返回「可能存在」。\n\n");

    // Combined table
    // 综合表格
    s.push_str("### 性能对比\n\n");
    s.push_str("| 库 | 误判率 | 查询 (百万/秒) | 添加 (百万/秒) | 删除 (百万/秒) | 内存 (KB) |\n");
    s.push_str("|---------|-----|----------------|-----------|--------------|-------------|\n");
    for r in &data.results {
        let ar = r.add_mops / base.add_mops;
        let cr = r.contains_mops / base.contains_mops;
        let rr = r.remove_mops / base.remove_mops;
        s.push_str(&format!(
            "| {} | {:.2}% | {:.2} ({:.2}) | {:.2} ({:.2}) | {:.2} ({:.2}) | {:.1} |\n",
            r.lib,
            r.fpp * 100.0,
            r.contains_mops, cr,
            r.add_mops, ar,
            r.remove_mops, rr,
            r.memory_kb
        ));
    }

    s.push_str("\n*括号内为相对性能：以 autoscale_cuckoo_filter 为基准（1.00 = 基准值）*\n");

    s
}
