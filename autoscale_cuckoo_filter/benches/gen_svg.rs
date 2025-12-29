//! Generate SVG charts from bench.json.
//! 从 bench.json 生成 SVG 图表

use charming::component::{Axis, Grid, Legend, Title};
use charming::element::{AxisType, ItemStyle, Label, LabelPosition, Tooltip, Trigger};
use charming::series::Bar;
use charming::{Chart, ImageRenderer};
use serde::Deserialize;

const JSON_PATH: &str = "bench.json";
const SVG_DIR: &str = "readme";

// Colors for each library (consistent across all charts)
// 每个库的颜色（所有图表中保持一致）
const COLORS: [&str; 3] = ["#5470c6", "#91cc75", "#fac858"];
// Darker shades for 3D effect
// 用于 3D 效果的深色
const COLORS_DARK: [&str; 3] = ["#3d5a9e", "#6fa85a", "#d4a83d"];

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
    results: Vec<BenchResult>,
}

fn main() {
    let json = std::fs::read_to_string(JSON_PATH).expect("Failed to read bench.json");
    let data: BenchOutput = sonic_rs::from_str(&json).expect("Failed to parse JSON");

    let libs: Vec<String> = data.results.iter().map(|r| r.lib.clone()).collect();

    // English combined chart
    // 英文合并图表
    let en_svg = gen_combined_svg(
        &data,
        &libs,
        "Performance Benchmark",
        &["Contains", "Add", "Remove"],
        "M ops/s",
        "Memory (KB)",
        "FPP - False Positive Probability (lower is better) %",
    );
    std::fs::write(format!("{SVG_DIR}/en.bench.svg"), en_svg).unwrap();

    // Chinese combined chart
    // 中文合并图表
    let zh_svg = gen_combined_svg(
        &data,
        &libs,
        "性能测试",
        &["查询", "添加", "删除"],
        "百万次/秒",
        "内存 (KB)",
        "FPP 误判率 (越低越好) %",
    );
    std::fs::write(format!("{SVG_DIR}/zh.bench.svg"), zh_svg).unwrap();

    // Clean up old files
    // 清理旧文件
    for f in ["en.perf.svg", "en.memory.svg", "en.fpp.svg", "zh.perf.svg", "zh.memory.svg", "zh.fpp.svg"] {
        let _ = std::fs::remove_file(format!("{SVG_DIR}/{f}"));
    }

    println!("Generated {SVG_DIR}/en.bench.svg and {SVG_DIR}/zh.bench.svg");
}

fn gen_combined_svg(
    data: &BenchOutput,
    libs: &[String],
    title: &str,
    ops: &[&str; 3],
    perf_unit: &str,
    mem_unit: &str,
    fpp_unit: &str,
) -> String {
    let width = 800;
    let chart_height = 320;
    let total_height = chart_height * 3;

    let mut renderer = ImageRenderer::new(width, chart_height);

    // Performance chart (Contains, Add, Remove)
    // 性能图表（查询、添加、删除）
    let perf_chart = Chart::new()
        .title(Title::new().text(format!("{title} - {perf_unit}")).left("center"))
        .tooltip(Tooltip::new().trigger(Trigger::Axis))
        .legend(Legend::new().top("bottom"))
        .grid(Grid::new().left("10%").right("5%").bottom("18%").top("12%"))
        .x_axis(Axis::new().type_(AxisType::Category).data(vec![ops[0], ops[1], ops[2]]))
        .y_axis(Axis::new().type_(AxisType::Value));

    let mut perf_chart = perf_chart;
    for (i, r) in data.results.iter().enumerate() {
        perf_chart = perf_chart.series(
            Bar::new()
                .name(&r.lib)
                .data(vec![
                    round2(r.contains_mops),
                    round2(r.add_mops),
                    round2(r.remove_mops),
                ])
                .item_style(ItemStyle::new().color(COLORS[i % 3]).border_color(COLORS_DARK[i % 3]).border_width(2))
                .label(Label::new().show(true).position(LabelPosition::Top)),
        );
    }

    // Memory chart
    // 内存图表
    let mem_chart = Chart::new()
        .title(Title::new().text(mem_unit).left("center"))
        .tooltip(Tooltip::new().trigger(Trigger::Axis))
        .legend(Legend::new().top("bottom"))
        .grid(Grid::new().left("10%").right("5%").bottom("18%").top("12%"))
        .x_axis(Axis::new().type_(AxisType::Category).data(libs.to_vec()))
        .y_axis(Axis::new().type_(AxisType::Value));

    let mut mem_chart = mem_chart;
    for (i, r) in data.results.iter().enumerate() {
        mem_chart = mem_chart.series(
            Bar::new()
                .name(&r.lib)
                .data(vec![round2(r.memory_kb)])
                .item_style(ItemStyle::new().color(COLORS[i % 3]).border_color(COLORS_DARK[i % 3]).border_width(2))
                .label(Label::new().show(true).position(LabelPosition::Top)),
        );
    }

    // FPP chart
    // 误判率图表
    let fpp_chart = Chart::new()
        .title(Title::new().text(fpp_unit).left("center"))
        .tooltip(Tooltip::new().trigger(Trigger::Axis))
        .legend(Legend::new().top("bottom"))
        .grid(Grid::new().left("10%").right("5%").bottom("18%").top("12%"))
        .x_axis(Axis::new().type_(AxisType::Category).data(libs.to_vec()))
        .y_axis(Axis::new().type_(AxisType::Value));

    let mut fpp_chart = fpp_chart;
    for (i, r) in data.results.iter().enumerate() {
        fpp_chart = fpp_chart.series(
            Bar::new()
                .name(&r.lib)
                .data(vec![round2(r.fpp * 100.0)])
                .item_style(ItemStyle::new().color(COLORS[i % 3]).border_color(COLORS_DARK[i % 3]).border_width(2))
                .label(Label::new().show(true).position(LabelPosition::Top)),
        );
    }

    // Render each chart
    // 渲染每个图表
    let perf_svg = renderer.render(&perf_chart).unwrap();
    let mem_svg = renderer.render(&mem_chart).unwrap();
    let fpp_svg = renderer.render(&fpp_chart).unwrap();

    // Extract inner content from SVG
    // 从 SVG 提取内部内容
    fn extract_inner(svg: &str) -> &str {
        let start = svg.find('>').map(|i| i + 1).unwrap_or(0);
        let end = svg.rfind("</svg>").unwrap_or(svg.len());
        &svg[start..end]
    }

    // Combine into single SVG with vertical layout
    // 合并为单个垂直布局的 SVG
    format!(
        r#"<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{total_height}">
<defs>
  <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
    <feDropShadow dx="3" dy="3" stdDeviation="2" flood-opacity="0.3"/>
  </filter>
</defs>
<g transform="translate(0,0)" filter="url(#shadow)">{}</g>
<g transform="translate(0,{chart_height})" filter="url(#shadow)">{}</g>
<g transform="translate(0,{})" filter="url(#shadow)">{}</g>
</svg>"#,
        extract_inner(&perf_svg),
        extract_inner(&mem_svg),
        chart_height * 2,
        extract_inner(&fpp_svg)
    )
}

#[inline]
fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}
