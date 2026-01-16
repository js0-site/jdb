
export const LANG = "en";

export const RESOURCES = {
    en: {
        table_title_perf: "Performance",
        table_title_accuracy: "Accuracy",
        table_headers_perf: ["Library", "Filter", "Build (Mops/s)", "Query (Mops/s)", "Memory", "Speedup"],
        table_headers_accuracy: ["Library", "Filter", "FPR", "FNR"],
        svg_title_main: "Benchmark Results",
        svg_title_build: "Build Throughput",
        svg_title_query: "Query Throughput",
        svg_title_memory: "Memory Usage",
        svg_title_accuracy: "False Positive Rate",
        svg_no_data: "No Data",
        metric: "Metric",
        build_ops: "Build (Mops/s)",
        query_ops: "Query (Mops/s)",
        memory_kb: "Memory (KB)",
        fp_rate: "FPR (%)",
        running_bench: "Running benchmarks...",
        saved_history: "Saved history",
        unit_label: "Million Ops/s",
        unit_short: "Mops/s",
        compression_ratio: "Compression Ratio (%)",
        op_get: "Random Get",
        op_next_ge: "Get Next",
        op_iter: "Iterate",
        op_rev_iter: "Reverse Iterate",
        op_range: "Range Forward",
        op_rev_range: "Range Reverse",
    },
    zh: {
        table_title_perf: "性能",
        table_title_accuracy: "准确率",
        table_headers_perf: ["库", "过滤器", "构建 (Mops/s)", "查询 (Mops/s)", "内存", "加速比"],
        table_headers_accuracy: ["库", "过滤器", "误报率", "漏报率"],
        svg_title_main: "基准测试结果",
        svg_title_build: "构建吞吐量",
        svg_title_query: "查询吞吐量",
        svg_title_memory: "内存使用",
        svg_title_accuracy: "误报率",
        svg_no_data: "无数据",
        metric: "指标",
        build_ops: "构建 (Mops/s)",
        query_ops: "查询 (Mops/s)",
        memory_kb: "内存 (KB)",
        fp_rate: "误报率 (%)",
        running_bench: "正在运行基准测试...",
        saved_history: "保存历史记录",
        unit_label: "百万操作/秒",
        unit_short: "百万/秒",
        compression_ratio: "压缩率 (%)",
        op_get: "随机读取",
        op_next_ge: "获取下一个",
        op_iter: "正向遍历",
        op_rev_iter: "反向遍历",
        op_range: "范围正向",
        op_rev_range: "范围反向",
    }
};

const lang = process.env.LANG || "";
const isZh = lang.includes("zh") || lang.includes("CN");

export const CURRENT_LABELS = isZh ? RESOURCES.zh : RESOURCES.en;
export default CURRENT_LABELS;
