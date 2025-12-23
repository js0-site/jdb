#!/bin/bash
# Performance benchmark with git commit tracking
# 带 git commit 追踪的性能测试

set -e

RESULT_DIR="bench_results"
mkdir -p "$RESULT_DIR"

# Get git info 获取 git 信息
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
DATE=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="$RESULT_DIR/${DATE}_${COMMIT}.txt"

echo "=== JDB Performance Benchmark ==="
echo "Commit: $COMMIT"
echo "Branch: $BRANCH"
echo "Date: $(date)"
echo ""

# Run criterion benchmark 运行 criterion 测试
echo "Running benchmarks..."
cargo bench --bench perf -- --noplot 2>&1 | tee "$RESULT_FILE"

echo ""
echo "=== Results saved to: $RESULT_FILE ==="

# Show summary 显示摘要
echo ""
echo "=== Summary ==="
grep -E "^(put|get|range|mixed)" "$RESULT_FILE" | head -20 || true
