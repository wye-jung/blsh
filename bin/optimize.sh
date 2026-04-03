#!/bin/bash
# Grid Search 최적화

cd /home/wye/workspace/blsh

LOG_DIR="$HOME/.blsh/logs"
mkdir -p "$LOG_DIR"

# --rebuild: 캐시 강제 재빌드 (+~3분). 미지정 시 캐시 범위 불일치(5일 초과)만 자동 재빌드.
$HOME/.local/bin/uv run python -m wye.blsh.domestic.optimize.grid_search --alternating >> "$LOG_DIR/optimize.log" 2>&1
