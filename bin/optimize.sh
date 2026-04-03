#!/bin/bash
# Grid Search 최적화

cd /home/wye/workspace/blsh

LOG_DIR="$HOME/.blsh/logs"
mkdir -p "$LOG_DIR"

$HOME/.local/bin/uv run python -m wye.blsh.domestic.optimize.grid_search --alternating --rebuild >> "$LOG_DIR/optimize.log" 2>&1
