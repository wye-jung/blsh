#!/bin/bash
# Grid Search 최적화

cd /home/wye/workspace/blsh

LOG_DIR="$HOME/.blsh/logs"
mkdir -p "$LOG_DIR"

uv run python -m wye.blsh.domestic.optimize.grid_search 2>&1 > "$LOG_DIR/optimize.log"
