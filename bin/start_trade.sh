#!/bin/bash
# 트레이더 실행 (uv run이 .venv 자동 감지)
cd /home/wye/workspace/blsh || { echo "[ERROR] 디렉토리 이동 실패"; exit 1; }
$HOME/.local/bin/uv run python -m wye.blsh start 2>&1
