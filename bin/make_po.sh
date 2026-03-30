#!/bin/bash
# PO 생성 (데이터 수집 + 스캔 + PO 파일 생성)
cd /home/wye/workspace/blsh || { echo "[ERROR] 디렉토리 이동 실패"; exit 1; }
$HOME/.local/bin/uv run python -m wye.blsh po 2>&1
