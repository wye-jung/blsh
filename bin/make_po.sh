#!/bin/bash
# PO 생성 (데이터 수집 + 스캔 + PO 파일 생성)
cd /home/wye/workspace/blsh
uv run python -m wye.blsh po 2>&1
