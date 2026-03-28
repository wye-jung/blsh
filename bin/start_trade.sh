#!/bin/bash
# 트레이더 실행 (uv run이 .venv 자동 감지)
cd /home/wye/workspace/blsh
uv run python -m wye.blsh 2>&1
