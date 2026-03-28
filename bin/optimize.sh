#!/bin/bash

source /home/wye/workspace/blsh/.venv/bin/activate
#/home/wye/.local/bin/uv run python -m wye.blsh.domestic.optimize.grid_search --mode SWING 2>&1 | grep "★" -A 15 > /home/wye/.blsh/config/SWING.dat &
#/home/wye/.local/bin/uv run python -m wye.blsh.domestic.optimize.grid_search --mode DAY 2>&1 | grep "★" -A 15 > /home/wye/.blsh/config/DAY.dat &
/home/wye/.local/bin/uv run python -m wye.blsh.domestic.optimize.grid_search --mode SWING 2>&1 > /home/wye/workspace/blsh/bin/SWING.dat &
/home/wye/.local/bin/uv run python -m wye.blsh.domestic.optimize.grid_search --mode DAY 2>&1 | /home/wye/workspace/blsh/bin/DAY.dat &