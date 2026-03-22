#!/bin/bash

/home/wye/.local/bin/uv run python -m wye.blsh.domestic.optimize.grid_search --mode SWING 2>&1 | grep "★" -A 15 > /home/wye/.blsh/config/SWING.dat &
/home/wye/.local/bin/uv run python -m wye.blsh.domestic.optimize.grid_search --mode DAY 2>&1 | grep "★" -A 15 > /home/wye/.blsh/config/DAY.dat &