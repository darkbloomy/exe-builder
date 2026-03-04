#!/bin/bash
# Build script for Sick Leave Processor
# Run this on the target OS to produce a native executable.
#
# Usage:
#   pip install -r requirements.txt
#   bash build.sh

pyinstaller \
  --onefile \
  --windowed \
  --name "SickLeaveProcessor" \
  --noconfirm \
  sick_leave_gui.py
