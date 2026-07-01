#!/usr/bin/env bash
#
# mktestdirs.sh — create scratch directories used by ScorpioFS local FUSE/overlay
# experiments and ad-hoc manual tests. This is NOT a deployment or service
# initialization script; it only makes empty local directories under the current
# working directory.
#
# (Renamed from the old root-level `init.bash`, whose name wrongly implied it
# initialized the system or service.)
set -euo pipefail

mkdir -p ./dictest
mkdir -p ./lower/a ./lower/b ./lower/c ./lower/d
mkdir -p ./upper/e
mkdir -p ./workerdir
mkdir -p ./true_temp

echo "created local test directories: dictest, lower/{a,b,c,d}, upper/e, workerdir, true_temp"
