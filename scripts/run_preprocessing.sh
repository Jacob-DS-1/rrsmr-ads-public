#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export PYTHONPATH="$(pwd)/src${PYTHONPATH:+:$PYTHONPATH}"

python -m rrsmr_ads.preprocessing.runner "$@"
