#!/usr/bin/env bash
# Local execution of the same stage sequence Fabric Data Factory orchestrates.
# Useful for iterating without a Fabric capacity; the notebooks are unchanged.
set -euo pipefail

cd "$(dirname "$0")"
export PYTHONPATH=src
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17}"
PY=.venv/bin/python
# Spark launches Python workers with whatever `python3` resolves to, which is the
# system interpreter -- a minor-version mismatch with the driver aborts any stage
# that uses Python workers. Pin both to the venv.
export PYSPARK_PYTHON="$PWD/$PY"
export PYSPARK_DRIVER_PYTHON="$PWD/$PY"
RUN_ID="${RUN_ID:-local$(date +%Y%m%d%H%M%S)}"

# Fabric passes parameters into a notebook; locally we inject them by rewriting
# the parameter cell, which keeps the notebook file itself free of CLI plumbing.
stage() {
  local script="$1"; shift
  local tmp; tmp=$(mktemp /tmp/cas_stage_XXXXXXXX.py)
  cp "notebooks/$script" "$tmp"
  for kv in "$@"; do
    local k="${kv%%=*}" v="${kv#*=}"
    # Match the parameter's initial assignment only (first occurrence).
    /usr/bin/sed -i '' "1,/^${k} = /s|^${k} = .*|${k} = ${v}|" "$tmp"
  done
  echo ""
  echo "=============================================================="
  echo ">> $script  $*"
  echo "=============================================================="
  $PY "$tmp" 2>&1 | grep -vE "WARNING: |Ivy Default|:: loading|:: resolution|:: retrieving|confs: \[|artifacts copied|found [a-z]|downloading https|\[Stage |^\s*$|^\s*\||^\s*-+$|setLogLevel"
  rm -f "$tmp"
}

MANIFEST_DIR=lakehouse/Files/landing/open_payments/_manifest

stage 00_ingest_source_to_files.py "run_id=\"$RUN_ID\""
MANIFEST=$(ls -t $MANIFEST_DIR/*.json | head -1)

stage 01_files_to_bronze.py "run_id=\"$RUN_ID\"" "pipeline=\"bronze_general_payments\""  "manifest_path=\"$MANIFEST\""
stage 03_data_quality_checks.py "run_id=\"$RUN_ID\"" "pipeline=\"bronze_general_payments\""  "layer=\"bronze\""

stage 01_files_to_bronze.py "run_id=\"$RUN_ID\"" "pipeline=\"bronze_research_payments\"" "manifest_path=\"$MANIFEST\""
stage 03_data_quality_checks.py "run_id=\"$RUN_ID\"" "pipeline=\"bronze_research_payments\"" "layer=\"bronze\""

stage 02_bronze_to_silver.py "run_id=\"$RUN_ID\"" "pipeline=\"silver_payments\""
stage 03_data_quality_checks.py "run_id=\"$RUN_ID\"" "pipeline=\"silver_payments\"" "layer=\"silver\""

stage 04_silver_to_gold.py "run_id=\"$RUN_ID\"" "pipeline=\"gold_star_schema\""
stage 03_data_quality_checks.py "run_id=\"$RUN_ID\"" "pipeline=\"gold_star_schema\"" "layer=\"gold\"" "table=\"fact_payment\""

echo ""
echo "Pipeline complete. run_id=$RUN_ID"
