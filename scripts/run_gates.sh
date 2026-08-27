#!/usr/bin/env bash
# Run all Orditect CI gates (stdlib-only, runnable without installing anything).
# Usage: bash scripts/run_gates.sh
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> business-neutrality gate"
python scripts/gates/check_business_neutrality.py

echo "==> import-boundary gate"
python scripts/gates/check_import_boundary.py

echo "==> api-surface gate"
python scripts/gates/check_api_surface.py

echo "==> lua-time-source gate"
python scripts/gates/check_lua_time_source.py

echo "==> error-surface gate"
python scripts/gates/check_error_surface.py

echo "==> schema-vocabulary gate"
python scripts/gates/check_schema_vocabulary.py

echo "==> pin-flip ledger"
python scripts/gates/list_pin_flips.py

echo "ALL GATES PASSED"