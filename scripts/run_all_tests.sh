#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

for pkg in protocol adapter-memory adapter-local adapter-ui flow bridge-openai stream core; do
  echo "==> packages/$pkg"
  python -m pytest "packages/$pkg/tests" -v
done

echo "==> tests/integration"
python -m pytest tests/integration -v