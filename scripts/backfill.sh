#!/usr/bin/env bash
# Ingest multiple VRA months into the Bronze Iceberg table.
# Idempotent: re-running the same month does not duplicate data.
#
# Usage:
#   bash scripts/backfill.sh                   # 2024 and 2025 full
#   bash scripts/backfill.sh 2024              # 2024 only
#   bash scripts/backfill.sh 2024 2025 01 06   # 2024-2025, months 01-06
#
# Requires: export GRU_BASE_DIR=$(pwd)

set -euo pipefail

export GRU_BASE_DIR="${GRU_BASE_DIR:-$(pwd)}"
ANO_INICIO="${1:-2024}"
ANO_FIM="${2:-2025}"
MES_INICIO="${3:-01}"
MES_FIM="${4:-12}"

ANOS=()
for ((ANO=ANO_INICIO; ANO<=ANO_FIM; ANO++)); do
    ANOS+=("$ANO")
done

MESES=()
for ((M=10#$MES_INICIO; M<=10#$MES_FIM; M++)); do
    MESES+=("$(printf '%02d' "$M")")
done

TOTAL_JOBS=$(( ${#ANOS[@]} * ${#MESES[@]} ))
JOB=0
SUCCESS=0
FAILED=0

echo "GRU Connect Analytics - Backfill"
echo "Years: ${ANOS[*]} | Months: ${MESES[*]} | Total: $TOTAL_JOBS"
echo "GRU_BASE_DIR: $GRU_BASE_DIR"
echo ""

for ANO in "${ANOS[@]}"; do
    for MES in "${MESES[@]}"; do
        JOB=$((JOB + 1))
        echo "[$JOB/$TOTAL_JOBS] Ingesting VRA $ANO/$MES..."

        if poetry run python spark_jobs/ingestion_vra.py --ano "$ANO" --mes "$MES"; then
            SUCCESS=$((SUCCESS + 1))
            echo "  OK: VRA $ANO/$MES"
        else
            FAILED=$((FAILED + 1))
            echo "  SKIP: VRA $ANO/$MES (file may not be available yet)"
        fi
    done
done

echo ""
echo "Backfill complete — success: $SUCCESS | failed: $FAILED | total: $TOTAL_JOBS"
