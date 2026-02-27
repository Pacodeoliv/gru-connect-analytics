#!/usr/bin/env bash
# =============================================================================
# Backfill Histórico — GRU Connect Analytics
# =============================================================================
# Ingere múltiplos meses do VRA da ANAC na camada Bronze (Iceberg).
# Idempotente: re-rodar o mesmo mês não duplica dados (upsert por nr_ano/nr_mes).
#
# Uso:
#   bash scripts/backfill.sh                    # 2024 e 2025 completos
#   bash scripts/backfill.sh 2024               # apenas 2024
#   bash scripts/backfill.sh 2025 01 06         # 2025, meses 01 a 06
#
# Pré-requisito:
#   export GRU_BASE_DIR=$(pwd)
# =============================================================================

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
    MESES+=("$(printf '%02d' $M)")
done

TOTAL_JOBS=$(( ${#ANOS[@]} * ${#MESES[@]} ))
JOB=0
SUCESSO=0
FALHA=0

echo "════════════════════════════════════════════════════"
echo "  GRU Connect Analytics — Backfill Histórico"
echo "  Anos: ${ANOS[*]}"
echo "  Meses: ${MESES[*]}"
echo "  Total de jobs: $TOTAL_JOBS"
echo "  GRU_BASE_DIR: $GRU_BASE_DIR"
echo "════════════════════════════════════════════════════"

for ANO in "${ANOS[@]}"; do
    for MES in "${MESES[@]}"; do
        JOB=$((JOB + 1))
        echo ""
        echo "[$JOB/$TOTAL_JOBS] Ingerindo VRA $ANO/$MES..."

        if poetry run python spark_jobs/ingestion_vra.py --ano "$ANO" --mes "$MES"; then
            SUCESSO=$((SUCESSO + 1))
            echo "  ✅ VRA $ANO/$MES — OK"
        else:
            FALHA=$((FALHA + 1))
            echo "  ⚠️  VRA $ANO/$MES — FALHOU (arquivo pode não existir ainda na ANAC)"
        fi
    done
done

echo ""
echo "════════════════════════════════════════════════════"
echo "  Backfill concluído!"
echo "  Sucesso: $SUCESSO | Falha: $FALHA | Total: $TOTAL_JOBS"
echo "════════════════════════════════════════════════════"
echo ""
echo "Para inspecionar os dados consolidados:"
echo "  GRU_BASE_DIR=\$GRU_BASE_DIR python spark_jobs/inspect_gold.py"
