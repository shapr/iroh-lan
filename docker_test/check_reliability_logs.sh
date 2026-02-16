#!/bin/bash
set -euo pipefail

echo "========================================================"
echo "     5-NODE RELIABILITY TEST REPORT"
echo "========================================================"

overall_fail=0
phases=(baseline degraded post_rolling post_simultaneous post_outage_recovery recovery)

for node in 0 1 2 3 4; do
    base="docker_test/results_reliability/node${node}"
    latest=$(ls -td "$base"/*/ 2>/dev/null | head -n1 || true)

    if [ -z "$latest" ]; then
        echo "[FAIL] node${node}: no log directory found"
        overall_fail=1
        continue
    fi

    echo "node${node}: $latest"

    for phase in "${phases[@]}"; do
        log="$latest/mesh_${phase}.log"
        if [ ! -f "$log" ]; then
            echo "  [FAIL] ${phase}: missing log"
            overall_fail=1
            continue
        fi

        if grep -q "MESH_CHECK: PASS" "$log"; then
            echo "  [PASS] ${phase}"
        else
            echo "  [FAIL] ${phase}"
            tail -n 20 "$log" || true
            overall_fail=1
        fi
    done

done

echo "========================================================"
if [ "$overall_fail" -eq 0 ]; then
    echo "RELIABILITY RESULT: PASS"
    exit 0
else
    echo "RELIABILITY RESULT: FAIL"
    exit 1
fi
