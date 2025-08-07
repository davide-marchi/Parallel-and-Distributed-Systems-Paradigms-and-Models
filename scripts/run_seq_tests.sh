#!/usr/bin/env bash
#
# run_seq_tests.sh
#
# Usage: sbatch [SBATCH ARGS] -- ./run_seq_tests.sh <output.csv>
# Example: sbatch --cpus-per-task=1 --time=02:00:00 --output=logs/seq_%j.out \
#                --error=logs/seq_%j.err -- ./run_seq_tests.sh results/seq_mmap.csv

OUTPUT_FILE="$1"
shift

TRIALS=1
RECORDS=(10000 100000 1000000) # 10000000 100000000(?)  -> bin/sequential_seq_mmap -n 100000000 -p 128 -> 45s + 12s + 727s
PAYLOAD_MAX=(8 16 32) # 64 128 256(?)

# initialize CSV (only on the first SLURM step)
if [[ -z "$SLURM_STEP_ID" || "$SLURM_STEP_ID" -eq 0 ]]; then
  echo "trial,n_records,payload_max,build_index_ms,rewrite_sorted_ms" > "$OUTPUT_FILE"
fi

for trial in $(seq 1 $TRIALS); do
  for n in "${RECORDS[@]}"; do
    for p in "${PAYLOAD_MAX[@]}"; do
      echo ">> Trial $trial, N=$n, P=$p"
      # each srun is a separate Slurm job‐step, but they’ll run one after the other
      OUT=$(srun --exclusive -N1 -n1 \
            ./sequential_seq_mmap.a -n "$n" -p "$p")
      build=$(echo "$OUT"  | grep 'build_index'    | grep -o '[0-9.]\+')
      sort=$(echo "$OUT"  | grep 'sort_records'    | grep -o '[0-9.]\+')
      rewrite=$(echo "$OUT" | grep 'rewrite_sorted' | grep -o '[0-9.]\+')
      total=$(echo "$OUT" | grep 'total_time' | grep -o '[0-9.]\+')

      printf "%d,%d,%d,%.3f,%.3f,%.3f,%.3f\n" \
        "$trial" "$n" "$p" "$build" "$sort" "$rewrite" "$total" \
        >> "$OUTPUT_FILE"
    done
  done
done
