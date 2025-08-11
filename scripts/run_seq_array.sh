#!/usr/bin/env bash
#
# run_seq_array.sh
#
# Default behavior:
#   ./run_seq_array.sh
#   -> computes TOTAL tasks and submits itself as a Slurm array with %1 (one at a time).
#
# Override concurrency:
#   ./run_seq_array.sh --max-parallel 8
#
# Extra sbatch args (after --):
#   ./run_seq_array.sh --max-parallel 4 -- --partition=normal --time=00:20:00
#
# Debug a single worker:
#   export SLURM_ARRAY_TASK_ID=0
#   ./run_seq_array.sh --worker
#
# Slurm defaults for worker tasks:
#SBATCH --job-name=seq-grid
#SBATCH --cpus-per-task=1
#SBATCH --time=00:30:00
#SBATCH --output=logs/seq_%A_%a.out
#SBATCH --error=logs/seq_%A_%a.err

set -euo pipefail

# --------------------- Sweep parameters (edit as needed) ---------------------
TRIALS=1
RECORDS=(10000 100000 1000000 10000000 100000000)
PAYLOAD_MAX=(8 16 32 64 128)
# ---------------------------------------------------------------------------

SCRIPT_PATH="$(readlink -f "$0")"

mode="submit"
max_parallel="1"   # default max concurrency
extra_sbatch_args=()

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker) mode="worker"; shift ;;
    --max-parallel) max_parallel="${2:?}"; shift 2 ;;
    --) shift; extra_sbatch_args=("$@"); break ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

calc_total() {
  local num_r=${#RECORDS[@]}
  local num_p=${#PAYLOAD_MAX[@]}
  echo $(( TRIALS * num_r * num_p ))
}

# --------------------- Submit mode (default) ---------------------
if [[ "$mode" == "submit" ]]; then
  mkdir -p results logs
  TOTAL="$(calc_total)"
  [[ "$TOTAL" -gt 0 ]] || { echo "TOTAL=$TOTAL – nothing to run."; exit 1; }
  echo "Submitting array: 0-$((TOTAL-1))%${max_parallel}  (TOTAL=${TOTAL})"
  sbatch --array=0-$((TOTAL-1))%${max_parallel} \
         "${extra_sbatch_args[@]}" \
         "$SCRIPT_PATH" --worker
  exit 0
fi

# --------------------- Worker mode ---------------------
: "${SLURM_ARRAY_TASK_ID:?SLURM_ARRAY_TASK_ID not set}"

NUM_R=${#RECORDS[@]}
NUM_P=${#PAYLOAD_MAX[@]}
COMB=$(( NUM_R * NUM_P ))
t=$(( SLURM_ARRAY_TASK_ID / COMB ))
rem=$(( SLURM_ARRAY_TASK_ID % COMB ))
ri=$(( rem / NUM_P ))
pi=$(( rem % NUM_P ))

trial=$(( t + 1 ))
n=${RECORDS[$ri]}
p=${PAYLOAD_MAX[$pi]}

BIN=./bin/sequential_seq_mmap
[[ -x "$BIN" ]] || { echo "ERROR: $BIN not found or not executable."; exit 2; }

mkdir -p results logs
OUTCSV=results/seq_mmap.csv

echo "Task ${SLURM_ARRAY_TASK_ID}: trial=$trial N=$n PAYLOAD_MAX=$p" >&2

# Run program via srun
OUT="$(
  srun --exclusive -N1 -n1 --cpus-per-task=1 --cpu-bind=core \
       "$BIN" -n "$n" -p "$p"
)"

# Extract timings
build="$(echo "$OUT"   | grep -m1 'build_index'    | grep -oE '[0-9]+(\.[0-9]+)?' || echo 0)"
sort="$(echo "$OUT"    | grep -m1 'sort_records'   | grep -oE '[0-9]+(\.[0-9]+)?' || echo 0)"
rewrite="$(echo "$OUT" | grep -m1 'rewrite_sorted' | grep -oE '[0-9]+(\.[0-9]+)?' || echo 0)"
total="$(echo "$OUT"   | grep -m1 'total_time'     | grep -oE '[0-9]+(\.[0-9]+)?' || echo 0)"

# Atomically append to CSV (lock the CSV itself; no .lock file needed)
flock -x "$OUTCSV" -c '
  if [[ ! -s "'"$OUTCSV"'" ]]; then
    printf "trial,records,payload_max,build_index_s,sort_records_s,rewrite_sorted_s,total_time_s\n" > "'"$OUTCSV"'"
  fi
  printf "%d,%d,%d,%.3f,%.3f,%.3f,%.3f\n" \
    '"$trial"' '"$n"' '"$p"' '"$build"' '"$sort"' '"$rewrite"' '"$total"' >> "'"$OUTCSV"'"
'

echo "Done task ${SLURM_ARRAY_TASK_ID}" >&2
