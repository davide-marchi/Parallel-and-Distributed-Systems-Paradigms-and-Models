#!/usr/bin/env bash
#
# run_ff_array.sh
#
# Default:
#   ./run_ff_array.sh            # submits one array per thread count (%1)
# Faster:
#   ./run_ff_array.sh --max-parallel 4
# Debug one worker:
#   export SLURM_ARRAY_TASK_ID=0
#   ./run_ff_array.sh --worker --threads-fixed 8

#SBATCH --job-name=ff-grid
#SBATCH --cpus-per-task=1
#SBATCH --time=00:20:00
#SBATCH --output=logs/ff_%A_%a.out
#SBATCH --error=logs/ff_%A_%a.err

set -euo pipefail

# --------------------- Sweep parameters ---------------------
TRIALS=1
RECORDS=(10000 100000 1000000 10000000 100000000)
PAYLOAD_MAX=(8 16 32 64 128)
CUTOFFS=(1000 10000 100000)
THREADS=(1 2 4 8 16 32)   # same advice: avoid 0 on cluster
MAX_CPUS_FOR_T0=32
BIN=./bin/fastflow_seq_mmap  # adjust if needed
OUTCSV=results/ff_mmap.csv
# ------------------------------------------------------------

SCRIPT_PATH="$(readlink -f "$0")"

mode="submit"
max_parallel="1"
threads_fixed=""
extra_sbatch_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker) mode="worker"; shift ;;
    --threads-fixed) threads_fixed="${2:?}"; shift 2 ;;
    --max-parallel) max_parallel="${2:?}"; shift 2 ;;
    --) shift; extra_sbatch_args=("$@"); break ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

calc_total_per_thread() {
  local nr=${#RECORDS[@]} np=${#PAYLOAD_MAX[@]} nc=${#CUTOFFS[@]}
  echo $(( TRIALS * nr * np * nc ))
}

if [[ "$mode" == "submit" ]]; then
  mkdir -p results logs
  total_each="$(calc_total_per_thread)"
  [[ "$total_each" -gt 0 ]] || { echo "Nothing to run."; exit 1; }

  for t in "${THREADS[@]}"; do
    alloc_cpus="$t"
    if [[ "$t" == "0" ]]; then
      alloc_cpus="$MAX_CPUS_FOR_T0"
    fi
    echo "Submitting FF array for T=$t : 0-$((total_each-1))%${max_parallel}  (cpus-per-task=${alloc_cpus})"

    sbatch --array=0-$((total_each-1))%${max_parallel} \
           --cpus-per-task="${alloc_cpus}" \
           "${extra_sbatch_args[@]}" \
           "$SCRIPT_PATH" --worker --threads-fixed "$t"
  done
  exit 0
fi

# --------------------- Worker mode ---------------------
: "${SLURM_ARRAY_TASK_ID:?SLURM_ARRAY_TASK_ID not set}"
[[ -n "$threads_fixed" ]] || { echo "worker: --threads-fixed is required"; exit 2; }

[[ -x "$BIN" ]] || { echo "ERROR: $BIN not found or not executable."; exit 3; }
mkdir -p results logs

NUM_R=${#RECORDS[@]}
NUM_P=${#PAYLOAD_MAX[@]}
NUM_C=${#CUTOFFS[@]}
COMB=$(( NUM_R * NUM_P * NUM_C ))

ttrial=$(( SLURM_ARRAY_TASK_ID / COMB ))
rem=$(( SLURM_ARRAY_TASK_ID % COMB ))
ri=$(( rem / (NUM_P * NUM_C) ))
rem2=$(( rem % (NUM_P * NUM_C) ))
pi=$(( rem2 / NUM_C ))
ci=$(( rem2 % NUM_C ))

trial=$(( ttrial + 1 ))
n=${RECORDS[$ri]}
p=${PAYLOAD_MAX[$pi]}
c=${CUTOFFS[$ci]}
T="$threads_fixed"

echo "FF task ${SLURM_ARRAY_TASK_ID}: trial=$trial N=$n P=$p CUTOFF=$c T=$T" >&2

OUT="$(
  srun --exclusive -N1 -n1 --cpu-bind=cores "$BIN" \
       -n "$n" -p "$p" -c "$c" -t "$T"
)"

build="$(echo "$OUT"   | grep -m1 'build_index'    | grep -oE '[0-9]+(\.[0-9]+)?' || echo 0)"
sortt="$(echo "$OUT"   | grep -m1 'sort_records'   | grep -oE '[0-9]+(\.[0-9]+)?' || echo 0)"
rewrite="$(echo "$OUT" | grep -m1 'rewrite_sorted' | grep -oE '[0-9]+(\.[0-9]+)?' || echo 0)"
total="$(echo "$OUT"   | grep -m1 'total_time'     | grep -oE '[0-9]+(\.[0-9]+)?' || echo 0)"

flock -x "$OUTCSV" -c '
  if [[ ! -s "'"$OUTCSV"'" ]]; then
    printf "trial,records,payload_max,cutoff,threads,build_index_s,sort_records_s,rewrite_sorted_s,total_time_s\n" > "'"$OUTCSV"'"
  fi
  printf "%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f\n" \
    '"$trial"' '"$n"' '"$p"' '"$c"' '"$T"' \
    '"$build"' '"$sortt"' '"$rewrite"' '"$total"' >> "'"$OUTCSV"'"
'

echo "Done FF task ${SLURM_ARRAY_TASK_ID}" >&2
