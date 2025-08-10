#!/usr/bin/env bash
#
# run_ff_array_single.sh
#
# Default:
#   ./run_ff_array_single.sh
#   -> ONE array, %1 concurrency
#
#SBATCH --job-name=ff-grid
#SBATCH --cpus-per-task=32                # reserve max threads for all elements
#SBATCH --time=00:20:00
#SBATCH --output=logs/ff_%A_%a.out
#SBATCH --error=logs/ff_%A_%a.err

set -euo pipefail

TRIALS=3
RECORDS=(10000 100000 1000000 10000000 100000000)
PAYLOAD_MAX=(8 16 32 64 128)
CUTOFFS=(1000 10000 100000)
THREADS=(1 2 4 8 16 32)                  # ≤ --cpus-per-task
BIN=./bin/fastflow_seq_mmap
OUTCSV=results/ff_mmap.csv

SCRIPT_PATH="$(readlink -f "$0")"
mode="submit"; max_parallel="1"; extra_sbatch_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker) mode="worker"; shift ;;
    --max-parallel) max_parallel="${2:?}"; shift 2 ;;
    --) shift; extra_sbatch_args=("$@"); break ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

calc_total(){ echo $(( TRIALS * ${#RECORDS[@]} * ${#PAYLOAD_MAX[@]} * ${#CUTOFFS[@]} * ${#THREADS[@]} )); }

if [[ "$mode" == "submit" ]]; then
  mkdir -p results logs
  TOTAL=$(calc_total); [[ $TOTAL -gt 0 ]] || { echo "Nothing to run."; exit 1; }
  echo "Submitting ONE FF array: 0-$((TOTAL-1))%${max_parallel} (cpus-per-task=32)"
  sbatch --array=0-$((TOTAL-1))%${max_parallel} \
         "${extra_sbatch_args[@]}" \
         "$SCRIPT_PATH" --worker
  exit 0
fi

: "${SLURM_ARRAY_TASK_ID:?}"
[[ -x "$BIN" ]] || { echo "ERROR: $BIN missing"; exit 2; }
mkdir -p results logs

NR=${#RECORDS[@]}; NP=${#PAYLOAD_MAX[@]}; NC=${#CUTOFFS[@]}; NT=${#THREADS[@]}
COMB_RP=$(( NR * NP ))
COMB_RPC=$(( COMB_RP * NC ))

idx=$SLURM_ARRAY_TASK_ID
ttrial=$(( idx / (COMB_RPC * NT) ))
rem=$(( idx % (COMB_RPC * NT) ))
ri=$(( rem / (NP * NC * NT) ))
rem=$(( rem % (NP * NC * NT) ))
pi=$(( rem / (NC * NT) ))
rem=$(( rem % (NC * NT) ))
ci=$(( rem / NT ))
ti=$(( rem % NT ))

trial=$(( ttrial + 1 ))
n=${RECORDS[$ri]}
p=${PAYLOAD_MAX[$pi]}
c=${CUTOFFS[$ci]}
T=${THREADS[$ti]}                          # ≤ 32

echo "FF task $SLURM_ARRAY_TASK_ID: trial=$trial N=$n P=$p CUTOFF=$c T=$T" >&2

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
