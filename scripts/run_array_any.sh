#!/usr/bin/env bash
#
# scripts/run_array_any.sh
#
# WHAT IT DOES
#   Submits ONE Slurm job array to run your chosen binary over:
#   TRIALS × RECORDS × PAYLOAD_MAX × CUTOFFS × THREADS
#   Appends a CSV row per task to results/<binary>.csv (file-locked).
#
# USAGE
#   ./scripts/run_array_any.sh --bin bin/sequential_seq_mmap
#   ./scripts/run_array_any.sh --bin bin/openmp_seq_mmap
#   ./scripts/run_array_any.sh --bin bin/fastflow_seq_mmap
#   ./scripts/run_array_any.sh --bin bin/openmp_seq_mmap --max-parallel 4
#
# LOGS
#   Single log per array (not per-task): logs/<binary>_%A.out / .err
#   Note: if you raise --max-parallel > 1, output interleaves in that one file.
#
#SBATCH --job-name=grid
#SBATCH --cpus-per-task=1              # overridden at submit to max(THREADS)
#SBATCH --time=00:20:00
#SBATCH --output=logs/any_%A_%a.out    # overridden at submit (single file)
#SBATCH --error=logs/any_%A_%a.err     # overridden at submit (single file)

set -euo pipefail

# ---------------------- EDIT YOUR SWEEP HERE ----------------------
TRIALS=(1 2 3)
RECORDS=(10000 100000 1000000 10000000 100000000)
PAYLOAD_MAX=(8 16 32 64 128)
CUTOFFS=(1000 10000 100000)
THREADS=(1 2 4 8 16 32)   # We ALWAYS reserve cpus-per-task = max(THREADS)
# -----------------------------------------------------------------

SCRIPT_PATH="$(readlink -f "$0")"
mode="submit"
max_parallel="1"           # array throttle: %1 by default
BIN=""                     # e.g., bin/openmp_seq_mmap

# ------------------------- ARG PARSING ---------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker) mode="worker"; shift ;;
    --bin)    BIN="${2:-}"; shift 2 ;;
    --max-parallel) max_parallel="${2:?}"; shift 2 ;;
    *) echo "Usage: $0 --bin bin/<executable> [--max-parallel N]" >&2; exit 1 ;;
  esac
done

[[ -n "$BIN" ]] || { echo "ERROR: --bin is required"; exit 2; }
[[ "$BIN" == */* ]] || BIN="./bin/$BIN"
BIN_BASENAME="$(basename "$BIN")"
OUTCSV="results/${BIN_BASENAME}.csv"

# Numeric max of THREADS (used for --cpus-per-task)
max_threads="${THREADS[0]}"
for v in "${THREADS[@]}"; do (( v > max_threads )) && max_threads="$v"; done

calc_total() {
  echo $(( ${#TRIALS[@]} * ${#RECORDS[@]} * ${#PAYLOAD_MAX[@]} * ${#CUTOFFS[@]} * ${#THREADS[@]} ))
}

# ------------------------- SUBMIT MODE ---------------------------
if [[ "$mode" == "submit" ]]; then
  mkdir -p results logs
  TOTAL="$(calc_total)"; [[ "$TOTAL" -gt 0 ]] || { echo "Nothing to run."; exit 1; }

  echo "Submitting ONE array for ${BIN_BASENAME}: 0-$((TOTAL-1))%${max_parallel}"
  echo "cpus-per-task = max(THREADS) = ${max_threads}"
  # One .out/.err for the whole array (no %a)
  sbatch \
    --array=0-$((TOTAL-1))%${max_parallel} \
    --cpus-per-task="${max_threads}" \
    --job-name="grid" \
    --output="logs/${BIN_BASENAME}_%A.out" \
    --error="logs/${BIN_BASENAME}_%A.err" \
    "$SCRIPT_PATH" --worker --bin "$BIN"
  exit 0
fi

# ------------------------- WORKER MODE ---------------------------
: "${SLURM_ARRAY_TASK_ID:?SLURM_ARRAY_TASK_ID is not set}"
[[ -x "$BIN" ]] || { echo "ERROR: $BIN not found or not executable."; exit 3; }
mkdir -p results logs

NT=${#TRIALS[@]}
NR=${#RECORDS[@]}
NP=${#PAYLOAD_MAX[@]}
NC=${#CUTOFFS[@]}
NTH=${#THREADS[@]}

COMB_RP=$(( NR * NP ))
COMB_RPC=$(( COMB_RP * NC ))
COMB_RPCT=$(( COMB_RPC * NTH ))

idx=$SLURM_ARRAY_TASK_ID
ti=$(( idx / COMB_RPCT ))
rem=$(( idx % COMB_RPCT ))
ri=$(( rem / (NP * NC * NTH) ))
rem=$(( rem % (NP * NC * NTH) ))
pi=$(( rem / (NC * NTH) ))
rem=$(( rem % (NC * NTH) ))
ci=$(( rem / NTH ))
thi=$(( rem % NTH ))

trial="${TRIALS[$ti]}"
n="${RECORDS[$ri]}"
p="${PAYLOAD_MAX[$pi]}"
c="${CUTOFFS[$ci]}"
T="${THREADS[$thi]}"

# OpenMP binaries: set OMP_NUM_THREADS; harmless for others.
if echo "$BIN_BASENAME" | grep -qiE 'omp|openmp'; then
  export OMP_NUM_THREADS="$T"
fi

echo "Task $SLURM_ARRAY_TASK_ID  bin=$BIN_BASENAME  trial=$trial  N=$n  P=$p  C=$c  T=$T" >&2

OUT="$(
  srun --exclusive -n1 --cpu-bind=cores "$BIN" \
       -n "$n" -p "$p" -c "$c" -t "$T"
)"

# --------------------- TIMERS + SORTED FLAG ----------------------
# Match bracketed labels exactly, e.g. "[reading] 123.456 ms"
num='[0-9]+(\.[0-9]+)?'
gen_ms="$(echo "$OUT" | grep -m1 '\[generate_unsorted\]'    | grep -oE "$num" || echo 0)"
rd_ms="$( echo "$OUT" | grep -m1 '\[reading\]'              | grep -oE "$num" || echo 0)"   # new
rs_ms="$( echo "$OUT" | grep -m1 '\[reading_and_sorting\]'  | grep -oE "$num" || echo 0)"
wr_ms="$( echo "$OUT" | grep -m1 '\[writing\]'              | grep -oE "$num" || echo 0)"
chk_ms="$(echo "$OUT" | grep -m1 '\[check_if_sorted\]'      | grep -oE "$num" || echo 0)"

# Presence of the success line
if echo "$OUT" | grep -q 'File is sorted\.'; then
  sorted=1
else
  sorted=0
fi

# --------------------- CSV (single writer at a time) --------------
flock -x "$OUTCSV" -c '
  if [[ ! -s "'"$OUTCSV"'" ]]; then
    printf "trial,records,payload_max,cutoff,threads,generate_unsorted_ms,reading_ms,reading_and_sorting_ms,writing_ms,check_if_sorted_ms,sorted\n" > "'"$OUTCSV"'"
  fi
  printf "%s,%s,%s,%s,%s,%.3f,%.3f,%.3f,%.3f,%.3f,%d\n" \
    "'"$trial"'" "'"$n"'" "'"$p"'" "'"$c"'" "'"$T"'" \
    '"${gen_ms:-0}"' '"${rd_ms:-0}"' '"${rs_ms:-0}"' '"${wr_ms:-0}"' '"${chk_ms:-0}"' '"$sorted"' >> "'"$OUTCSV"'"
'

echo "Done $SLURM_ARRAY_TASK_ID → $OUTCSV" >&2
