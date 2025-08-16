#!/usr/bin/env bash
#
# scripts/run_array_any.sh
#
# ONE script to launch a single Slurm job array for ANY of your executables:
#   --bin bin/sequential_seq_mmap
#   --bin bin/openmp_seq_mmap
#   --bin bin/fastflow_seq_mmap
#
# It sweeps TRIALS × RECORDS × PAYLOAD_MAX × CUTOFFS × THREADS and appends
# a row per task into results/<binary>.csv (with file locking).
#
# Examples:
#   ./scripts/run_array_any.sh --bin bin/sequential_seq_mmap
#   ./scripts/run_array_any.sh --bin bin/openmp_seq_mmap --max-parallel 4
#
# Logging:
#   Single log per array: logs/<binary>_%A.out and logs/<binary>_%A.err
#   (We also force --open-mode=append so earlier tasks aren't overwritten.)
#
#SBATCH --job-name=grid
#SBATCH --cpus-per-task=1              # overridden at submit to max(THREADS)
#SBATCH --time=00:30:00
#SBATCH --output=logs/any_%A_%a.out    # overridden at submit (single .out)
#SBATCH --error=logs/any_%A_%a.err     # overridden at submit (single .err)

set -euo pipefail

# ---------------------- EDIT YOUR SWEEP HERE ----------------------
TRIALS=(1)
RECORDS=(100000 1000000 10000000 100000000)
PAYLOAD_MAX=(8 32 128)
CUTOFFS=(10000)
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

  # One .out/.err for the whole array, append, fixed job-name=grid
  sbatch \
    --array=0-$((TOTAL-1))%${max_parallel} \
    --cpus-per-task="${max_threads}" \
    --job-name="grid" \
    --output="logs/${BIN_BASENAME}_%A.out" \
    --error="logs/${BIN_BASENAME}_%A.err" \
    --open-mode=append \
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

# --- status line to STDOUT (so it lands in the single .out file) ---
echo "[task $SLURM_ARRAY_TASK_ID] bin=$BIN_BASENAME  trial=$trial  N=$n  P=$p  C=$c  T=$T"

# --- run the program; capture output AND also print it to .out ---
tmplog="$(mktemp)"
# capture both stdout+stderr to a temp file
if srun --exclusive -n1 --cpu-bind=cores "$BIN" -n "$n" -p "$p" -c "$c" -t "$T" >"$tmplog" 2>&1; then
  : # ok
else
  echo "[task $SLURM_ARRAY_TASK_ID] WARNING: program exited non-zero" >&2
fi
# show the program output in the .out file (appended as we forced --open-mode=append)
cat "$tmplog"
# also keep it in a variable for parsing
OUT="$(cat "$tmplog")"
rm -f "$tmplog"

# --------------------- TIMERS + SORTED FLAG ----------------------
# Match bracketed labels exactly, e.g. "[reading] 123.456 ms"
# number regex
num='[0-9]+(\.[0-9]+)?'
gen_ms="$(echo "$OUT" | grep -m1 -E '\[generate_unsorted[[:space:]]*\]'    | grep -oE "$num" | head -n1 || echo 0)"
rd_ms="$( echo "$OUT" | grep -m1 -E '\[reading[[:space:]]*\]'              | grep -oE "$num" | head -n1 || echo 0)"
rs_ms="$( echo "$OUT" | grep -m1 -E '\[reading_and_sorting[[:space:]]*\]'  | grep -oE "$num" | head -n1 || echo 0)"
wr_ms="$( echo "$OUT" | grep -m1 -E '\[writing[[:space:]]*\]'              | grep -oE "$num" | head -n1 || echo 0)"
chk_ms="$(echo "$OUT" | grep -m1 -E '\[check_if_sorted[[:space:]]*\]'      | grep -oE "$num" | head -n1 || echo 0)"

# Presence of the success line
sorted=0
echo "$OUT" | grep -q 'File is sorted\.' && sorted=1

# --------------------- CSV (single writer at a time) --------------
flock -x "$OUTCSV" -c '
  if [[ ! -s "'"$OUTCSV"'" ]]; then
    printf "trial,records,payload_max,cutoff,threads,generate_unsorted_ms,reading_ms,reading_and_sorting_ms,writing_ms,check_if_sorted_ms,sorted\n" > "'"$OUTCSV"'"
  fi
  printf "%s,%s,%s,%s,%s,%.3f,%.3f,%.3f,%.3f,%.3f,%d\n" \
    "'"$trial"'" "'"$n"'" "'"$p"'" "'"$c"'" "'"$T"'" \
    '"${gen_ms:-0}"' '"${rd_ms:-0}"' '"${rs_ms:-0}"' '"${wr_ms:-0}"' '"${chk_ms:-0}"' '"$sorted"' >> "'"$OUTCSV"'"
'

echo "[task $SLURM_ARRAY_TASK_ID] done  → $OUTCSV"
