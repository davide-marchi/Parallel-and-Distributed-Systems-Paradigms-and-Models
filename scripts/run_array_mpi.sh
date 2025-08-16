#!/usr/bin/env bash
#
# scripts/run_array_mpi.sh
#
# WHAT IT DOES
#   Submits ONE Slurm job array per requested NODES value for your chosen MPI binary.
#   Each array sweeps: TRIALS × RECORDS × PAYLOAD_MAX × CUTOFFS × THREADS.
#   Each task runs exactly 1 MPI rank per node, with T threads per rank.
#   Results append to results/<binary>.csv with file locking.
#
# USAGE
#   bash scripts/run_array_mpi.sh --bin bin/mpi_omp_mmap
#   bash scripts/run_array_mpi.sh --bin bin/mpi_omp_seq_mmap --max-parallel 2
#
# LOGGING
#   Single .out/.err per array (per NODES value): logs/<bin>_N<NODES>_%A.out / .err (append).
#
#SBATCH --job-name=grid
#SBATCH --nodes=1                   # overridden per-array at submit time
#SBATCH --cpus-per-task=1           # overridden per-array to max(THREADS)
#SBATCH --time=00:30:00
#SBATCH --output=logs/any_%A_%a.out # overridden at submit (single file per array)
#SBATCH --error=logs/any_%A_%a.err  # overridden at submit (single file per array)

set -euo pipefail

# ---------------------- EDIT YOUR SWEEP HERE ----------------------
TRIALS=(1)
RECORDS=(100000 1000000 10000000 100000000)
PAYLOAD_MAX=(8 32 128)
CUTOFFS=(10000)
THREADS=(1 2 4 8 16 32)        # threads per MPI rank
NODES=(1 2)                    # we submit one array per value here

# Optional safety: set to your cluster's cores per node (e.g., 32 or 64).
# If >0, tasks with T > CORES_PER_NODE are skipped to avoid oversubscription.
CORES_PER_NODE=0

# Optional MPI plugin for srun. Common values: pmix, pmi2. Leave empty to let Slurm/mpi pick.
MPI_PLUGIN=""
# -----------------------------------------------------------------

SCRIPT_PATH="$(readlink -f "$0")"
mode="submit"
max_parallel="1"               # array throttle within each NODES array
BIN=""                         # e.g., bin/mpi_omp_mmap
nodes_fixed=""                 # passed to workers so they know their NODES value

# ------------------------- ARG PARSING ---------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker) mode="worker"; shift ;;
    --bin)    BIN="${2:-}"; shift 2 ;;
    --max-parallel) max_parallel="${2:?}"; shift 2 ;;
    --nodes-fixed) nodes_fixed="${2:-}"; shift 2 ;;   # internal (worker)
    *) echo "Usage: $0 --bin bin/<mpi-executable> [--max-parallel N]" >&2; exit 1 ;;
  esac
done

[[ -n "$BIN" ]] || { echo "ERROR: --bin is required"; exit 2; }
[[ "$BIN" == */* ]] || BIN="./bin/$BIN"
BIN_BASENAME="$(basename "$BIN")"
OUTCSV="results/${BIN_BASENAME}.csv"

# Numeric max of THREADS (used for cpus-per-task per array)
max_threads="${THREADS[0]}"
for v in "${THREADS[@]}"; do (( v > max_threads )) && max_threads="$v"; done

calc_total_per_nodes() {
  echo $(( ${#TRIALS[@]} * ${#RECORDS[@]} * ${#PAYLOAD_MAX[@]} * ${#CUTOFFS[@]} * ${#THREADS[@]} ))
}

# ------------------------- SUBMIT MODE ---------------------------
if [[ "$mode" == "submit" ]]; then
  mkdir -p results logs
  total_each="$(calc_total_per_nodes)"; [[ "$total_each" -gt 0 ]] || { echo "Nothing to run."; exit 1; }

  prev_jid=""
  for nn in "${NODES[@]}"; do
    echo "Submitting MPI array for ${BIN_BASENAME}: NODES=${nn}, tasks 0-$((total_each-1))%${max_parallel}"

    # Build sbatch command
    cmd=( sbatch
      --parsable                          # echo just the JobID so we can capture it
      --array=0-$((total_each-1))%${max_parallel}
      --nodes="${nn}"
      --cpus-per-task="${max_threads}"
      --job-name="grid"
      --output="logs/${BIN_BASENAME}_N${nn}_%A.out"
      --error="logs/${BIN_BASENAME}_N${nn}_%A.err"
      --open-mode=append
    )

    # If there is a previous array, chain this one after it finished OK
    if [[ -n "$prev_jid" ]]; then
      cmd+=( --dependency="afterok:${prev_jid}" )
    fi

    # Submit and capture the new JobID
    jid="$("${cmd[@]}" "$SCRIPT_PATH" --worker --bin "$BIN" --nodes-fixed "${nn}")"
    echo "Submitted NODES=${nn} as JobID ${jid}"
    prev_jid="$jid"
  done
  exit 0
fi

# ------------------------- WORKER MODE ---------------------------
: "${SLURM_ARRAY_TASK_ID:?SLURM_ARRAY_TASK_ID is not set}"
[[ -x "$BIN" ]] || { echo "ERROR: $BIN not found or not executable."; exit 3; }
: "${nodes_fixed:?missing --nodes-fixed}"  # must be passed from submit loop
mkdir -p results logs

# lengths for mapping idx -> tuple
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

# Optional safety: skip combos that don't fit on the node
if (( CORES_PER_NODE > 0 && T > CORES_PER_NODE )); then
  echo "[task $SLURM_ARRAY_TASK_ID] SKIP: T=$T > CORES_PER_NODE=$CORES_PER_NODE (nodes=$nodes_fixed)" 
  exit 0
fi

# Hybrid: one rank per node, T threads per rank
export OMP_NUM_THREADS="$T"
ranks="$nodes_fixed"     # one rank on each node

echo "[task $SLURM_ARRAY_TASK_ID] bin=$BIN_BASENAME trial=$trial N=$n P=$p C=$c T=$T NODES=$nodes_fixed RANKS=$ranks"

tmplog="$(mktemp)"

# Build srun options (add --mpi if specified)
SRUN_OPTS=( --exclusive -N "$nodes_fixed" --ntasks-per-node=1 --cpus-per-task="$T" --cpu-bind=cores )
[[ -n "$MPI_PLUGIN" ]] && SRUN_OPTS+=( --mpi="$MPI_PLUGIN" )

# launch, capture both stdout+stderr, mirror to .out
if srun "${SRUN_OPTS[@]}" "$BIN" -n "$n" -p "$p" -c "$c" -t "$T" >"$tmplog" 2>&1; then
  : # ok
else
  echo "[task $SLURM_ARRAY_TASK_ID] WARNING: program exited non-zero" >&2
fi
cat "$tmplog"
OUT="$(cat "$tmplog")"
rm -f "$tmplog"

# --------- parse timers + sorted flag (robust to padding before ']') --------
num='[0-9]+(\.[0-9]+)?'
gen_ms="$(echo "$OUT" | grep -m1 -E '\[generate_unsorted[[:space:]]*\]'    | grep -oE "$num" | head -n1 || echo 0)"
rd_ms="$( echo "$OUT" | grep -m1 -E '\[reading[[:space:]]*\]'              | grep -oE "$num" | head -n1 || echo 0)"
rs_ms="$( echo "$OUT" | grep -m1 -E '\[reading_and_sorting[[:space:]]*\]'  | grep -oE "$num" | head -n1 || echo 0)"
wr_ms="$( echo "$OUT" | grep -m1 -E '\[writing[[:space:]]*\]'              | grep -oE "$num" | head -n1 || echo 0)"
chk_ms="$(echo "$OUT" | grep -m1 -E '\[check_if_sorted[[:space:]]*\]'      | grep -oE "$num" | head -n1 || echo 0)"
sorted=0; echo "$OUT" | grep -q 'File is sorted\.' && sorted=1

# ----------------------------- CSV output -----------------------------------
flock -x "$OUTCSV" -c '
  if [[ ! -s "'"$OUTCSV"'" ]]; then
    printf "trial,records,payload_max,cutoff,threads,nodes,total_ranks,generate_unsorted_ms,reading_ms,reading_and_sorting_ms,writing_ms,check_if_sorted_ms,sorted\n" > "'"$OUTCSV"'"
  fi
  printf "%s,%s,%s,%s,%s,%s,%s,%.3f,%.3f,%.3f,%.3f,%.3f,%d\n" \
    "'"$trial"'" "'"$n"'" "'"$p"'" "'"$c"'" "'"$T"'" "'"$nodes_fixed"'" "'"$ranks"'" \
    '"${gen_ms:-0}"' '"${rd_ms:-0}"' '"${rs_ms:-0}"' '"${wr_ms:-0}"' '"${chk_ms:-0}"' '"$sorted"' >> "'"$OUTCSV"'"
'

echo "[task $SLURM_ARRAY_TASK_ID] done → $OUTCSV"
