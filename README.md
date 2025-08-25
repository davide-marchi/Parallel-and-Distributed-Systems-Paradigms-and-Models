# Parallel and Distributed Systems - Project 1 (MergeSort)

Distributed out-of-core MergeSort in C++17 using `mmap`. The project implements a common I/O pipeline (index build → index sort → rewrite) across:
- **Sequential** (baseline)
- **OpenMP** (task-based mergesort)
- **FastFlow** (farm: emitter + workers)
- **MPI + OpenMP** (one rank per node; pairwise distributed merges)

The repo includes runnable scripts for experiments, a Jupyter notebook for plotting, and a PDF report. Results and logs are saved to folders so runs are reproducible.

---

## Repository layout

```
.
├── analysis/                       
│   ├── seq_omp_ff_composites.ipynb # Notebook for plots/analysis
│   └── plots_seq_omp_ff/           # Plots saved by the notebook
├── report/
│   ├── report.pdf                  # Compiled report
│   └── imgs/                       # Images used in the report
├── scripts/                        # Helper scripts for sweeps
│   ├── run_array_any.sh            # Single-node: seq / omp / ff
│   └── run_array_mpi.sh            # Multi-node: MPI+OMP
├── results/                        # CSV output (created at run time)
├── logs/                           # Logs from runs (created at run time)
├── fastflow/
│   └── ff/                         # FastFlow
│       └── mapping_string.sh
├── bin/                         # Executables (created by make)
├── Makefile
├── utils.hpp
├── sequential_seq_mmap.cpp
├── omp_mmap.cpp
├── ff_mmap.cpp
├── mpi_omp_mmap.cpp
└── README.md
```

---

## Build

### Requirements
- GCC or Clang with C++17 and OpenMP
- MPI toolchain (Open MPI or MPICH) for the MPI target
- Linux with `mmap`
- FastFlow headers (cloned locally as shown below)

### FastFlow setup
Clone the official FastFlow repository **in this project directory** so it sits alongside the sources:
```bash
git clone https://github.com/fastflow/fastflow.git fastflow
```
Before using the FastFlow executable, run the CPU mapping helper:
```bash
bash ff/mapping_string.sh
```
It prints a mapping string and guidance you can apply to match your machine topology.

### Compile
```bash
make                 # builds all targets into ./bin
```
Targets produced:
- `bin/sequential_seq_mmap`
- `bin/omp_mmap`
- `bin/ff_mmap`
- `bin/mpi_omp_mmap`

### Clean
```bash
make clean           # remove objects
make distclean       # also remove ./bin and build artifacts
```

---

## Command-line parameters

All binaries print help with `-h`. Example:
```
bin/omp_mmap -h
Usage: bin/omp_mmap [options]
  -n, --records N      number of records (default 1e6)
  -p, --payload B      maximum payload size in bytes (default 256)
  -t, --threads T      threads to use (0 = hw concurrency)
  -c, --cutoff  N      task cutoff size    (default 10000)
  -h, --help           show this help
```
(The same flags apply to the other executables. For the MPI+OMP binary, `-t` selects **threads per rank**.)

---

## Running on Slurm with `srun`

Create output folders once:
```bash
mkdir -p results logs
```

### Single-node examples (`srun -n 1`)
```bash
# Sequential baseline
srun -n 1 bin/sequential_seq_mmap -n 100000000 -p 32

# OpenMP on 16 threads
srun -n 1 --cpus-per-task=16 bin/omp_mmap -n 100000000 -p 32 -t 16 -c 10000

# FastFlow on 16 threads (1 emitter + 15 workers)
# Run ff/mapping_string.sh beforehand to get a good CPU mapping.
srun -n 1 --cpus-per-task=16 bin/ff_mmap -n 100000000 -p 32 -t 16 -c 10000
```

### Multi-node MPI + OMP (`srun --mpi=pmix`)
One rank per node (example: 4 nodes, 16 threads per rank):
```bash
srun --mpi=pmix -N 4 -n 4 --cpus-per-task=16 bin/mpi_omp_mmap -n 100000000 -p 32 -t 16 -c 10000
```
(Alternatively: `-N 4 --ntasks-per-node=1`.)

---

## Scripts for testing

Run the sweep scripts with just the binary path; outputs go to `results/` and logs to `logs/` by default.

```bash
# Single-node sweeps (seq / omp / ff)
bash scripts/run_array_any.sh -bin bin/omp_mmap

# Multi-node sweeps (MPI + OMP)
bash scripts/run_array_mpi.sh -bin bin/mpi_omp_mmap
```
Each script defines default grids for records, payloads, threads, and trials. Override them by passing the appropriate flags/environment variables (see the script header).

---

## Plots and analysis

The plotting notebook is at `analysis/seq_omp_ff_composites.ipynb`. At the top, a boolean variable toggles whether to include write-time in the figures. Running the notebook saves composite figures under `imgs/`.
