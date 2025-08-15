/*  OpenMP Merge-Sort with overlapped index-building (simple locking)
 *  - Overlaps progressive index build with task-parallel mergesort.
 *  - Requires your utils.hpp as-is (no changes).
 *  - Compile: g++ -O3 -std=c++17 -fopenmp -o bin/omp_mmap omp_mmap.cpp
 */

#include "utils.hpp"
#include <omp.h>

/* --------------------- mergesort tasks with gating ----------------------- */
static inline void mergesort_task(IndexRec* base,
                                  std::size_t left,
                                  std::size_t right,
                                  int cutoff,
                                  ProgressGate* gate)
{
  if (left >= right) return;

  const std::size_t mid = (left + right) / 2;

  if (static_cast<int>(right - left) > cutoff) {
    #pragma omp task shared(base, gate)
    mergesort_task(base, left,  mid,   cutoff, gate);

    #pragma omp task shared(base, gate)
    mergesort_task(base, mid+1, right, cutoff, gate);

    #pragma omp taskwait

    // No extra wait here: both children already waited before sorting.
    merge_records(base, left, mid, right);
  } else {
    // Leaf work: wait until our whole slice is available, then sort it.
    gate->wait_until(right + 1);
    sort_records(base + left, right - left + 1);
  }
}

/* --------------------------------- main ---------------------------------- */
int main(int argc, char** argv)
{
  Params opt = parse_argv(argc, argv);
  if (opt.n_threads > 0) omp_set_num_threads(opt.n_threads);

  BENCH_START(total_time);

  // 1) Generate unsorted file (unchanged)
  BENCH_START(generate);
  std::string unsorted_file = generate_unsorted_file_mmap(opt.n_records, opt.payload_max);
  BENCH_STOP(generate);

  // 2+3) Overlap index build and mergesort
  IndexRec* idx = static_cast<IndexRec*>(std::malloc(opt.n_records * sizeof(IndexRec)));
  if (!idx) { std::perror("malloc"); std::exit(1); }

  BENCH_START(index_plus_sort);

  {
    

    #pragma omp parallel
    {
      #pragma omp single
      {

        ProgressGate gate;
        gate.reset();

        // A) Progressive index builder (wake every opt.cutoff records)
        #pragma omp task shared(idx, gate)
        build_index_mmap(unsorted_file, idx, opt.n_records, opt.cutoff, &gate);

        // B) Mergesort on the index with readiness gating
        #pragma omp task shared(idx, gate)
        mergesort_task(idx, 0, opt.n_records - 1, opt.cutoff, &gate);

        #pragma omp taskwait
      }
    }
  }

  BENCH_STOP(index_plus_sort);

  // 4) Rewrite sorted file (unchanged; rewrite_sorted_mmap frees idx)
  BENCH_START(rewrite_sorted);
  const std::string sorted_file =
      "files/sorted_" + std::to_string(opt.n_records) + "_"
                       + std::to_string(opt.payload_max) + ".bin";
  rewrite_sorted_mmap(unsorted_file, sorted_file, idx, opt.n_records);
  BENCH_STOP(rewrite_sorted);

  // 5) Verify (unchanged; will also unlink the sorted file in your helpers)
  BENCH_START(check_if_sorted);
  check_if_sorted_mmap(sorted_file, opt.n_records);
  BENCH_STOP(check_if_sorted);

  BENCH_STOP(total_time);
  return 0;
}
