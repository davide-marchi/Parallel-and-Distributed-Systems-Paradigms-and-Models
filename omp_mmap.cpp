// OpenMP Merge-Sort with overlapped index-building (simple locking)
// Overlaps progressive index build with task-parallel mergesort

#include "utils.hpp"
#include <omp.h>

// Mergesort tasks with gating
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

    // No extra wait here: both children already waited before sorting
    merge_records(base, left, mid, right);
  } else {
    // Leaf work: wait until our whole slice is available, then sort it
    gate->wait_until(right + 1);
    sort_records(base + left, right - left + 1);
  }
}


// Main
int main(int argc, char** argv)
{
  Params opt = parse_argv(argc, argv);
  if (opt.n_threads > 0) omp_set_num_threads(opt.n_threads);

  // 1) Generate unsorted file
  BENCH_START(generate_unsorted);
  std::string unsorted_file = generate_unsorted_file_mmap(opt.n_records, opt.payload_max);
  BENCH_STOP(generate_unsorted);

  BENCH_START(reading_and_sorting);

  // 2+3) Overlap index build and mergesort
  IndexRec* idx = static_cast<IndexRec*>(std::malloc(opt.n_records * sizeof(IndexRec)));
  if (!idx) { std::perror("malloc"); std::exit(1); }

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

  BENCH_STOP(reading_and_sorting);

  // 4) Rewrite sorted file (rewrite_sorted_mmap frees idx)
  BENCH_START(writing);
  const std::string sorted_file =
      "files/sorted_" + std::to_string(opt.n_records) + "_"
                       + std::to_string(opt.payload_max) + ".bin";
  rewrite_sorted_mmap(unsorted_file, sorted_file, idx, opt.n_records);
  BENCH_STOP(writing);

  // 5) Verify
  BENCH_START(check_if_sorted);
  check_if_sorted_mmap(sorted_file, opt.n_records);
  BENCH_STOP(check_if_sorted);

  return 0;
}
