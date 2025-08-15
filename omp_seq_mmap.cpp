/*  Parallel Merge-Sort (task-parallel)  ─────────────────────────────────────
 *  Uses the data structures, random generator, timers and helpers defined
 *  in utils.hpp.  Compile with  -fopenmp  and run:
 *
 *      ./bin/openmp   -n 1000000  -p 256  -t 8
 *
 *  where -t sets the number of OpenMP threads (0 = auto).
 *  -------------------------------------------------------------------------*/

#include "utils.hpp"
#include <omp.h>


/*-------------------------------------------------------------------------*/
/*  Recursive task-parallel MergeSort                                      */
/*-------------------------------------------------------------------------*/
static inline void mergesort_task(IndexRec* base,
                                  std::size_t left,
                                  std::size_t right,
                                  int cutoff)
{
    if (left >= right) return;
    std::size_t mid = (left + right) / 2;

    if (static_cast<int>(right - left) > cutoff) {
        #pragma omp task shared(base)
        mergesort_task(base, left,  mid,       cutoff);

        #pragma omp task shared(base)
        mergesort_task(base, mid+1, right,     cutoff);

        #pragma omp taskwait

        /* merge the two sorted halves in-place -------------------------------*/
        merge_records(base, left, mid, right); // Use merge_records wrapper
    } else {
        sort_records(base + left, right - left + 1); // Use sort_records for base case
    }
}

//------------------------------------------------------------------------------
//  Main                                                                        
//------------------------------------------------------------------------------
int main(int argc, char** argv)
{
    Params opt = parse_argv(argc, argv);

    // Phase 1 – streaming generation --------------------------------------
    BENCH_START(generate_unsorted);
    std::string unsorted_file = generate_unsorted_file_mmap(opt.n_records, opt.payload_max);
    BENCH_STOP(generate_unsorted);

    // Phase 2 – build index ------------------------------------------------
    BENCH_START(reading_and_sorting);
    IndexRec*   idx   = build_index_mmap(unsorted_file, opt.n_records);

    // Phase 3 – sort index in RAM -----------------------------------------
    if (opt.n_threads > 0)
        omp_set_num_threads(opt.n_threads);
    #pragma omp parallel
    {
        #pragma omp single nowait
        mergesort_task(idx, 0, opt.n_records - 1, opt.cutoff);
    }
    BENCH_STOP(reading_and_sorting);

    // Phase 4 – rewrite sorted file ---------------------------------------
    BENCH_START(writing);
    rewrite_sorted_mmap(unsorted_file, "files/sorted_"
                     + std::to_string(opt.n_records) + "_"
                     + std::to_string(opt.payload_max) + ".bin", idx, opt.n_records);
    BENCH_STOP(writing);

    // Phase 5 – verify -----------------------------------------------------
    BENCH_START(check_if_sorted);
    check_if_sorted_mmap("files/sorted_"
                     + std::to_string(opt.n_records) + "_"
                     + std::to_string(opt.payload_max) + ".bin", opt.n_records);
    BENCH_STOP(check_if_sorted);

    return 0;
}