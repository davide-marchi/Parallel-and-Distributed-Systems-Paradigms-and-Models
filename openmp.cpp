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
static inline void mergesort_task(Record* base,
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

/*-------------------------------------------------------------------------*/
/*  Main driver                                                            */
/*-------------------------------------------------------------------------*/
int main(int argc, char** argv)
{
    Params opt = parse_argv(argc, argv);

    if (opt.n_threads > 0)
        omp_set_num_threads(opt.n_threads);

    Record* data = alloc_random_records(opt.n_records, opt.payload_max);

    const std::size_t cutoff = opt.cutoff;           // task granularity

    BENCH_START(parallel_merge_sort);
    #pragma omp parallel
    {
        #pragma omp single nowait
        mergesort_task(data, 0, opt.n_records - 1, cutoff);
    }
    BENCH_STOP(parallel_merge_sort);

    check_if_sorted(data, opt.n_records);
    release_records(data, opt.n_records);
}
