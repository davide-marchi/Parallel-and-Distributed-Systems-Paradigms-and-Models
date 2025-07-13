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
#include <memory>     // std::unique_ptr
#include <cstring>    // std::memcpy


/*-------------------------------------------------------------------------*/
/*  Recursive task-parallel MergeSort                                      */
/*-------------------------------------------------------------------------*/
static void mergesort_task(Record* base,
                           Record* aux,
                           std::size_t left,
                           std::size_t right,
                           int cutoff)
{
    if (left >= right) return;

    std::size_t mid = (left + right) / 2;

    if (static_cast<int>(right - left) > cutoff) {
        #pragma omp task shared(base, aux)
        mergesort_task(base, aux, left, mid, cutoff);

        #pragma omp task shared(base, aux)
        mergesort_task(base, aux, mid + 1, right, cutoff);

        #pragma omp taskwait
    } else {
        mergesort_task(base, aux, left, mid, cutoff);
        mergesort_task(base, aux, mid + 1, right, cutoff);
    }

    merge_into_dest(base + left,  mid - left + 1, base + mid + 1, right - mid, base + left);
}

/*-------------------------------------------------------------------------*/
/*  Main driver                                                            */
/*-------------------------------------------------------------------------*/
int main(int argc, char** argv)
{
    Params opt = parse_argv(argc, argv);

    /* honour -t <threads> if given */
    if (opt.n_threads > 0)
        omp_set_num_threads(opt.n_threads);

    /* generate input */
    Record* data = alloc_random_records(opt.n_records, opt.payload_max);

    /* one auxiliary buffer reused by every merge */
    std::unique_ptr<Record[]> aux(new Record[opt.n_records]);

    const int cutoff = 10'000;     // task-creation threshold

    BENCH_START(parallel_merge_sort);
    #pragma omp parallel
    {
        #pragma omp single nowait
        mergesort_task(data, aux.get(), 0, opt.n_records - 1, cutoff);
    }
    BENCH_STOP(parallel_merge_sort);

    check_if_sorted(data, opt.n_records);

    release_records(data, opt.n_records);
    return 0;
}
