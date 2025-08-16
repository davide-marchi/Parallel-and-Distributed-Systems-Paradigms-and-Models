#include "utils.hpp"


// Main
int main(int argc, char** argv)
{
    Params opt = parse_argv(argc, argv);

    // Phase 1 - streaming generation
    BENCH_START(generate_unsorted);
    std::string unsorted_file = generate_unsorted_file_mmap(opt.n_records, opt.payload_max);
    BENCH_STOP(generate_unsorted);

    // Phase 2 - build index
    BENCH_START(reading_and_sorting);
    IndexRec*   idx   = build_index_mmap(unsorted_file, opt.n_records);

    // Phase 3 - sort index in RAM
    sort_records(idx, opt.n_records);
    BENCH_STOP(reading_and_sorting);

    // Phase 4 - rewrite sorted file
    BENCH_START(writing);
    rewrite_sorted_mmap(unsorted_file, "files/sorted_"
                     + std::to_string(opt.n_records) + "_"
                     + std::to_string(opt.payload_max) + ".bin", idx, opt.n_records);
    BENCH_STOP(writing);

    // Phase 5 - verify
    BENCH_START(check_if_sorted);
    check_if_sorted_mmap("files/sorted_"
                     + std::to_string(opt.n_records) + "_"
                     + std::to_string(opt.payload_max) + ".bin", opt.n_records);
    BENCH_STOP(check_if_sorted);

    return 0;
}
