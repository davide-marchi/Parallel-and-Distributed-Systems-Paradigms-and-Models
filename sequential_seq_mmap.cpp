#include "utils.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdio>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <string>


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
    BENCH_START(build_index);
    IndexRec*   idx   = build_index_mmap(unsorted_file, opt.n_records);
    BENCH_STOP(build_index);

    // Phase 3 – sort index in RAM -----------------------------------------
    BENCH_START(sort_records);
    sort_records(idx, opt.n_records);
    BENCH_STOP(sort_records);

    // Phase 4 – rewrite sorted file ---------------------------------------
    BENCH_START(rewrite_sorted);
    rewrite_sorted_mmap(unsorted_file, "files/sorted_"
                     + std::to_string(opt.n_records) + "_"
                     + std::to_string(opt.payload_max) + ".bin", idx, opt.n_records);
    BENCH_STOP(rewrite_sorted);

    // Phase 5 – verify -----------------------------------------------------
    BENCH_START(check_if_sorted);
    check_if_sorted_mmap("files/sorted_"
                     + std::to_string(opt.n_records) + "_"
                     + std::to_string(opt.payload_max) + ".bin", opt.n_records);
    BENCH_STOP(check_if_sorted);
    return 0;
}
