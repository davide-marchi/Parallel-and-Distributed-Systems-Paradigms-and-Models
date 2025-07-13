#include "utils.hpp"

int main(int argc, char** argv)
{
    Params opt = parse_argv(argc, argv);

    Record* rec = alloc_random_records(opt.n_records, opt.payload_max);

    BENCH_START(sort_records);
    sort_records(rec, opt.n_records);
    BENCH_STOP(sort_records);

    check_if_sorted(rec, opt.n_records);

    // dump_records(rec, opt.n_records, 5);

    release_records(rec, opt.n_records);
}
