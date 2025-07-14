#ifndef UTILS_HPP
#define UTILS_HPP

#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <unistd.h>       // getopt / getopt_long
#include <getopt.h>
#include <algorithm>
#include <random>
#include <ctime>
#include <chrono>
#include <memory>     // std::unique_ptr
#include <cstring>    // std::memcpy

/*---------------------------------------------------------------------------*/
/* 1.  Run-time parameters                                                   */
/*---------------------------------------------------------------------------*/
struct Params {
    std::size_t   n_records   = 1'000'000;  // -n
    std::uint32_t payload_max = 256;        // -p
    int           n_threads   = 0;          // -t   (0 => use hw_concurrency)
    std::size_t   cutoff      = 10'000;      // -c   task-size threshold
};

/*---------------------------------------------------------------------------*/
/* 2.  Timing utilities (chrono version, C++-20)                             */
/*---------------------------------------------------------------------------*/
#define BENCH_START(tag) \
    auto __bench_start_##tag = std::chrono::steady_clock::now()

#define BENCH_STOP(tag)                                                        \
    do {                                                                       \
        auto   __bench_end_##tag  = std::chrono::steady_clock::now();          \
        double __bench_ms_##tag   = std::chrono::duration<double,              \
                                  std::milli>(__bench_end_##tag                \
                                  - __bench_start_##tag).count();              \
        std::printf("[%-20s] %10.3f ms\n", #tag, __bench_ms_##tag);            \
    } while (0)

/*---------------------------------------------------------------------------*/
/* 3.  Record definition                                                     */
/*---------------------------------------------------------------------------*/
struct Record {
    unsigned long key;      // sorting key
    std::uint32_t len;      // payload length in bytes
    char*         payload;  // malloc-owned
};

/*---------------------------------------------------------------------------*/
/* 4.  Command-line parsing                                                  */
/*---------------------------------------------------------------------------*/
static inline Params parse_argv(int argc, char** argv)
{
    Params opt;

    static const struct option long_opts[] = {
        {"records",    required_argument, nullptr, 'n'},
        {"payload",    required_argument, nullptr, 'p'},
        {"threads",    required_argument, nullptr, 't'},
        {"cutoff",     required_argument, nullptr, 'c'},
        {"help",       no_argument,       nullptr, 'h'},
        {nullptr,      0,                 nullptr,  0 }
    };

    int c;
    while ((c = getopt_long(argc, argv, "n:p:t:qc h", long_opts, nullptr)) != -1) {
        switch (c) {
            case 'n': opt.n_records   = std::strtoull(optarg, nullptr, 10); break;
            case 'p':
                opt.payload_max = static_cast<std::uint32_t>(std::strtoul(optarg,nullptr,10));
                if (opt.payload_max < 8) {
                    std::fprintf(stderr,
                        "Error: --payload must be ≥ 8 (got %u)\n", opt.payload_max);
                    std::exit(1);
                }
                break;
            case 't': opt.n_threads   = std::atoi(optarg); break;
            case 'c':
                opt.cutoff = std::strtoull(optarg, nullptr, 10);
                if (opt.cutoff == 0) {
                    std::fprintf(stderr, "Error: --cutoff must be > 0\n");
                    std::exit(1);
                }
                break;
            case 'h':
            default:
                std::fprintf(stderr,
                    "Usage: %s [options]\n"
                    "  -n, --records N      number of records (default 1e6)\n"
                    "  -p, --payload B      maximum payload size in bytes (default 256)\n"
                    "  -t, --threads T      threads to use (0 = hw concurrency)\n"
                    "  -c, --cutoff  N      task cutoff size    (default 10000)\n"
                    "  -h, --help           show this help\n", argv[0]);
                std::exit(c == 'h' ? 0 : 1);
        }
    }
    return opt;
}

/*---------------------------------------------------------------------------*/
/* 5.  Data generation & release                                             */
/*---------------------------------------------------------------------------*/
static inline Record* alloc_random_records(std::size_t n, std::uint32_t payload_max,
                                           unsigned int seed = static_cast<unsigned int>(time(nullptr)))
{
    std::mt19937                     rng(seed);
    std::uniform_int_distribution<>  key_gen(0, INT32_MAX);
    std::uniform_int_distribution<>  len_gen(8, payload_max);
    std::uniform_int_distribution<>  byte_gen(0, 255);

    Record* rec = static_cast<Record*>(std::malloc(n * sizeof(Record)));
    if (!rec) { std::perror("malloc"); std::exit(1); }

    for (std::size_t i = 0; i < n; ++i) {
        rec[i].key = static_cast<unsigned long>(key_gen(rng));
        rec[i].len = static_cast<std::uint32_t>(len_gen(rng));
        rec[i].payload = static_cast<char*>(std::malloc(rec[i].len));
        if (!rec[i].payload) { std::perror("malloc"); std::exit(1); }

        for (std::uint32_t j = 0; j < rec[i].len; ++j)
            rec[i].payload[j] = static_cast<char>(byte_gen(rng));
    }
    return rec;
}

static inline void release_records(Record* base, std::size_t n)
{
    if (!base) return;
    for (std::size_t i = 0; i < n; ++i) {
        if (base[i].payload) {
            std::free(base[i].payload);
        }
    }
    std::free(base);
}

/*---------------------------------------------------------------------------*/
/* 6.  Sorting & validation helpers                                          */
/*---------------------------------------------------------------------------*/
static inline void sort_records(Record* base, std::size_t n)
{
    std::sort(base, base + n,
            [](const Record& a, const Record& b) { return a.key < b.key; });
}

static inline bool check_if_sorted(const Record* base, std::size_t n)
{
    if(!std::is_sorted(base, base + n,
            [](const Record& a, const Record& b){ return a.key < b.key; })) {
                std::fprintf(stderr, "Array NOT sorted!\n");
                return false;
            }
    return true; 
}

static inline void dump_records(const Record* base, std::size_t n, std::size_t max_lines = 10)
{
    for (std::size_t i = 0; i < n && i < max_lines; ++i)
        std::printf("%4zu : key=%lu  len=%u\n", i, base[i].key, base[i].len);
    if (n > max_lines) std::puts("…");
}

/*---------------------------------------------------------------------------*/
/* 7.  Merge two sorted runs into an arbitrary destination using a temp arr  */
/*---------------------------------------------------------------------------*/
/*
static inline void merge_into_dest(const Record*  a,   std::size_t na,
                                   const Record*  b,   std::size_t nb,
                                   Record*        dest,
                                   Record*        scratch)          // <- temp
{
    const std::size_t n_total = na + nb;

    // ---- merge into the scratch buffer ---------------------------------
    std::size_t i = 0, j = 0, k = 0;
    while (i < na && j < nb)
        scratch[k++] = (a[i].key <= b[j].key) ? a[i++] : b[j++];
    while (i < na)  scratch[k++] = a[i++];
    while (j < nb)  scratch[k++] = b[j++];

    // ---- copy back into destination ------------------------------------
    std::memcpy(dest, scratch, n_total * sizeof(Record));
}
*/

static inline void merge_records(Record* base, std::size_t left, std::size_t mid, std::size_t right)
{
    std::inplace_merge(base + left,           // first half begin
                       base + mid + 1,        // second half begin
                       base + right + 1,      // range end (one-past-last)
                       [](const Record& a, const Record& b) { return a.key < b.key; });
}

/*---------------------------------------------------------------------------*/
#endif /* UTILS_HPP */
