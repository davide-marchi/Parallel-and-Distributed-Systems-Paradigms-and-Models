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

/*---------------------------------------------------------------------------*/
/* 1.  Run-time parameters                                                   */
/*---------------------------------------------------------------------------*/
struct Params {
    std::size_t   n_records   = 1'000'000;  // -n
    std::uint32_t payload_max = 256;        // -p
    int           n_threads   = 0;          // -t   (0 => use hw_concurrency)
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
            case 'h':
            default:
                std::fprintf(stderr,
                    "Usage: %s [options]\n"
                    "  -n, --records N      number of records (default 1e6)\n"
                    "  -p, --payload B      maximum payload size in bytes (default 256)\n"
                    "  -t, --threads T      threads to use (0 = hw concurrency)\n"
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

static inline bool is_sorted(const Record* base, std::size_t n)
{
    return std::is_sorted(base, base + n,
            [](const Record& a, const Record& b){ return a.key < b.key; });
}

static inline void dump_records(const Record* base, std::size_t n, std::size_t max_lines = 10)
{
    for (std::size_t i = 0; i < n && i < max_lines; ++i)
        std::printf("%4zu : key=%lu  len=%u\n", i, base[i].key, base[i].len);
    if (n > max_lines) std::puts("…");
}

/*---------------------------------------------------------------------------*/
/* 7.  Two-way merge (stable, shallow copy)                                  */
/*---------------------------------------------------------------------------*/
static inline void merge_two_runs(const Record* a, std::size_t na,
                                  const Record* b, std::size_t nb,
                                  Record* out)
{
    std::size_t i = 0, j = 0, k = 0;
    while (i < na && j < nb) {
        if (a[i].key <= b[j].key)
            out[k++] = a[i++];
        else
            out[k++] = b[j++];
    }
    while (i < na) out[k++] = a[i++];
    while (j < nb) out[k++] = b[j++];
}

/*---------------------------------------------------------------------------*/
#endif /* UTILS_HPP */
