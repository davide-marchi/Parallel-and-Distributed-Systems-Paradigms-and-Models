#ifndef UTILS_HPP
#define UTILS_HPP

#include <cstddef>              // std::size_t
#include <cstdint>              // std::uint32_t, std::uint64_t
#include <cstdlib>              // std::malloc, std::free, std::exit
#include <cstdio>               // std::printf, std::puts, std::perror
#include <string>               // std::string, stoull, stoul, to_string
#include <cstring>              // std::memcpy
#include <algorithm>            // std::sort, std::inplace_merge
#include <random>               // std::mt19937, std::uniform_int_distribution
#include <chrono>               // timing (BENCH_* macros)
#include <iostream>             // std::cout, std::cerr
#include <vector>               // std::vector
#include <filesystem>           // create_directories, path ops
#include <mutex>                // std::mutex, lock_guard, unique_lock
#include <condition_variable>   // std::condition_variable

// POSIX
#include <sys/mman.h>           // mmap, munmap
#include <sys/stat.h>           // fstat, struct stat
#include <fcntl.h>              // open, O_* flags
#include <unistd.h>             // close, unlink, ftruncate, getopt
#include <getopt.h>             // getopt_long, struct option


// Run-time parameters
struct Params {
    std::size_t   n_records   = 1'000'000;  // -n
    std::uint32_t payload_max = 256;        // -p
    std::size_t   n_threads   = 0;          // -t   (0 => use hw_concurrency)
    std::size_t   cutoff      = 10'000;     // -c   task-size threshold
};


// Timing utilities (chrono version, C++-20)
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


// Record definition
struct Record {
    unsigned long key;      // sorting key
    std::uint32_t len;      // payload length in bytes
    char*         payload;  // malloc-owned
};


// Build index (key + offset)
struct IndexRec {
    unsigned long key;      // same as in Record
    uint64_t      offset; 
    uint32_t      len;      // payload length
};


// Simple progress gate: wait until at least `need` records are ready, and allow the producer to notify progress
struct ProgressGate {

    std::mutex m;
    std::condition_variable cv;
    std::size_t filled = 0;

    void reset() {
        std::lock_guard<std::mutex> lk(m);
        filled = 0;
    }

    void notify(std::size_t filled_now) {
        // fprintf(stdout, "Notifying progress: %zu records ready\n", filled_now);
        {
            std::lock_guard<std::mutex> lk(m);
            filled = filled_now;
        }
        cv.notify_all();
    }

    void wait_until(std::size_t need) {
        std::unique_lock<std::mutex> lk(m);
        // fprintf(stdout, "Waiting for %zu records...\n", need);
        cv.wait(lk, [&]{ return filled >= need; });
    }
};


// Command-line parsing
static inline Params parse_argv(int argc, char** argv)
{
    Params opt{};

    static const struct option long_opts[] = {
        {"records",    required_argument, nullptr, 'n'},
        {"payload",    required_argument, nullptr, 'p'},
        {"threads",    required_argument, nullptr, 't'},
        {"cutoff",     required_argument, nullptr, 'c'},
        {"help",       no_argument,       nullptr, 'h'},
        {nullptr,      0,                 nullptr,  0 }
    };

    int c;
    while ((c = getopt_long(argc, argv, "n:p:t:c:h", long_opts, nullptr)) != -1) {
        switch (c) {
            case 'n':
                try {
                    opt.n_records = std::stoull(optarg);
                } catch (const std::invalid_argument& e) {
                    std::fprintf(stderr, "Error: --records not a number (%s)\n", optarg);
                    std::exit(1);
                } catch (const std::out_of_range& e) {
                    std::fprintf(stderr, "Error: --records out of range (%s)\n", optarg);
                    std::exit(1);
                }
                if (opt.n_records <= 0) {
                    std::fprintf(stderr, "Error: --records must be > 0 (got %zu)\n", opt.n_records);
                    std::exit(1);
                }
                break;
            case 'p':
                try {
                    auto v = std::stoul(optarg);
                    opt.payload_max = static_cast<std::uint32_t>(v);
                } catch (const std::exception&) {
                    std::fprintf(stderr, "Error: --payload not a number (%s)\n", optarg);
                    std::exit(1);
                }
                if (opt.payload_max < 8) {
                    std::fprintf(stderr, "Error: --payload must be ≥ 8 (got %u)\n", opt.payload_max);
                    std::exit(1);
                }
                break;
            case 't':
                try {
                    opt.n_threads = std::stoull(optarg);
                } catch (const std::invalid_argument&) {
                    std::fprintf(stderr, "Error: --threads not a number (%s)\n", optarg);
                    std::exit(1);
                } catch (const std::out_of_range&) {
                    std::fprintf(stderr, "Error: --threads out of range (%s)\n", optarg);
                    std::exit(1);
                }
                break;
            case 'c':
                try {
                    opt.cutoff = std::stoull(optarg);
                } catch (const std::invalid_argument&) {
                    std::fprintf(stderr, "Error: --cutoff not a number (%s)\n", optarg);
                    std::exit(1);
                } catch (const std::out_of_range&) {
                    std::fprintf(stderr, "Error: --cutoff out of range (%s)\n", optarg);
                    std::exit(1);
                }
                if (opt.cutoff <= 0) {
                    std::fprintf(stderr, "Error: --cutoff must be > 0 (got %zu)\n", opt.cutoff);
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


// Sorting & validation helpers
static inline void sort_records(IndexRec* base, std::size_t n)
{
    std::sort(base, base + n,
            [](const IndexRec& a, const IndexRec& b) { return a.key < b.key; });
}


static inline void dump_records(const Record* base, std::size_t n, std::size_t max_lines = 10)
{
    for (std::size_t i = 0; i < n && i < max_lines; ++i)
        std::printf("%4zu : key=%lu  len=%u\n", i, base[i].key, base[i].len);
    if (n > max_lines) std::puts("…");
}


// Merge two sorted runs into their location
static inline void merge_records(IndexRec* base, std::size_t left, std::size_t mid, std::size_t right)
{
    std::inplace_merge(base + left,           // first half begin
                       base + mid + 1,        // second half begin
                       base + right + 1,      // range end (one-past-last)
                       [](const IndexRec& a, const IndexRec& b) { return a.key < b.key; });
}


// mmap generator with exact-size preallocation and single-recopy
static std::string generate_unsorted_file_mmap(std::size_t total_n,
                                               std::uint32_t payload_max)
{
    namespace fs = std::filesystem;
    fs::create_directories("files");

    std::string path = "files/unsorted_"
                     + std::to_string(total_n) + "_"
                     + std::to_string(payload_max) + ".bin";

    if (fs::exists(path)) {
        std::cout << "Skipping gen; found “" << path << "”.\n";
        return path;
    }

    constexpr std::size_t KEY_SZ = sizeof(unsigned long);
    constexpr std::size_t LEN_SZ = sizeof(uint32_t);

    // RNG setup
    std::mt19937                    rng{42};
    std::uniform_int_distribution<> key_gen(0, INT32_MAX);
    std::uniform_int_distribution<> len_gen(8, payload_max);
    std::uniform_int_distribution<> byte_gen(0, 255);

    // 1) Precompute keys & lengths so we know exact file size
    //BENCH_START(generate_arrays);
    std::vector<unsigned long> keys (total_n);
    std::vector<uint32_t>      lens (total_n);
    std::size_t exact_size = 0;
    for (std::size_t i = 0; i < total_n; ++i) {
        keys[i] = static_cast<unsigned long>( key_gen(rng) );
        lens[i] = static_cast<uint32_t>      ( len_gen(rng) );
        exact_size += KEY_SZ + LEN_SZ + lens[i];
    }
    //BENCH_STOP(generate_arrays);

    // 2) open & preallocate exactly exact_size bytes
    //BENCH_START(open_truncate);
    int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        std::perror("open");
        std::exit(1);
    }
    if (ftruncate(fd, exact_size) < 0) {
        std::perror("ftruncate");
        std::exit(1);
    }
    //BENCH_STOP(open_truncate);

    // 3) mmap(write-only) the exact_size region
    //BENCH_START(mmap);
    char* map = static_cast<char*>(
        mmap(nullptr, exact_size, PROT_WRITE, MAP_SHARED, fd, 0)
    );
    if (map == MAP_FAILED) {
        std::perror("mmap");
        std::exit(1);
    }
    //BENCH_STOP(mmap);

    // 4) prepare a single-record buffer: header + max-payload
    //BENCH_START(generate_records);
    std::vector<char> record_buf(KEY_SZ + LEN_SZ + payload_max);
    std::size_t offset = 0;

    for (std::size_t i = 0; i < total_n; ++i) {
        unsigned long key = keys[i];
        uint32_t      len = lens[i];

        // fill header into record_buf
        std::memcpy(record_buf.data(), &key, KEY_SZ);
        std::memcpy(record_buf.data() + KEY_SZ, &len, LEN_SZ);

        // fill payload bytes
        for (uint32_t j = 0; j < len; ++j) {
            record_buf[KEY_SZ + LEN_SZ + j] = static_cast<char>(byte_gen(rng));
        }

        // one bulk copy into the mmap’d file
        std::size_t rec_sz = KEY_SZ + LEN_SZ + len;
        std::memcpy(map + offset, record_buf.data(), rec_sz);
        offset += rec_sz;
    }
    //BENCH_STOP(generate_records);

    // 5) unmap & close
    //BENCH_START(teardown);
    munmap(map, exact_size);
    ::close(fd);
    //BENCH_STOP(teardown);

    std::cout << "Generated “" << path << "” (" << exact_size << " bytes).\n";
    return path;
}


// Returns a malloc’d IndexRec[total_n], or nullptr on error.
inline void build_index_mmap(const std::string& path,   // path to the unsorted file
                             IndexRec* idx,
                             std::size_t n,             // expected number of records
                             int notify_every = 0,
                             ProgressGate* gate = nullptr)
{
  BENCH_START(reading);
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) { std::perror("open"); std::exit(1); }
  struct stat st{};
  if (fstat(fd, &st) < 0) { std::perror("fstat"); std::exit(1); }
  const std::size_t file_sz = static_cast<std::size_t>(st.st_size);

  void* map = ::mmap(nullptr, file_sz, PROT_READ, MAP_SHARED, fd, 0);
  if (map == MAP_FAILED) { std::perror("mmap"); std::exit(1); }

  const unsigned char* base = static_cast<const unsigned char*>(map);

  std::size_t pos = 0;
  for (std::size_t i = 0; i < n; ++i) {
    const std::size_t rec_offset = pos;

    unsigned long key;
    std::memcpy(&key, base + pos, sizeof(unsigned long));
    pos += sizeof(unsigned long);

    uint32_t len;
    std::memcpy(&len, base + pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    idx[i].key    = key;
    idx[i].offset = rec_offset;
    idx[i].len    = len;

    pos += len;

    if (gate && notify_every > 0) {
      const std::size_t filled_now = i + 1;
      if (filled_now % static_cast<std::size_t>(notify_every) == 0) {
        gate->notify(filled_now);
      }
    }
  }

  if (gate) gate->notify(n);

  ::munmap(const_cast<unsigned char*>(base), file_sz);
  ::close(fd);
  BENCH_STOP(reading);
}


// ALLOCATING OVERLOAD (backward compatible with old seq code)
inline IndexRec* build_index_mmap(const std::string& path, std::size_t n)
{
  // allocate with malloc because rewrite_sorted_mmap() calls free(idx)
  auto* idx = static_cast<IndexRec*>(std::malloc(n * sizeof(IndexRec)));
  if (!idx) { std::perror("malloc"); std::exit(1); }

  // delegate to the prealloc version with default behavior (no notifications)
  build_index_mmap(path, idx, n, /*notify_every=*/0, /*gate=*/nullptr);
  return idx;
}


//  Rewrite sorted file: Returns true on success, false on any error.
static bool
rewrite_sorted_mmap(const std::string& in_path,     // path to the unsorted input file
                    const std::string& out_path,    // path for the sorted output file
                    IndexRec*          idx,         // array of IndexRec entries (key, offset, len), already sorted by key
                    std::size_t        n_idx)       // number of entries in idx[]
{
    // 1) open & stat input
    int fd_in = ::open(in_path.c_str(), O_RDONLY);
    if (fd_in < 0) { perror("open in"); return false; }
    struct stat st;
    if (fstat(fd_in, &st) < 0) { perror("fstat in"); close(fd_in); return false; }
    std::size_t in_size = st.st_size;

    // 2) mmap entire input read-only
    char* in_map = (char*)mmap(nullptr, in_size,
                               PROT_READ, MAP_SHARED, fd_in, 0);
    if (in_map == MAP_FAILED) { perror("mmap in"); close(fd_in); return false; }

    // 3) compute total output size
    std::size_t out_size = 0;
    for (std::size_t i = 0; i < n_idx; ++i) {
        out_size += sizeof(idx[i].key)
                  + sizeof(idx[i].len)
                  + idx[i].len;
    }

    //BENCH_START(open_and_mmap_output);
    // 4) open, truncate & mmap output read/write
    int fd_out = ::open(out_path.c_str(),
                        O_CREAT|O_RDWR|O_TRUNC, 0644);
    if (fd_out < 0) { perror("open out"); munmap(in_map, in_size); close(fd_in); return false; }
    if (ftruncate(fd_out, out_size) < 0) { perror("ftruncate"); munmap(in_map, in_size); close(fd_in); close(fd_out); return false; }

    char* out_map = (char*)mmap(nullptr, out_size,
                                PROT_WRITE, MAP_SHARED, fd_out, 0);
    if (out_map == MAP_FAILED) { perror("mmap out"); munmap(in_map, in_size); close(fd_in); close(fd_out); return false; }

    //BENCH_STOP(open_and_mmap_output);

    // 5) copy each record in one memcpy
    std::size_t out_off = 0;
    for (std::size_t i = 0; i < n_idx; ++i) {
        IndexRec& r = idx[i];
        std::size_t rec_size = sizeof(r.key) + sizeof(r.len) + r.len;

        // direct memcpy from input-mapped region
        std::memcpy(out_map + out_off,
                    in_map  + r.offset,
                    rec_size);

        out_off += rec_size;
    }

    // 6) cleanup
    munmap(in_map,  in_size);
    munmap(out_map, out_size);
    close(fd_in);
    close(fd_out);
    free(idx);
    return true;
}


// Verification                                                                
static bool check_if_sorted_mmap(const std::string& path,
                                 std::size_t        total_n)
{
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror("open"); return false; }

    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); close(fd); return false; }
    std::size_t sz = st.st_size;

    char* map = (char*)mmap(nullptr, sz,
                            PROT_READ, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) { perror("mmap"); close(fd); return false; }

    std::size_t pos = 0;
    unsigned long prev_key = 0;
    for (std::size_t i = 0; i < total_n; ++i) {
        if (pos + sizeof(unsigned long)+sizeof(uint32_t) > sz) {
            std::cerr << "Unexpected EOF at record " << i << "\n";
            munmap(map, sz);
            close(fd);
            return false;
        }

        unsigned long key = *reinterpret_cast<unsigned long*>(map + pos);
        uint32_t      len = *reinterpret_cast<uint32_t*>     (map + pos + sizeof(unsigned long));

        if (i > 0 && key < prev_key) {
            std::cerr << "Out of order at record " << i
                      << ": " << key << " < " << prev_key << "\n";
            munmap(map, sz);
            close(fd);
            return false;
        }
        prev_key = key;
        pos += sizeof(unsigned long) + sizeof(uint32_t) + len;
    }

    munmap(map, sz);
    close(fd);
    std::cout << "File is sorted.\n";

    // POSIX unlink
    if (unlink(path.c_str()) < 0) {
        perror("unlink");
    }

    return true;
}


#endif /* UTILS_HPP */
