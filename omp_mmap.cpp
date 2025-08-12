/*  OpenMP Merge-Sort with overlapped index-building (simple locking)
 *  - Overlaps progressive index build with task-parallel mergesort.
 *  - Requires your utils.hpp as-is (no changes).
 *  - Compile: g++ -O3 -std=c++17 -fopenmp -o bin/omp_mmap omp_mmap.cpp
 */

#include "utils.hpp"
#include <omp.h>

#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdlib>  // for std::malloc

/* ------------------------- shared progress state ------------------------- */
static std::mutex              g_mtx;
static std::condition_variable g_cv;
static std::size_t             g_filled = 0;  // how many IndexRec entries are ready

static inline void wait_until_filled(std::size_t need_count) {
  std::unique_lock<std::mutex> lk(g_mtx);
  g_cv.wait(lk, [&]{ return g_filled >= need_count; });
}

static inline void notify_filled(std::size_t filled_now) {
  {
    std::lock_guard<std::mutex> lk(g_mtx);
    g_filled = filled_now;
  }
  g_cv.notify_all();
}

/* --------- progressive index builder (mmap), wakes every chunk ------------ */
static void build_index_mmap_progressive(const std::string& path,
                                         IndexRec* idx,
                                         std::size_t n,
                                         int notify_every)
{
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

    if (notify_every > 0) {
      const std::size_t filled_now = i + 1;
      if (filled_now % static_cast<std::size_t>(notify_every) == 0) {
        notify_filled(filled_now);
      }
    }
  }

  // Final wake-up so everyone can proceed up to n
  notify_filled(n);

  ::munmap(const_cast<unsigned char*>(base), file_sz);
  ::close(fd);
}

/* --------------------- mergesort tasks with gating ----------------------- */
static inline void mergesort_task(IndexRec* base,
                                  std::size_t left,
                                  std::size_t right,
                                  int cutoff)
{
  if (left >= right) return;

  const std::size_t mid = (left + right) / 2;

  if (static_cast<int>(right - left) > cutoff) {
    #pragma omp task shared(base)
    mergesort_task(base, left,  mid,   cutoff);

    #pragma omp task shared(base)
    mergesort_task(base, mid+1, right, cutoff);

    #pragma omp taskwait

    // No extra wait here: both children already waited before sorting.
    merge_records(base, left, mid, right);
  } else {
    // Leaf work: wait until our whole slice is available, then sort it.
    wait_until_filled(right + 1);
    sort_records(base + left, right - left + 1);
  }
}

/* --------------------------------- main ---------------------------------- */
int main(int argc, char** argv)
{
  Params opt = parse_argv(argc, argv);
  if (opt.n_threads > 0) omp_set_num_threads(opt.n_threads);

  BENCH_START(total_time);

  // 1) Generate unsorted file (unchanged)
  BENCH_START(generate);
  std::string unsorted_file = generate_unsorted_file_mmap(opt.n_records, opt.payload_max);
  BENCH_STOP(generate);

  // 2+3) Overlap index build and mergesort
  IndexRec* idx = static_cast<IndexRec*>(std::malloc(opt.n_records * sizeof(IndexRec)));
  if (!idx) { std::perror("malloc"); std::exit(1); }

  BENCH_START(index_plus_sort);

  {
    // reset progress
    {
      std::lock_guard<std::mutex> lk(g_mtx);
      g_filled = 0;
    }

    #pragma omp parallel
    {
      #pragma omp single
      {
        // A) Progressive index builder (wake every opt.cutoff records)
        #pragma omp task shared(idx)
        build_index_mmap_progressive(unsorted_file, idx, opt.n_records, opt.cutoff);

        // B) Mergesort on the index with readiness gating
        #pragma omp task shared(idx)
        mergesort_task(idx, 0, opt.n_records - 1, opt.cutoff);

        #pragma omp taskwait
      }
    }
  }

  BENCH_STOP(index_plus_sort);

  // 4) Rewrite sorted file (unchanged; rewrite_sorted_mmap frees idx)
  BENCH_START(rewrite_sorted);
  const std::string sorted_file =
      "files/sorted_" + std::to_string(opt.n_records) + "_"
                       + std::to_string(opt.payload_max) + ".bin";
  rewrite_sorted_mmap(unsorted_file, sorted_file, idx, opt.n_records);
  BENCH_STOP(rewrite_sorted);

  // 5) Verify (unchanged; will also unlink the sorted file in your helpers)
  BENCH_START(check_if_sorted);
  check_if_sorted_mmap(sorted_file, opt.n_records);
  BENCH_STOP(check_if_sorted);

  BENCH_STOP(total_time);
  return 0;
}
