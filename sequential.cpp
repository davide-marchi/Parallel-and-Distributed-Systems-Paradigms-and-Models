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
// Phase 1 – mmap generator with exact-size preallocation and single-recopy
//------------------------------------------------------------------------------
static std::string generate_unsorted_file_streaming(std::size_t    total_n,
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

    // Open file for streaming writes
    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        std::perror("open");
        std::exit(1);
    }

    // I/O buffer (1 GiB), flush when >= 512 MiB
    constexpr std::size_t IO_BUF_SZ    = 1 << 30;
    constexpr std::size_t FLUSH_THRESH = 512 << 20; // 512 MiB
    std::vector<char> io_buf;
    io_buf.reserve(IO_BUF_SZ);

    // Payload scratch buffer
    std::vector<char> payload_buf(payload_max);

    // RNG setup
    std::mt19937                    rng{42};
    std::uniform_int_distribution<> key_gen(0, INT32_MAX);
    std::uniform_int_distribution<> len_gen(8, payload_max);
    std::uniform_int_distribution<> byte_gen(0, 255);

    // Generate records into io_buf, flush in big batches
    for (std::size_t i = 0; i < total_n; ++i) {
        
        // inside your loop, *instead* of RecHdr hdr + single insert:
        unsigned long key = static_cast<unsigned long>( key_gen(rng) );
        uint32_t      len = static_cast<uint32_t>      ( len_gen(rng) );

        // fill payload_buf[0..len)
        for (uint32_t j = 0; j < len; ++j)
            payload_buf[j] = static_cast<char>(byte_gen(rng));

        // pack header into a small on-stack buffer
        char hdr_buf[sizeof(key) + sizeof(len)];
        std::memcpy(hdr_buf,             &key, sizeof(key));
        std::memcpy(hdr_buf + sizeof(key), &len,  sizeof(len));

        // append header bytes
        io_buf.insert(io_buf.end(), hdr_buf, hdr_buf + sizeof(hdr_buf));

        // append payload bytes
        io_buf.insert(io_buf.end(),
                    payload_buf.data(),
                    payload_buf.data() + len);

        // flush if buffer is big
        if (io_buf.size() >= FLUSH_THRESH) {
            ssize_t w = ::write(fd, io_buf.data(), io_buf.size());
            if (w < 0 || static_cast<size_t>(w) != io_buf.size()) {
                std::perror("write");
                ::close(fd);
                std::exit(1);
            }
            io_buf.clear();
        }
    }

    // flush any remaining data
    if (!io_buf.empty()) {
        ssize_t w = ::write(fd, io_buf.data(), io_buf.size());
        if (w < 0 || static_cast<size_t>(w) != io_buf.size()) {
            std::perror("write final");
            ::close(fd);
            std::exit(1);
        }
    }

    ::close(fd);
    std::cout << "Generated “" << path << "”.\n";
    return path;
}

//------------------------------------------------------------------------------
//  Phase 2 – build index (key + offset)                                        
//------------------------------------------------------------------------------
struct IndexRec {
    unsigned long key;  // same as in Record
    uint64_t      offset; 
    uint32_t      len;     // payload length
};

/**
 * build_index_mmap
 *  - in_path:   path to the unsorted file
 *  - total_n:   expected number of records
 * Returns a malloc’d IndexRec[total_n], or nullptr on error.
 */
static IndexRec*
build_index_mmap(const std::string& in_path,
                 std::size_t        total_n)
{
    // 1) open & stat
    int fd = ::open(in_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::perror("[build_index_mmap] open");
        return nullptr;
    }
    struct stat st;
    if (fstat(fd, &st) < 0) {
        std::perror("[build_index_mmap] fstat");
        close(fd);
        return nullptr;
    }
    size_t file_sz = st.st_size;

    // 2) mmap entire file
    void* map = mmap(nullptr, file_sz,
                     PROT_READ, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        std::perror("[build_index_mmap] mmap");
        close(fd);
        return nullptr;
    }
    const char* data = static_cast<const char*>(map);

    // 3) allocate exact‐size index array
    IndexRec* idx = static_cast<IndexRec*>(
                        std::malloc(total_n * sizeof(IndexRec)));
    if (!idx) {
        std::cerr << "[build_index_mmap] malloc failed\n";
        munmap(map, file_sz);
        close(fd);
        return nullptr;
    }

    // 4) one pass: parse each record in memory
    size_t pos = 0;
    for (size_t i = 0; i < total_n; ++i) {
        if (pos + sizeof(unsigned long) + sizeof(uint32_t) > file_sz) {
            std::cerr << "[build_index_mmap] unexpected EOF at rec " << i << "\n";
            free(idx);
            munmap(map, file_sz);
            close(fd);
            return nullptr;
        }

        // read key & len from mapped memory
        unsigned long key = *reinterpret_cast<const unsigned long*>(data + pos);
        uint32_t      len = *reinterpret_cast<const uint32_t*>     (data + pos + sizeof(unsigned long));

        // record in index
        idx[i].key    = key;
        idx[i].offset = pos;
        idx[i].len    = len;

        // advance pos over header + payload
        pos += sizeof(unsigned long) + sizeof(uint32_t) + len;
    }

    // 5) cleanup mmap (we still hold our own copy of offsets/lengths)
    munmap(map, file_sz);
    close(fd);

    return idx;
}


//------------------------------------------------------------------------------
//  Phase 4 – rewrite sorted file                                               
//------------------------------------------------------------------------------
/**
 * rewrite_sorted_mmap
 *  - in_path:  path to the unsorted input file
 *  - out_path: path for the sorted output file
 *  - idx:      array of IndexRec entries (key, offset, len), already sorted by key
 *  - n_idx:    number of entries in idx[]
 *
 * Returns true on success, false on any error.
 */
static bool
rewrite_sorted_mmap(const std::string& in_path,
                    const std::string& out_path,
                    IndexRec*          idx,
                    std::size_t        n_idx)
{
    // 1) open & stat input
    int fd_in = ::open(in_path.c_str(), O_RDONLY);
    if (fd_in < 0) { perror("open in"); return false; }
    struct stat st;
    if (fstat(fd_in, &st) < 0) { perror("fstat in"); close(fd_in); return false; }
    size_t in_size = st.st_size;

    // 2) mmap entire input read-only
    char* in_map = (char*)mmap(nullptr, in_size,
                               PROT_READ, MAP_SHARED, fd_in, 0);
    if (in_map == MAP_FAILED) { perror("mmap in"); close(fd_in); return false; }

    // 3) compute total output size
    size_t out_size = 0;
    for (size_t i = 0; i < n_idx; ++i) {
        out_size += sizeof(idx[i].key)
                  + sizeof(idx[i].len)
                  + idx[i].len;
    }

    BENCH_START(open_and_mmap_output);
    // 4) open, truncate & mmap output read/write
    int fd_out = ::open(out_path.c_str(),
                        O_CREAT|O_RDWR|O_TRUNC, 0644);
    if (fd_out < 0) { perror("open out"); munmap(in_map, in_size); close(fd_in); return false; }
    if (ftruncate(fd_out, out_size) < 0) { perror("ftruncate"); munmap(in_map, in_size); close(fd_in); close(fd_out); return false; }

    char* out_map = (char*)mmap(nullptr, out_size,
                                PROT_WRITE, MAP_SHARED, fd_out, 0);
    if (out_map == MAP_FAILED) { perror("mmap out"); munmap(in_map, in_size); close(fd_in); close(fd_out); return false; }

    BENCH_STOP(open_and_mmap_output);

    // 5) copy each record in one memcpy
    size_t out_off = 0;
    for (size_t i = 0; i < n_idx; ++i) {
        IndexRec& r = idx[i];
        size_t rec_size = sizeof(r.key) + sizeof(r.len) + r.len;

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
    return true;
}


//------------------------------------------------------------------------------
//  Verification                                                                
//------------------------------------------------------------------------------
static bool check_if_sorted_mmap(const std::string& path,
                     std::size_t        total_n)
{
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror("open"); return false; }

    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); close(fd); return false; }
    size_t sz = st.st_size;

    char* map = (char*)mmap(nullptr, sz,
                            PROT_READ, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) { perror("mmap"); close(fd); return false; }

    size_t pos = 0;
    unsigned long prev_key = 0;
    for (size_t i = 0; i < total_n; ++i) {
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
    return true;
}

//------------------------------------------------------------------------------
//  Main                                                                        
//------------------------------------------------------------------------------
int main(int argc, char** argv)
{
    Params opt = parse_argv(argc, argv);

    // Phase 1 – streaming generation --------------------------------------
    BENCH_START(generate_unsorted_file);
    std::string unsorted_file = generate_unsorted_file_streaming(opt.n_records, opt.payload_max);
    BENCH_STOP(generate_unsorted_file);

    // Phase 2 – build index ------------------------------------------------
    BENCH_START(build_index);
    IndexRec*   idx   = build_index_mmap(unsorted_file, opt.n_records);
    BENCH_STOP(build_index);

    // Phase 3 – sort index in RAM -----------------------------------------
    BENCH_START(sort_records);
    std::sort(idx, idx + opt.n_records,
          [](const IndexRec& a, const IndexRec& b) {
              return a.key < b.key;
          });
    BENCH_STOP(sort_records);

    // Phase 4 – rewrite sorted file ---------------------------------------
    BENCH_START(rewrite_sorted);
    rewrite_sorted_mmap(unsorted_file, "files/sorted_"
                     + std::to_string(opt.n_records) + "_"
                     + std::to_string(opt.payload_max) + ".bin", idx, opt.n_records);
    BENCH_STOP(rewrite_sorted);

    // Phase 5 – free index ------------------------------------------------
    std::free(idx);

    // Phase 6 – verify -----------------------------------------------------
    std::cout << "Verifying output…\n";
    BENCH_START(check_if_sorted_mmap);
    check_if_sorted_mmap("files/sorted_"
                     + std::to_string(opt.n_records) + "_"
                     + std::to_string(opt.payload_max) + ".bin", opt.n_records);
    BENCH_STOP(check_if_sorted_mmap);
    return 0;
}
