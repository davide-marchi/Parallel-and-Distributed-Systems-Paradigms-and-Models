#include "utils.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdio>


//------------------------------------------------------------------------------
// Phase 1 – direct streaming generator
//------------------------------------------------------------------------------
static std::string generate_unsorted_file(std::size_t total_n,
                                          std::uint32_t payload_max)
{
    // 1) ensure output folder
    std::filesystem::create_directories("files");

    // 2) cache-keyed filename
    std::string path = "files/unsorted_"
                     + std::to_string(total_n) + "_"
                     + std::to_string(payload_max) + ".bin";

    // 3) skip if already exists
    if (std::filesystem::exists(path)) {
        std::cout << "Found existing unsorted file (“" << path 
                  << "”) – skipping generation.\n";
        return path;
    }

    // 4) open for binary writing
    std::ofstream fout(path, std::ios::binary | std::ios::trunc);
    if (!fout) {
        std::perror("opening unsorted file");
        std::exit(1);
    }

    // 5) set up RNG & distributions
    std::mt19937                      rng{42};
    std::uniform_int_distribution<>  key_gen(0, INT32_MAX);
    std::uniform_int_distribution<>  len_gen(8, payload_max);
    std::uniform_int_distribution<>  byte_gen(0, 255);

    // 6) one reusable buffer for payload bytes
    std::vector<char> buffer;
    buffer.reserve(payload_max);

    std::cout << "Streaming-generating " << total_n 
              << " records into “" << path << "”…\n";

    // 7) generate-and-write loop
    for (std::size_t i = 0; i < total_n; ++i) {
        unsigned long key = static_cast<unsigned long>(key_gen(rng));
        uint32_t len    = static_cast<uint32_t>(len_gen(rng));

        // fill buffer[0..len)
        buffer.resize(len);
        for (uint32_t j = 0; j < len; ++j)
            buffer[j] = static_cast<char>(byte_gen(rng));

        // write key, len, payload
        fout.write(reinterpret_cast<const char*>(&key), sizeof(key));
        fout.write(reinterpret_cast<const char*>(&len),   sizeof(len));
        fout.write(buffer.data(),                         len);
    }

    std::cout << "Unsorted file ready: “" << path << "”.\n";
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
 * build_index
 *  - input_path: path to the unsorted binary file
 *  - total_n:     the exact number of records you expect
 * Returns a malloc'd array of IndexRec (length total_n), or nullptr on error.
 * Caller must free()
 */
static IndexRec* build_index(const std::string& input_path,
            std::size_t total_n)
{
    std::ifstream fin(input_path, std::ios::binary);
    if (!fin) {
        std::cerr << "[build_index] Error: cannot open “"
                  << input_path << "”.\n";
        return nullptr;
    }

    // allocate exact‐size index array
    IndexRec* idx = static_cast<IndexRec*>(
                        std::malloc(total_n * sizeof(IndexRec)));
    if (!idx) {
        std::cerr << "[build_index] Error: malloc(" 
                  << total_n * sizeof(IndexRec) 
                  << ") failed.\n";
        return nullptr;
    }

    for (std::size_t i = 0; i < total_n; ++i) {
        std::streampos pos = fin.tellg();
        if (pos < 0) {
            std::cerr << "[build_index] Unexpected EOF before record "
                      << i << "\n";
            std::free(idx);
            return nullptr;
        }

        unsigned long key;
        uint32_t      len;
        if (!fin.read(reinterpret_cast<char*>(&key), sizeof key)
         || !fin.read(reinterpret_cast<char*>(&len), sizeof len))
        {
            std::cerr << "[build_index] Truncated header at record "
                      << i << "\n";
            std::free(idx);
            return nullptr;
        }

        idx[i].key    = key;
        idx[i].offset = static_cast<uint64_t>(pos);
        idx[i].len    = len;

        // skip payload in one go
        fin.seekg(len, std::ios::cur);
        if (!fin) {
            std::cerr << "[build_index] Truncated payload at record "
                      << i << "\n";
            std::free(idx);
            return nullptr;
        }
    }

    return idx;
}


//------------------------------------------------------------------------------
//  Phase 4 – rewrite sorted file                                               
//------------------------------------------------------------------------------
static bool rewrite_sorted(const std::string& in_path,
               const std::string& out_path,
               IndexRec*          idx,
               std::size_t        n_idx,
               std::size_t        payload_max = 256)
{
    std::ifstream fin(in_path,  std::ios::binary);
    if (!fin) {
        std::cerr << "[rewrite_sorted] Error: cannot open input file “" << in_path << "”.\n";
        return false;
    }

    std::ofstream fout(out_path, std::ios::binary | std::ios::trunc);
    if (!fout) {
        std::cerr << "[rewrite_sorted] Error: cannot create sorted output file “" << out_path << "”.\n";
        return false;
    }

    // Max payload possible working buffer
    std::vector<char> buf(payload_max);

    for (std::size_t i = 0; i < n_idx; ++i) {
        const IndexRec& r = idx[i];

        // seek straight to the payload
        uint64_t payload_pos = r.offset
                             + sizeof(r.key)
                             + sizeof(r.len);
        fin.seekg(payload_pos, std::ios::beg);

        fin.read(buf.data(), r.len);

        // write from our in-memory index + buf
        fout.write(reinterpret_cast<const char*>(&r.key), sizeof(r.key));
        fout.write(reinterpret_cast<const char*>(&r.len), sizeof(r.len));
        fout.write(buf.data(),                         r.len);
    }

    return true;
}


//------------------------------------------------------------------------------
//  Verification                                                                
//------------------------------------------------------------------------------
static bool file_is_sorted(const std::string& path)
{
    std::ifstream fin(path, std::ios::binary);
    if (!fin) return false;

    uint64_t prev_key = 0, key;  uint32_t len; bool first = true;
    while (fin.read(reinterpret_cast<char*>(&key), sizeof key)) {
        fin.read(reinterpret_cast<char*>(&len), sizeof len);
        fin.seekg(len, std::ios::cur);
        if (!first && key < prev_key) return false;
        prev_key = key; first = false;
    }
    return true;
}

//------------------------------------------------------------------------------
//  Main                                                                        
//------------------------------------------------------------------------------
int main(int argc, char** argv)
{
    Params opt = parse_argv(argc, argv);

    // Phase 1 – streaming generation --------------------------------------
    std::string unsorted_file = generate_unsorted_file(opt.n_records, opt.payload_max);

    // Phase 2 – build index ------------------------------------------------
    BENCH_START(build_index);
    IndexRec*   idx   = build_index(unsorted_file, opt.n_records);
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
    rewrite_sorted(unsorted_file, "files/sorted_"
                     + std::to_string(opt.n_records) + "_"
                     + std::to_string(opt.payload_max) + ".bin", idx, opt.n_records, opt.payload_max);
    BENCH_STOP(rewrite_sorted);

    // Phase 5 – free index ------------------------------------------------
    std::free(idx);

    // Phase 6 – verify -----------------------------------------------------
    std::cout << "Verifying output…\n";
    if (!file_is_sorted("files/sorted_"
                     + std::to_string(opt.n_records) + "_"
                     + std::to_string(opt.payload_max) + ".bin")) {
        std::cerr << "[main] Error: output NOT sorted\n";
    } else {
        std::cout << "Success: sorted file is in order.\n";
    }
    return 0;
}
