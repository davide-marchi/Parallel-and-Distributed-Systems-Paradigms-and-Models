// sequential_external.cpp – Streaming generator + external sort
// -----------------------------------------------------------------------------
// Phases (each keeps memory use bounded):
//   1) **Streaming generation** – create records_unsorted.bin without ever
//      holding all records in RAM.  We allocate ≤GEN_CHUNK records at a time,
//      dump them, and free.
//   2) Build an in‑RAM index (key + offset) – fits as long as 16 B·N < RAM.
//   3) Use existing sort_records() on that index.
//   4) Rewrite the sorted file by streaming payloads.
//   5) Verify ordering with a one‑pass scan.
// -----------------------------------------------------------------------------

#include "utils.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdio>

namespace fs = std::filesystem;

static void fatal(const char* msg) {
    std::cerr << "[external_sort] " << msg << "\n";
    std::exit(EXIT_FAILURE);
}

//------------------------------------------------------------------------------
// Phase 1 – direct streaming generator
//------------------------------------------------------------------------------
static std::string generate_unsorted_file(std::size_t total_n,
                                          std::uint32_t payload_max)
{
    // 1) ensure output folder
    fs::create_directories("files");

    // 2) cache-keyed filename
    std::string path = "files/unsorted_"
                     + std::to_string(total_n) + "_"
                     + std::to_string(payload_max) + ".bin";

    // 3) skip if already exists
    if (fs::exists(path)) {
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
struct OffsetRec : Record { /* inherits key, len, payload ptr */ };

static std::vector<Record> build_index(const std::string& input_path)
{
    std::ifstream fin(input_path, std::ios::binary);
    if (!fin) fatal("cannot open input file");

    std::vector<Record> idx;
    idx.reserve(1 << 20);

    while (true) {
        std::streampos pos = fin.tellg();
        if (pos == -1) break;

        uint64_t key;  uint32_t len;
        if (!fin.read(reinterpret_cast<char*>(&key), sizeof key)) break;
        if (!fin.read(reinterpret_cast<char*>(&len), sizeof len))
            fatal("truncated header while building index");

        Record stub{};
        stub.key     = key;
        stub.len     = 0;
        stub.payload = reinterpret_cast<char*>(static_cast<uint64_t>(pos));
        idx.push_back(stub);

        fin.seekg(len, std::ios::cur);
        if (!fin) fatal("truncated payload while building index");
    }

    std::cout << "[build_index] N=" << idx.size() << "  index="
              << (idx.size() * sizeof(Record) / (1024 * 1024)) << " MiB\n";
    return idx;
}

//------------------------------------------------------------------------------
//  Phase 4 – rewrite sorted file                                               
//------------------------------------------------------------------------------
static void rewrite_sorted(const std::string& input_path,
                           const std::string& output_path,
                           const std::vector<Record>& sorted_idx)
{
    std::ifstream fin(input_path, std::ios::binary);
    if (!fin) fatal("cannot reopen input file");

    std::ofstream fout(output_path, std::ios::binary | std::ios::trunc);
    if (!fout) fatal("cannot create sorted output file");

    std::vector<char> buf(1 << 20);

    for (const Record& r : sorted_idx) {
        uint64_t offset = reinterpret_cast<uint64_t>(r.payload);
        fin.seekg(offset);

        uint64_t key;  uint32_t len;
        fin.read(reinterpret_cast<char*>(&key), sizeof key);
        fin.read(reinterpret_cast<char*>(&len), sizeof len);

        if (len > buf.size()) buf.resize(len);
        fin.read(buf.data(), len);

        fout.write(reinterpret_cast<char*>(&key), sizeof key);
        fout.write(reinterpret_cast<char*>(&len), sizeof len);
        fout.write(buf.data(), len);
    }
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
    std::vector<Record> index = build_index(unsorted_file);
    BENCH_STOP(build_index);

    // Phase 3 – sort index in RAM -----------------------------------------
    BENCH_START(sort_records);
    sort_records(index.data(), index.size());
    BENCH_STOP(sort_records);

    // Phase 4 – rewrite sorted file ---------------------------------------
    BENCH_START(rewrite_sorted);
    rewrite_sorted(unsorted_file, "files/sorted_"
                     + std::to_string(opt.n_records) + "_"
                     + std::to_string(opt.payload_max) + ".bin", index);
    BENCH_STOP(rewrite_sorted);

    // Phase 5 – verify -----------------------------------------------------
    std::cout << "Verifying output…\n";
    if (!file_is_sorted("files/sorted_"
                     + std::to_string(opt.n_records) + "_"
                     + std::to_string(opt.payload_max) + ".bin")) fatal("output NOT sorted");
    std::cout << "Success: sorted file is in order.\n";
    return 0;
}
