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
//  Phase 1 – streaming generator                                               
//------------------------------------------------------------------------------
static void generate_unsorted_file(const std::string& path,
                                   std::size_t        total_n,
                                   std::size_t        payload_max)
{
    std::ofstream fout(path, std::ios::binary | std::ios::trunc);
    if (!fout) fatal("cannot create unsorted file");

    constexpr std::size_t GEN_CHUNK = 1 << 20;  // generate 1 M records at a time

    std::size_t remaining = total_n;
    while (remaining) {
        std::size_t n = std::min(GEN_CHUNK, remaining);
        Record* rec  = alloc_random_records(n, payload_max);

        for (std::size_t i = 0; i < n; ++i) {
            fout.write(reinterpret_cast<const char*>(&rec[i].key), sizeof rec[i].key);
            fout.write(reinterpret_cast<const char*>(&rec[i].len), sizeof rec[i].len);
            fout.write(rec[i].payload, rec[i].len);
        }
        release_records(rec, n);
        remaining -= n;
    }
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

    const std::string unsorted_file = "records_unsorted.bin";
    const std::string sorted_file   = "records_sorted.bin";

    // Phase 1 – streaming generation --------------------------------------
    if (!fs::exists(unsorted_file)) {
        std::cout << "Streaming‑generating " << opt.n_records << " records…\n";
        generate_unsorted_file(unsorted_file, opt.n_records, opt.payload_max);
        std::cout << "Unsorted file ready.\n";
    } else {
        std::cout << "Found existing unsorted file – skipping generation.\n";
    }

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
    rewrite_sorted(unsorted_file, sorted_file, index);
    BENCH_STOP(rewrite_sorted);

    // Phase 5 – verify -----------------------------------------------------
    std::cout << "Verifying output…\n";
    if (!file_is_sorted(sorted_file)) fatal("output NOT sorted");
    std::cout << "Success: sorted file is in order.\n";
    return 0;
}
