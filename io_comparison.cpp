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

static std::string generate_unsorted_file_streaming(std::size_t    total_n,
                                                    std::uint32_t payload_max)
{
    namespace fs = std::filesystem;
    fs::create_directories("files");

    std::string path = "files/a_unsorted_"
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

    // I/O buffer (1 MiB), flush when >= 512 KiB
    constexpr std::size_t IO_BUF_SZ    = 1 << 20;
    constexpr std::size_t FLUSH_THRESH = 512 << 10; // 512 KiB
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

static std::string generate_unsorted_file(std::size_t total_n,
                                          std::uint32_t payload_max)
{
    // 1) ensure output folder
    std::filesystem::create_directories("files");

    // 2) cache-keyed filename
    std::string path = "files/b_unsorted_"
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
    std::vector<char> buffer(payload_max);

    // tune the ofstream's write-buffer
    std::vector<char> io_buf(512 << 10);       // 512 KiB

    std::cout << "Streaming-generating " << total_n 
              << " records into “" << path << "”…\n";

    // 7) generate-and-write loop
    for (std::size_t i = 0; i < total_n; ++i) {
        unsigned long key = static_cast<unsigned long>(key_gen(rng));
        uint32_t len    = static_cast<uint32_t>(len_gen(rng));

        // fill buffer[0..len)
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



static std::string generate_unsorted_file(std::size_t total_n,
                                          std::uint32_t payload_max)
{
    // 1) ensure output folder
    std::filesystem::create_directories("files");

    // 2) cache-keyed filename
    std::string path = "files/b_unsorted_"
                     + std::to_string(total_n) + "_"
                     + std::to_string(payload_max) + ".bin";

    // 3) skip if already exists
    if (std::filesystem::exists(path)) {
        std::cout << "Found existing unsorted file (\"" << path 
                  << "\") – skipping generation.\n";
        return path;
    }

    // 4) open for binary writing via C stdio
    FILE* f = std::fopen(path.c_str(), "wb");
    if (!f) {
        std::perror("opening unsorted file");
        std::exit(1);
    }

    struct stat stats;
    if (fstat(fileno(f), &stats) == -1) // POSIX only
    {
        std::perror("fstat");
        exit(1);
    }
 
    std::cout << "BUFSIZ is " << BUFSIZ << ", but optimal block size is "
              << stats.st_blksize << '\n';

    // 5) tune the stdio buffer to 512 KiB
    std::vector<char> io_buf(512 << 10);
    if (std::setvbuf(f, nullptr, _IOFBF, 512 << 10) != 0) {
        std::perror("setvbuf");
        std::fclose(f);
        std::exit(1);
    }

    // 6) set up RNG & distributions
    std::mt19937                     rng{42};
    std::uniform_int_distribution<>  key_gen(0, INT32_MAX);
    std::uniform_int_distribution<>  len_gen(8, payload_max);
    std::uniform_int_distribution<>  byte_gen(0, 255);

    // 7) one reusable buffer for payload bytes
    std::vector<char> buffer(payload_max);

    std::cout << "Streaming-generating " << total_n 
              << " records into \"" << path << "\"…\n";

    // right after you open f …
    // size of key+len header
    constexpr std::size_t HDR_SZ = sizeof(unsigned long) + sizeof(uint32_t);
    // record buffer big enough for header + max payload
    std::vector<char> rec_buf(HDR_SZ + payload_max);

    
    // 8) generate-and-write loop
    for (std::size_t i = 0; i < total_n; ++i) {
            // 1) draw key & len
        unsigned long key = static_cast<unsigned long>(key_gen(rng));
        uint32_t      len = static_cast<uint32_t>(len_gen(rng));

        // 2) pack header
        std::memcpy(rec_buf.data(),              &key, sizeof(key));
        std::memcpy(rec_buf.data() + sizeof(key), &len, sizeof(len));

        // 3) fill payload directly into rec_buf
        for (uint32_t j = 0; j < len; ++j) {
            rec_buf[HDR_SZ + j] = static_cast<char>(byte_gen(rng));
        }

        // 4) write exactly one buffer per record
        std::size_t rec_sz = HDR_SZ + len;
        if (std::fwrite(rec_buf.data(), 1, rec_sz, f) != rec_sz) {
            std::perror("fwrite");
            std::fclose(f);
            std::exit(1);
        }
    }

    // 9) clean up
    std::fclose(f);
    std::cout << "Unsorted file ready: \"" << path << "\".\n";
    return path;
}


static std::string generate_unsorted_file(std::size_t total_n,
                                          std::uint32_t payload_max)
{
    namespace fs = std::filesystem;
    // 1) ensure output folder
    fs::create_directories("files");

    // 2) build filename
    std::string path = "files/b_unsorted_"
                     + std::to_string(total_n) + "_"
                     + std::to_string(payload_max) + ".bin";

    // 3) skip if it exists
    if (fs::exists(path)) {
        std::cout << "Found existing file \"" << path << "\" – skipping.\n";
        return path;
    }

    // 4) determine alignment (page size)
    const size_t align = static_cast<size_t>(sysconf(_SC_PAGESIZE));

    // 5) open with O_DIRECT
    int fd = ::open(path.c_str(),
                    O_CREAT | O_WRONLY | O_TRUNC | O_DIRECT,
                    0644);
    if (fd < 0) {
        std::perror("open O_DIRECT");
        std::exit(1);
    }

    // 6) allocate aligned I/O buffer (1 MiB)
    constexpr size_t IO_BUF_SZ = 1 << 20; // 1 MiB
    char* io_buf = nullptr;
    if (posix_memalign(reinterpret_cast<void**>(&io_buf), align, IO_BUF_SZ) != 0) {
        std::perror("posix_memalign");
        ::close(fd);
        std::exit(1);
    }
    size_t io_pos = 0;

    // 7) prepare RNG
    std::mt19937                     rng{42};
    std::uniform_int_distribution<>  key_gen(0, INT32_MAX);
    std::uniform_int_distribution<>  len_gen(8, payload_max);
    std::uniform_int_distribution<>  byte_gen(0, 255);

    // 8) compute header size and allocate record-scratch buffer
    constexpr std::size_t HDR_SZ = sizeof(unsigned long) + sizeof(uint32_t);
    std::vector<char>     payload_buf(payload_max);

    std::cout << "Streaming-generating " << total_n
              << " records into \"" << path << "\" with O_DIRECT…\n";

    // 9) generate & pack records
    for (std::size_t i = 0; i < total_n; ++i) {
        unsigned long key = static_cast<unsigned long>(key_gen(rng));
        uint32_t      len = static_cast<uint32_t>(len_gen(rng));

        // fill payload
        for (uint32_t j = 0; j < len; ++j) {
            payload_buf[j] = static_cast<char>(byte_gen(rng));
        }

        // ensure room in io_buf; flush if necessary
        size_t rec_sz = HDR_SZ + len;
        if (io_pos + rec_sz > IO_BUF_SZ) {
            // write down to a multiple of align
            size_t write_sz = io_pos - (io_pos % align);
            if (write_sz > 0) {
                ssize_t w = ::write(fd, io_buf, write_sz);
                if (w < 0 || static_cast<size_t>(w) != write_sz) {
                    std::perror("direct write");
                    free(io_buf);
                    ::close(fd);
                    std::exit(1);
                }
            }
            // shift leftover
            size_t rem = io_pos - write_sz;
            if (rem > 0) {
                std::memmove(io_buf, io_buf + write_sz, rem);
            }
            io_pos = rem;
        }

        // pack header
        std::memcpy(io_buf + io_pos, &key, sizeof(key));
        std::memcpy(io_buf + io_pos + sizeof(key), &len, sizeof(len));
        // pack payload
        std::memcpy(io_buf + io_pos + HDR_SZ, payload_buf.data(), len);
        io_pos += rec_sz;
    }

    // 10) final flush (pad up to block size)
    if (io_pos > 0) {
        size_t write_sz = ((io_pos + align - 1) / align) * align;
        // zero pad
        std::memset(io_buf + io_pos, 0, write_sz - io_pos);
        ssize_t w = ::write(fd, io_buf, write_sz);
        if (w < 0 || static_cast<size_t>(w) != write_sz) {
            std::perror("final direct write");
            free(io_buf);
            ::close(fd);
            std::exit(1);
        }
    }

    // 11) cleanup
    free(io_buf);
    ::close(fd);
    std::cout << "Unsorted file ready: \"" << path << "\".\n";
    return path;
}

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
    BENCH_START(generate_arrays);
    std::vector<unsigned long> keys (total_n);
    std::vector<uint32_t>      lens (total_n);
    size_t exact_size = 0;
    for (size_t i = 0; i < total_n; ++i) {
        keys[i] = static_cast<unsigned long>( key_gen(rng) );
        lens[i] = static_cast<uint32_t>      ( len_gen(rng) );
        exact_size += KEY_SZ + LEN_SZ + lens[i];
    }
    BENCH_STOP(generate_arrays);

    BENCH_START(open_fallocate);
    int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        std::perror("open");
        std::exit(1);
    }
    if (int err = posix_fallocate(fd, 0, exact_size); err != 0) {
        std::cerr << "posix_fallocate failed: "
                  << std::strerror(err) << "\n";
        ::close(fd);
        std::exit(1);
    }
    BENCH_STOP(open_fallocate);

    // 3) mmap(write-only) the exact_size region
    BENCH_START(mmap);
    char* map = static_cast<char*>(
        mmap(nullptr, exact_size, PROT_WRITE, MAP_SHARED, fd, 0)
    );
    if (map == MAP_FAILED) {
        std::perror("mmap");
        std::exit(1);
    }
    BENCH_STOP(mmap);

    // 4) prepare a single-record buffer: header + max-payload
    BENCH_START(generate_records);
    std::vector<char> record_buf(KEY_SZ + LEN_SZ + payload_max);
    size_t offset = 0;

    for (size_t i = 0; i < total_n; ++i) {
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
        size_t rec_sz = KEY_SZ + LEN_SZ + len;
        std::memcpy(map + offset, record_buf.data(), rec_sz);
        offset += rec_sz;
    }
    BENCH_STOP(generate_records);

    // 5) unmap & close
    BENCH_START(teardown);
    munmap(map, exact_size);
    ::close(fd);
    BENCH_STOP(teardown);

    std::cout << "Generated “" << path << "” (" << exact_size << " bytes).\n";
    return path;
}
