// mpi_pairwise_tree_oneshot.cpp — Log2(P) pairwise merge; one-shot index sends
// Build: mpic++ -O3 -std=c++20 -fopenmp mpi_pairwise_tree_oneshot.cpp -o bin/mpi_pairwise_tree
// Run (Slurm): srun -N 4 -n 4 --cpus-per-task=8 ./bin/mpi_pairwise_tree -n 10000000 -p 8 -t 8 -c 10000

#include "utils.hpp" // parse_argv, BENCH_* timers, IndexRec { key, offset, len }, sort_records, merge_records, generate_unsorted_file_mmap, rewrite_sorted_mmap, check_if_sorted_mmap
#include <mpi.h>
#include <omp.h>

// ============================================================================
// OpenMP task mergesort for IndexRec — reuses your utils merge/sort primitives
// ============================================================================
static inline void mergesort_task(IndexRec* base,
                                  std::size_t left,
                                  std::size_t right,
                                  int cutoff)
{
    if (left >= right) return;
    const std::size_t mid = (left + right) / 2;

    if (static_cast<int>(right - left) > cutoff) {
        #pragma omp task shared(base)
        mergesort_task(base, left, mid, cutoff);
        #pragma omp task shared(base)
        mergesort_task(base, mid + 1, right, cutoff);
        #pragma omp taskwait
        merge_records(base, left, mid, right);
    } else {
        sort_records(base + left, right - left + 1);
    }
}

// ============================================================================
// MPI datatype for IndexRec so we can send/recv it directly
// ============================================================================
static inline MPI_Datatype make_mpi_indexrec_type() {
    MPI_Datatype dtype;
    int          blocklen[3] = {1, 1, 1};
    MPI_Aint     disp[3], base_addr;
    IndexRec     probe{};
    MPI_Get_address(&probe, &base_addr);
    MPI_Get_address(&probe.key,    &disp[0]);
    MPI_Get_address(&probe.offset, &disp[1]);
    MPI_Get_address(&probe.len,    &disp[2]);
    disp[0] -= base_addr; disp[1] -= base_addr; disp[2] -= base_addr;
    MPI_Datatype types[3] = { MPI_UNSIGNED_LONG, MPI_UINT64_T, MPI_UINT32_T };
    MPI_Type_create_struct(3, blocklen, disp, types, &dtype);
    MPI_Type_commit(&dtype);
    return dtype;
}

// ============================================================================
// Deterministic counts (no size messages / no Bcasts)
// ----------------------------------------------------------------------------
static inline int count_for_rank(int rank, uint64_t total_records, int world_size) {
    const uint64_t end   = (total_records * (uint64_t)(rank + 1)) / world_size;
    const uint64_t start = (total_records * (uint64_t) rank)      / world_size;
    return static_cast<int>(end - start);
}

// Size of partner's subtree at a given round (sender’s payload size)
static inline int partner_subtree_size(int partner_rank,
                                       int round,
                                       uint64_t total_records,
                                       int world_size)
{
    const int group = 1 << round;
    const int base  = (partner_rank / group) * group;
    int sum = 0;
    for (int k = 0; k < group; ++k) {
        sum += count_for_rank(base + k, total_records, world_size);
    }
    return sum;
}

// ============================================================================
// Pairwise log2(P) merge tree on IndexRec (no handshakes, no barriers)
// ============================================================================
static void pairwise_merge_tree(std::vector<IndexRec>& local_sorted_index,
                                int world_rank, int world_size,
                                uint64_t total_records,
                                MPI_Datatype MPI_IndexRec)
{
    std::vector<IndexRec> partner_buf;
    std::vector<IndexRec> concat;

    for (int round = 0; (1 << round) < world_size; ++round) {
        const int partner = world_rank ^ (1 << round);
        if (partner >= world_size) continue;

        const bool i_receive =
            ((world_rank & ((1 << (round + 1)) - 1)) == 0) && (world_rank < partner);

        if (i_receive) {
            const int expected = partner_subtree_size(partner, round, total_records, world_size);
            partner_buf.resize(expected);
            if (expected > 0) {
                MPI_Recv(partner_buf.data(), expected, MPI_IndexRec,
                         partner, /*tag*/ 700 + round, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
            if (expected == 0) {
                // nothing
            } else if (local_sorted_index.empty()) {
                local_sorted_index.swap(partner_buf);
            } else {
                const std::size_t mine_n = local_sorted_index.size();
                concat.resize(mine_n + partner_buf.size());
                std::memcpy(concat.data(), local_sorted_index.data(), mine_n * sizeof(IndexRec));
                std::memcpy(concat.data() + mine_n, partner_buf.data(), partner_buf.size() * sizeof(IndexRec));
                merge_records(concat.data(), 0, mine_n - 1, concat.size() - 1);
                local_sorted_index.swap(concat);
                std::vector<IndexRec>().swap(concat);
            }
            std::vector<IndexRec>().swap(partner_buf);
        } else {
            const int my_n = static_cast<int>(local_sorted_index.size());
            if (my_n > 0) {
                MPI_Send(local_sorted_index.data(), my_n, MPI_IndexRec,
                         partner, /*tag*/ 700 + round, MPI_COMM_WORLD);
            }
            local_sorted_index.clear();
            local_sorted_index.shrink_to_fit();
            break; // inactive for remaining rounds
        }
    }
}

// ============================================================================
// One-shot index distribution
// ----------------------------------------------------------------------------
// Idea: root (rank 0) scans the file once, fills exactly one vector per rank
// with that rank’s slice (contiguous by record index). As soon as a slice is
// complete (we reach its end record), root posts a single MPI_Isend of that
// vector to that rank. Non-root ranks pre-post a single MPI_Irecv for their
// expected slice size (computed deterministically), then wait for completion.
// This gives exactly one send/recv pair per rank, and still overlaps a bit
// because root sends each slice as soon as it finishes scanning it.
// ============================================================================

constexpr int TAG_FULL_SLICE = 650; // full IndexRec slice payload

/**
 * Root: build and send one full slice per rank (one Isend per rank).
 *
 * We **do not** allocate one giant IndexRec array; instead we keep
 * `per_rank[r]` vectors. Because records are assigned by index range,
 * each rank’s slice is contiguous in the scan, so we can send as soon
 * as we finish filling that vector.
 */
static void root_build_and_send_full_slices(const std::string& input_path,
                                            uint64_t           total_records,
                                            int                world_size,
                                            MPI_Datatype       MPI_IndexRec,
                                            std::vector<IndexRec>& out_local_slice)
{
    // 1) Open and mmap input (same pattern as your build_index_mmap).
    int fd = ::open(input_path.c_str(), O_RDONLY);
    if (fd < 0) { std::perror("[oneshot] open"); MPI_Abort(MPI_COMM_WORLD, 101); }
    struct stat st{};
    if (fstat(fd, &st) < 0) { std::perror("[oneshot] fstat"); close(fd); MPI_Abort(MPI_COMM_WORLD, 102); }
    const size_t file_sz = st.st_size;
    void* map = mmap(nullptr, file_sz, PROT_READ, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) { std::perror("[oneshot] mmap"); close(fd); MPI_Abort(MPI_COMM_WORLD, 103); }
    const char* data = static_cast<const char*>(map);

    // 2) Precompute per-rank ranges and reserve vectors at exact capacity.
    std::vector<int> slice_size(world_size);
    std::vector<uint64_t> start_idx(world_size), end_idx(world_size);
    for (int r = 0; r < world_size; ++r) {
        start_idx[r] = (total_records * (uint64_t) r)      / world_size;
        end_idx[r]   = (total_records * (uint64_t)(r + 1)) / world_size;
        slice_size[r] = static_cast<int>(end_idx[r] - start_idx[r]);
    }

    std::vector< std::vector<IndexRec> > per_rank(world_size);
    for (int r = 0; r < world_size; ++r) per_rank[r].reserve(slice_size[r]);

    // 3) Isend requests for ranks > 0; initialize null
    std::vector<MPI_Request> send_req(world_size, MPI_REQUEST_NULL);

    BENCH_START(build_index); // timing: parse + immediate sends when a slice completes

    // 4) Single pass over the file: fill slices in order.
    size_t pos = 0;
    uint64_t current_rank = 0;
    for (uint64_t i = 0; i < total_records; ++i) {
        // Move to the correct target rank based on i (indexes are contiguous)
        while (!(start_idx[current_rank] <= i && i < end_idx[current_rank])) {
            ++current_rank;
        }

        // Read header
        if (pos + sizeof(unsigned long) + sizeof(uint32_t) > file_sz) {
            std::cerr << "[oneshot] unexpected EOF at rec " << i << "\n";
            MPI_Abort(MPI_COMM_WORLD, 104);
        }
        const unsigned long key = *reinterpret_cast<const unsigned long*>(data + pos);
        const uint32_t      len = *reinterpret_cast<const uint32_t*>     (data + pos + sizeof(unsigned long));

        per_rank[current_rank].push_back(IndexRec{ key, (uint64_t)pos, len });

        // If we just completed a non-root rank's slice, send it now.
        if (i + 1 == end_idx[current_rank] && current_rank != 0) {
            const int n = slice_size[(int)current_rank];
            MPI_Isend(per_rank[current_rank].data(), n, MPI_IndexRec,
                      (int)current_rank, TAG_FULL_SLICE, MPI_COMM_WORLD, &send_req[(int)current_rank]);
        }

        pos += sizeof(unsigned long) + sizeof(uint32_t) + len;
    }

    BENCH_STOP(build_index);

    // 5) Root keeps its own slice locally
    out_local_slice.swap(per_rank[0]);

    // 6) Ensure all Isends completed before unmapping
    BENCH_START(distribute_index);
    for (int r = 1; r < world_size; ++r) {
        if (send_req[r] != MPI_REQUEST_NULL) {
            MPI_Wait(&send_req[r], MPI_STATUS_IGNORE);
            send_req[r] = MPI_REQUEST_NULL;
        }
    }
    BENCH_STOP(distribute_index);

    // 7) Clean up mapping
    munmap(map, file_sz);
    close(fd);
}

/**
 * Non-root: pre-post one Irecv for the full slice and wait for it.
 * Size is deterministic: floor(N*(r+1)/P) - floor(N*r/P).
 */
static void nonroot_recv_full_slice(int                my_rank,
                                    uint64_t           total_records,
                                    int                world_size,
                                    MPI_Datatype       MPI_IndexRec,
                                    std::vector<IndexRec>& out_local_slice)
{
    const int expected = count_for_rank(my_rank, total_records, world_size);
    out_local_slice.resize(expected);

    BENCH_START(distribute_index);
    MPI_Recv(out_local_slice.data(), expected, MPI_IndexRec,
         0, TAG_FULL_SLICE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    BENCH_STOP(distribute_index);
}

// ============================================================================
// Main
// ============================================================================
int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    int world_rank = 0, world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    Params params = parse_argv(argc, argv);
    if (params.n_threads > 0) omp_set_num_threads(params.n_threads);

    MPI_Datatype MPI_IndexRec = make_mpi_indexrec_type();

    BENCH_START(total_time);

    const uint64_t total_records = params.n_records;

    // ----------------- Phase 1: ensure input exists (rank 0) -----------------
    std::string input_path;
    if (world_rank == 0) {
        BENCH_START(generate_unsorted);
        input_path = generate_unsorted_file_mmap(params.n_records, params.payload_max);
        BENCH_STOP(generate_unsorted);
    }

    // ----------------- Phase 2: one-shot index distribution ------------------
    std::vector<IndexRec> local_index;

    if (world_rank == 0) {
        root_build_and_send_full_slices(
            input_path, total_records, world_size, MPI_IndexRec, local_index);
        // local_index now holds rank 0’s full slice (unsorted yet)
    } else {
        nonroot_recv_full_slice(
            world_rank, total_records, world_size, MPI_IndexRec, local_index);
    }

    // ----------------- Phase 3: local sort (OpenMP mergesort) ----------------
    BENCH_START(local_sort);
    #pragma omp parallel
    {
        #pragma omp single nowait
        mergesort_task(local_index.data(),
                       /*left=*/0,
                       /*right=*/local_index.empty() ? 0 : local_index.size() - 1,
                       /*cutoff=*/params.cutoff);
    }
    BENCH_STOP(local_sort);

    // ----------------- Phase 4: pairwise merge tree (IndexRec only) ----------
    BENCH_START(distributed_merge);
    pairwise_merge_tree(local_index, world_rank, world_size, total_records, MPI_IndexRec);
    BENCH_STOP(distributed_merge);

    // ----------------- Phase 5: final rewrite (rank 0) -----------------------
    if (world_rank == 0) {
        BENCH_START(rewrite_sorted);

        const std::string output_path =
            "files/sorted_" + std::to_string(params.n_records) + "_" +
            std::to_string(params.payload_max) + ".bin";

        IndexRec* final_index = (IndexRec*)std::malloc(local_index.size() * sizeof(IndexRec));
        if (!final_index) { std::fprintf(stderr, "[rank 0] malloc failed\n"); MPI_Abort(MPI_COMM_WORLD, 201); }
        std::memcpy(final_index, local_index.data(),
                    local_index.size() * sizeof(IndexRec));

        if (!rewrite_sorted_mmap(input_path, output_path, final_index, local_index.size())) {
            std::fprintf(stderr, "[rank 0] rewrite_sorted_mmap failed\n");
            MPI_Abort(MPI_COMM_WORLD, 202);
        }
        BENCH_STOP(rewrite_sorted);

        BENCH_START(check_if_sorted);
        if (!check_if_sorted_mmap(output_path, total_records)) {
            std::fprintf(stderr, "[rank 0] check_if_sorted_mmap FAILED\n");
            MPI_Abort(MPI_COMM_WORLD, 203);
        }
        BENCH_STOP(check_if_sorted);
    }

    BENCH_STOP(total_time);

    MPI_Type_free(&MPI_IndexRec);
    MPI_Finalize();
    return 0;
}
