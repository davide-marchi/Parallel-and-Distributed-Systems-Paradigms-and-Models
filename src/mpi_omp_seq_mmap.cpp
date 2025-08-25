// mpi_pairwise_tree.cpp  —  Log2(P) pairwise tree, minimal comms
// Build: mpic++ -O3 -std=c++20 -fopenmp mpi_pairwise_tree.cpp -o bin/mpi_pairwise_tree
// Run (Slurm): srun -N 4 -n 4 --cpus-per-task=8 ./bin/mpi_pairwise_tree -n 10000000 -p 8 -t 8 -c 10000

#include <mpi.h>
#include <omp.h>
#include <vector>
#include <string>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <iostream>

#include "utils.hpp" // parse_argv, BENCH_* timers, IndexRec, sort_records, merge_records,
                     // generate_unsorted_file_mmap, build_index_mmap, rewrite_sorted_mmap, check_if_sorted_mmap

// ============================================================================
// OpenMP task-based mergesort for IndexRec (reuses your utils.hpp primitives).
// Splits [left..right] recursively; for small spans uses sort_records; for
// larger spans spawns tasks and merges halves via merge_records (in-place).
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
        // Merge two adjacent sorted ranges: [left..mid], [mid+1..right]
        merge_records(base, left, mid, right);
    } else {
        sort_records(base + left, right - left + 1);
    }
}

// ============================================================================
// Create an MPI datatype describing IndexRec so we can Scatter/Send/Recv it.
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
// Deterministic counts (to avoid sending sizes)
// ----------------------------------------------------------------------------
// Our initial split is by record INDEX: rank r gets
//   count_for_rank(r) = floor(N*(r+1)/P) - floor(N*r/P)
// In merge round 'round' (0-based), each sender holds exactly the sum of the
// counts in its size-2^round subtree. This lets the receiver precompute how
// many IndexRec elements to expect from its partner without a Sendrecv.
// ============================================================================

// Count of records initially assigned to rank r (given total N, world size P).
static inline int count_for_rank(int rank, uint64_t total_records, int world_size) {
    const uint64_t end   = (total_records * (uint64_t)(rank + 1)) / world_size;
    const uint64_t start = (total_records * (uint64_t) rank)      / world_size;
    return static_cast<int>(end - start);
}

// Total size of the partner's subtree at a given round.
// At round R, subtree size = 2^R; the partner's block begins at
//   base = (partner / group) * group
// We sum counts for that whole block.
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
// Pairwise log2(P) merge tree on sorted IndexRec slices (NO size handshakes).
// Each round pairs ranks with partner = rank ^ (1<<round).
//  - Receivers (lower rank at block boundary) compute partner's size, recv it,
//    concatenate [mine | partner] and call merge_records on the concatenated
//    array to keep the result sorted.
//  - Senders send their entire slice and become inactive.
// Tags: payload uses (200 + round). No Barrier in the loop.
// ============================================================================
static void pairwise_merge_tree(std::vector<IndexRec>& local_sorted_index,
                                int world_rank, int world_size,
                                uint64_t total_records,
                                MPI_Datatype MPI_IndexRec)
{
    std::vector<IndexRec> partner_buffer;  // holds received partner slice
    std::vector<IndexRec> concat_buffer;   // [mine | partner] for inplace merge

    for (int round = 0; (1 << round) < world_size; ++round) {
        const int partner = world_rank ^ (1 << round);
        if (partner >= world_size) continue;

        // Receiver rule: lower rank at each 2^(round+1) block boundary.
        const bool i_am_receiver =
            ((world_rank & ((1 << (round + 1)) - 1)) == 0) && (world_rank < partner);

        if (i_am_receiver) {
            // How many records will my partner send this round?
            const int expect_from_partner =
                partner_subtree_size(partner, round, total_records, world_size);

            partner_buffer.resize(expect_from_partner);
            if (expect_from_partner > 0) {
                MPI_Recv(partner_buffer.data(), expect_from_partner, MPI_IndexRec,
                         partner, 200 + round, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }

            // Merge my slice with partner's slice.
            if (expect_from_partner == 0) {
                // nothing to merge
            } else if (local_sorted_index.empty()) {
                local_sorted_index.swap(partner_buffer);
            } else {
                const std::size_t mine_n = local_sorted_index.size();
                concat_buffer.resize(mine_n + partner_buffer.size());
                std::memcpy(concat_buffer.data(),
                            local_sorted_index.data(),
                            mine_n * sizeof(IndexRec));
                std::memcpy(concat_buffer.data() + mine_n,
                            partner_buffer.data(),
                            partner_buffer.size() * sizeof(IndexRec));
                // In-place merge adjacent sorted ranges in concat_buffer.
                merge_records(concat_buffer.data(),
                              /*left=*/0,
                              /*mid=*/mine_n - 1,
                              /*right=*/concat_buffer.size() - 1);
                local_sorted_index.swap(concat_buffer);
                std::vector<IndexRec>().swap(concat_buffer);   // free capacity
            }
            std::vector<IndexRec>().swap(partner_buffer);       // free capacity
        } else {
            // I am the sender in this pair: send my whole slice and stop participating.
            const int my_count = static_cast<int>(local_sorted_index.size());
            if (my_count > 0) {
                MPI_Send(local_sorted_index.data(), my_count, MPI_IndexRec,
                         partner, 200 + round, MPI_COMM_WORLD);
            }
            local_sorted_index.clear();
            local_sorted_index.shrink_to_fit();
            break; // inactive for remaining rounds
        }
        // No Barrier here — some ranks stop participating after sending.
    }
}

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    int world_rank = 0, world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // Parse CLI using your existing function (every rank does it).
    Params params = parse_argv(argc, argv);
    if (params.n_threads > 0) omp_set_num_threads(params.n_threads);

    // Build a matching MPI datatype for IndexRec once.
    MPI_Datatype MPI_IndexRec = make_mpi_indexrec_type();

    BENCH_START(total_time);

    // ------------------------------------------------------------------------
    // Phase 1 (rank 0 only): ensure input exists and build the full IndexRec.
    // Other ranks do not touch the file; they only receive their index slice.
    // ------------------------------------------------------------------------
    std::string input_path;   // only used by rank 0 later when rewriting
    IndexRec*   full_index_root = nullptr;   // malloc'ed by build_index_mmap on root

    if (world_rank == 0) {
        BENCH_START(generate_unsorted);
        input_path = generate_unsorted_file_mmap(params.n_records, params.payload_max);
        BENCH_STOP(generate_unsorted);

        BENCH_START(build_index);
        full_index_root = build_index_mmap(input_path, params.n_records);
        BENCH_STOP(build_index);

        if (!full_index_root) {
            std::fprintf(stderr, "[rank 0] build_index_mmap failed\n");
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
    }

    // We avoid a Bcast of N on purpose (every rank trusts params.n_records).
    const uint64_t total_records = params.n_records;

    // ------------------------------------------------------------------------
    // Phase 2: Scatter the global index by record-count slices (no Bcast).
    // Every rank can compute its own receive count deterministically.
    // ------------------------------------------------------------------------
    const uint64_t my_start_idx = (total_records * (uint64_t)world_rank)     / world_size;
    const uint64_t my_end_idx   = (total_records * (uint64_t)(world_rank+1)) / world_size;
    const int      my_slice_n   = static_cast<int>(my_end_idx - my_start_idx);

    std::vector<IndexRec> local_index(my_slice_n);

    std::vector<int> send_counts, send_displs;
    if (world_rank == 0) {
        send_counts.resize(world_size);
        send_displs.resize(world_size);
        for (int r = 0; r < world_size; ++r) {
            const uint64_t s = (total_records * (uint64_t)r)     / world_size;
            const uint64_t e = (total_records * (uint64_t)(r+1)) / world_size;
            send_counts[r] = static_cast<int>(e - s);
            send_displs[r] = static_cast<int>(s);
        }
    }

    BENCH_START(distribute_index);
    MPI_Scatterv(
        /*sendbuf (root only)*/ full_index_root,
        /*sendcounts*/           world_rank==0 ? send_counts.data() : nullptr,
        /*displs*/               world_rank==0 ? send_displs.data() : nullptr,
        /*sendtype*/             MPI_IndexRec,
        /*recvbuf*/              local_index.data(),
        /*recvcount*/            my_slice_n,
        /*recvtype*/             MPI_IndexRec,
        /*root*/                 0, MPI_COMM_WORLD);
    BENCH_STOP(distribute_index);

    if (world_rank == 0) { std::free(full_index_root); full_index_root = nullptr; }

    // ------------------------------------------------------------------------
    // Phase 3: Local sort (OpenMP tasks) of my contiguous IndexRec slice.
    // ------------------------------------------------------------------------
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

    // ------------------------------------------------------------------------
    // Phase 4: log2(P) pairwise merge tree (IndexRec only, no Sendrecv).
    // ------------------------------------------------------------------------
    BENCH_START(distributed_merge);
    pairwise_merge_tree(local_index, world_rank, world_size, total_records, MPI_IndexRec);
    BENCH_STOP(distributed_merge);

    // ------------------------------------------------------------------------
    // Phase 5 (rank 0): rewrite final sorted file using your mmap helper.
    // We pass a malloc'ed copy if your rewrite takes ownership and free()s it.
    // ------------------------------------------------------------------------
    if (world_rank == 0) {
        BENCH_START(rewrite_sorted);

        const std::string output_path =
            "files/sorted_" + std::to_string(params.n_records) + "_" +
            std::to_string(params.payload_max) + ".bin";

        IndexRec* final_index = (IndexRec*)std::malloc(local_index.size() * sizeof(IndexRec));
        if (!final_index) {
            std::fprintf(stderr, "[rank 0] malloc for final_index failed\n");
            MPI_Abort(MPI_COMM_WORLD, 3);
        }
        std::memcpy(final_index, local_index.data(),
                    local_index.size() * sizeof(IndexRec));

        if (!rewrite_sorted_mmap(input_path, output_path, final_index, local_index.size())) {
            std::fprintf(stderr, "[rank 0] rewrite_sorted_mmap failed\n");
            MPI_Abort(MPI_COMM_WORLD, 4);
        }
        BENCH_STOP(rewrite_sorted);

        // Optional verification
        BENCH_START(check_if_sorted);
        if (!check_if_sorted_mmap(output_path, total_records)) {
            std::fprintf(stderr, "[rank 0] check_if_sorted_mmap FAILED\n");
            MPI_Abort(MPI_COMM_WORLD, 5);
        }
        BENCH_STOP(check_if_sorted);
    }

    BENCH_STOP(total_time);

    MPI_Type_free(&MPI_IndexRec);
    MPI_Finalize();
    return 0;
}
