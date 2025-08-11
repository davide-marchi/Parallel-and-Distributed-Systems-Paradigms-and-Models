// mpi_pairwise_tree.cpp
// Build: mpic++ -O3 -std=c++20 -fopenmp mpi_pairwise_tree.cpp -o bin/mpi_pairwise_tree
// Run (Slurm srun example):
//   srun -N 4 -n 4 --cpus-per-task=8 ./bin/mpi_pairwise_tree -n 1000000 -p 256 -t 8 -c 10000

#include <mpi.h>
#include <omp.h>
#include <vector>
#include <string>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <iostream>

#include "utils.hpp" // <- provides: Params parse_argv(...), BENCH_* timers,
                     // IndexRec { key, offset, len }, sort_records, merge_records,
                     // generate_unsorted_file_mmap, build_index_mmap,
                     // rewrite_sorted_mmap, check_if_sorted_mmap

// -----------------------------------------------------------------------------
// OpenMP task-based mergesort (your pattern): recursively sort halves with tasks,
// use your utils.hpp: merge_records(...) for the merge and sort_records(...) for
// small ranges. We keep it exactly in the spirit of your single-node code.
// -----------------------------------------------------------------------------
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
        // Merge two *adjacent* sorted ranges in-place:
        // [left..mid] and [mid+1..right]
        merge_records(base, left, mid, right);
    } else {
        // Your fast small-range sorter (utils.hpp)
        sort_records(base + left, right - left + 1);
    }
}

// -----------------------------------------------------------------------------
// Create an MPI datatype that matches IndexRec so we can Scatter/Send/Recv it.
// -----------------------------------------------------------------------------
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

// -----------------------------------------------------------------------------
// Pairwise log2(P) merge tree on *sorted* IndexRec slices.
// - Each round s: partner = rank ^ (1<<s). Lower rank in each pair receives.
// - Receivers: recv partner slice, concatenate [mine | partner], then call
//   merge_records(base, 0, mine_end, total_end) to get one sorted slice.
// - Senders: send and become inactive.
// After log2(P) rounds, rank 0 holds the fully sorted index.
// -----------------------------------------------------------------------------
static void pairwise_merge_tree(std::vector<IndexRec>& local_sorted_index,
                                int world_rank, int world_size,
                                MPI_Datatype MPI_IndexRec)
{
    std::vector<IndexRec> partner_index; // buffer for partner data (when receiving)
    std::vector<IndexRec> concat;        // [mine | partner] before calling merge_records

    for (int round = 0; (1 << round) < world_size; ++round) {
        const int partner = world_rank ^ (1 << round);
        if (partner >= world_size) continue;

        // "Receiver" rule: lower rank in the 2^(round+1) block receives & merges
        const bool i_receive =
            ((world_rank & ((1 << (round + 1)) - 1)) == 0) && (world_rank < partner);

        // Exchange counts (number of IndexRec elements)
        int my_count = static_cast<int>(local_sorted_index.size());
        int partner_count = 0;
        MPI_Sendrecv(&my_count,     1, MPI_INT, partner, 100 + round,
                     &partner_count,1, MPI_INT, partner, 100 + round,
                     MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (i_receive) {
            // Receive partner's already-sorted slice (if non-empty)
            partner_index.resize(partner_count);
            if (partner_count > 0) {
                MPI_Recv(partner_index.data(), partner_count,
                         MPI_IndexRec, partner, 200 + round,
                         MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }

            // Trivial cases
            if (partner_count == 0) {
                // nothing to do
            } else if (local_sorted_index.empty()) {
                // I had nothing; partner slice becomes mine
                local_sorted_index.swap(partner_index);
            } else {
                // Concatenate [mine | partner] adjacently, then in-place merge
                const std::size_t mine_n = local_sorted_index.size();
                concat.resize(mine_n + partner_index.size());
                std::memcpy(concat.data(),
                            local_sorted_index.data(),
                            mine_n * sizeof(IndexRec));
                std::memcpy(concat.data() + mine_n,
                            partner_index.data(),
                            partner_index.size() * sizeof(IndexRec));
                // merge [0..mine_n-1] with [mine_n..(mine_n+partner_n-1)]
                merge_records(concat.data(),
                              /*left=*/0,
                              /*mid=*/mine_n - 1,
                              /*right=*/concat.size() - 1);
                local_sorted_index.swap(concat);
                std::vector<IndexRec>().swap(concat);       // release capacity
            }
            std::vector<IndexRec>().swap(partner_index);      // release capacity
        } else {
            // Sender: send my sorted slice (if any) and become inactive
            if (my_count > 0) {
                MPI_Send(local_sorted_index.data(), my_count,
                         MPI_IndexRec, partner, 200 + round, MPI_COMM_WORLD);
            }
            local_sorted_index.clear();
            local_sorted_index.shrink_to_fit();
            break; // done taking part in the remaining rounds
        }

        // Optional clarity sync (not required for correctness)
        MPI_Barrier(MPI_COMM_WORLD);
    }
}

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    const int world_rank = []{ int r; MPI_Comm_rank(MPI_COMM_WORLD, &r); return r; }();
    const int world_size = []{ int s; MPI_Comm_size(MPI_COMM_WORLD, &s); return s; }();

    // 0) Parse CLI using the function you already provide in utils.hpp
    Params params = parse_argv(argc, argv); // (n_records, payload_max, n_threads, cutoff, etc.)

    // Configure OpenMP threads from your CLI (if that’s how your parse_argv works)
    if (params.n_threads > 0) omp_set_num_threads(params.n_threads);

    // Create the MPI datatype for IndexRec once
    MPI_Datatype MPI_IndexRec = make_mpi_indexrec_type();

    BENCH_START(total_time);

    // -------------------------------------------------------------------------
    // Phase 1 (rank 0 only): Generate (or reuse) and build the full index.
    // Everyone else waits at the broadcast below.
    // -------------------------------------------------------------------------
    std::string input_path;      // created by rank 0 (generate_unsorted_file_mmap)
    uint64_t    total_records=0; // broadcast to all ranks
    IndexRec*   root_index = nullptr; // only valid on rank 0

    if (world_rank == 0) {
        // 1A) Create / load the input file (your helper)
        BENCH_START(generate_unsorted);
        input_path = generate_unsorted_file_mmap(params.n_records, params.payload_max);
        BENCH_STOP(generate_unsorted);

        // 1B) Build the full IndexRec array for the *whole* file (your helper)
        BENCH_START(build_index);
        // Signature in your codebase returns a heap buffer (malloc) with N entries
        root_index = build_index_mmap(input_path, params.n_records);
        BENCH_STOP(build_index);

        if (!root_index) {
            std::fprintf(stderr, "[rank 0] build_index_mmap failed\n");
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
        total_records = params.n_records;
    }

    // Share the total record count so every rank can size its receive buffers
    MPI_Bcast(&total_records, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    // -------------------------------------------------------------------------
    // Phase 2: Distribute the index using a single MPI_Scatterv of IndexRec.
    // Each rank pre-computes how many elements it will receive.
    // -------------------------------------------------------------------------
    // Compute my slice [start_idx, end_idx) by *record count* (not bytes).
    const uint64_t start_idx = (total_records * world_rank)     / world_size;
    const uint64_t end_idx   = (total_records * (world_rank+1)) / world_size;
    const int      local_n   = static_cast<int>(end_idx - start_idx);

    std::vector<IndexRec> local_index(local_n); // my slice buffer

    std::vector<int> counts, displs;
    if (world_rank == 0) {
        counts.resize(world_size);
        displs.resize(world_size);
        for (int r = 0; r < world_size; ++r) {
            const uint64_t s = (total_records * r) / world_size;
            const uint64_t e = (total_records * (r+1)) / world_size;
            counts[r] = static_cast<int>(e - s);
            displs[r] = static_cast<int>(s);
        }
    }

    BENCH_START(distribute_index);
    MPI_Scatterv(
        /*sendbuf (root only):*/ root_index,
        /*sendcounts:         */ world_rank==0 ? counts.data() : nullptr,
        /*displs:             */ world_rank==0 ? displs.data() : nullptr,
        /*sendtype:           */ MPI_IndexRec,
        /*recvbuf:            */ local_index.data(),
        /*recvcount:          */ local_n,
        /*recvtype:           */ MPI_IndexRec,
        /*root:               */ 0, MPI_COMM_WORLD);
    BENCH_STOP(distribute_index);

    // Root no longer needs the full index
    if (world_rank == 0) { std::free(root_index); root_index = nullptr; }

    // -------------------------------------------------------------------------
    // Phase 3: Local sort (OpenMP tasks) on *each* rank.
    // We sort our contiguous local_index range in-place by key.
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // Phase 4: Distributed log2(P) pairwise merge tree (IndexRec only).
    // This function is identical for all ranks; receivers grow their slice
    // by merging partner data; senders ship and finish.
    // -------------------------------------------------------------------------
    BENCH_START(distributed_merge);
    pairwise_merge_tree(local_index, world_rank, world_size, MPI_IndexRec);
    BENCH_STOP(distributed_merge);

    // -------------------------------------------------------------------------
    // Phase 5 (rank 0): Final rewrite of the FULLY sorted output file.
    // We pass a malloc'd copy because your rewrite_sorted_mmap may free() it.
    // -------------------------------------------------------------------------
    if (world_rank == 0) {
        BENCH_START(rewrite_sorted);

        // Prepare destination path (same pattern you used elsewhere is fine too)
        const std::string output_path =
            "files/sorted_" + std::to_string(params.n_records) + "_" +
            std::to_string(params.payload_max) + ".bin";

        // Make a C-style buffer that rewrite_sorted_mmap can free safely.
        IndexRec* final_index = (IndexRec*)std::malloc(local_index.size() * sizeof(IndexRec));
        if (!final_index) {
            std::fprintf(stderr, "[rank 0] malloc for final_index failed\n");
            MPI_Abort(MPI_COMM_WORLD, 3);
        }
        std::memcpy(final_index, local_index.data(),
                    local_index.size() * sizeof(IndexRec));

        // Your helper: rewrite the *actual records* in sorted order to output_path
        if (!rewrite_sorted_mmap(input_path, output_path, final_index, local_index.size())) {
            std::fprintf(stderr, "[rank 0] rewrite_sorted_mmap failed\n");
            MPI_Abort(MPI_COMM_WORLD, 4);
        }
        BENCH_STOP(rewrite_sorted);

        // Optional: correctness check
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
