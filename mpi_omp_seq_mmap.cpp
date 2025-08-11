// mpi_omp_seq_mmap.cpp
// Build: mpic++ -O3 -std=c++20 -fopenmp mpi_omp_seq_mmap.cpp -o bin/mpi_omp_seq_mmap
// Run:   mpirun -np 4 ./bin/mpi_omp_seq_mmap -n 1000000 -p 256 -t 8 -c 10000

#include <mpi.h>
#include <omp.h>
#include <vector>
#include <string>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "utils.hpp"   // Params, parse_argv, IndexRec, sort_records, merge_records,
                       // generate_unsorted_file_mmap, build_index_mmap,
                       // rewrite_sorted_mmap, check_if_sorted_mmap, BENCH_* macros

/* ---- same OpenMP task merge-sort you already have ---- */
static inline void mergesort_task(IndexRec* base,
                                  std::size_t left,
                                  std::size_t right,
                                  int cutoff)
{
    if (left >= right) return;
    std::size_t mid = (left + right) / 2;

    if (static_cast<int>(right - left) > cutoff) {
        #pragma omp task shared(base)
        mergesort_task(base, left,  mid,       cutoff);

        #pragma omp task shared(base)
        mergesort_task(base, mid+1, right,     cutoff);

        #pragma omp taskwait
        merge_records(base, left, mid, right); // utils.hpp
    } else {
        sort_records(base + left, right - left + 1); // utils.hpp
    }
}

/* ---- MPI datatype for IndexRec ---- */
static inline MPI_Datatype make_mpi_indexrec_type() {
    MPI_Datatype dt;
    int          blocklen[3] = {1, 1, 1};
    MPI_Aint     disp[3], base;
    IndexRec     probe{};
    MPI_Get_address(&probe, &base);
    MPI_Get_address(&probe.key,    &disp[0]);
    MPI_Get_address(&probe.offset, &disp[1]);
    MPI_Get_address(&probe.len,    &disp[2]);
    disp[0] -= base; disp[1] -= base; disp[2] -= base;
    MPI_Datatype types[3] = { MPI_UNSIGNED_LONG, MPI_UINT64_T, MPI_UINT32_T };
    MPI_Type_create_struct(3, blocklen, disp, types, &dt);
    MPI_Type_commit(&dt);
    return dt;
}

/* ---- merge two sorted vectors into OUT ---- */
static inline void merge_index_vec(const std::vector<IndexRec>& A,
                                   const std::vector<IndexRec>& B,
                                   std::vector<IndexRec>&       OUT)
{
    OUT.clear();
    OUT.reserve(A.size() + B.size());
    std::size_t i=0, j=0;
    while (i < A.size() && j < B.size()) {
        if (A[i].key <= B[j].key) OUT.push_back(A[i++]);
        else                       OUT.push_back(B[j++]);
    }
    while (i < A.size()) OUT.push_back(A[i++]);
    while (j < B.size()) OUT.push_back(B[j++]);
}

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    int rank=0, P=1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &P);

    // 0) parse CLI using your existing function (already validates)
    Params opt = parse_argv(argc, argv);

    BENCH_START(total_time);

    std::string unsorted_file;
    uint64_t    N_total = 0;

    if (rank == 0) {
        // Phase 1: generate (or reuse) input
        BENCH_START(generate_unsorted);
        unsorted_file = generate_unsorted_file_mmap(opt.n_records, opt.payload_max);
        BENCH_STOP(generate_unsorted);

        // Phase 2: build full index on rank 0
        BENCH_START(build_index);
        IndexRec* idx_root = build_index_mmap(unsorted_file, opt.n_records);
        BENCH_STOP(build_index);
        if (!idx_root) { std::fprintf(stderr,"[r0] build_index_mmap failed\n"); MPI_Abort(MPI_COMM_WORLD,2); }

        N_total = opt.n_records;

        // Let receivers compute their recv counts
        MPI_Bcast(&N_total, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

        // Scatter slices
        std::vector<int> counts(P), displs(P);
        for (int r=0; r<P; ++r) {
            uint64_t s = (N_total * r) / P;
            uint64_t e = (N_total * (r+1)) / P;
            counts[r] = static_cast<int>(e - s);
            displs[r] = static_cast<int>(s);
        }
        MPI_Datatype MPI_IndexRec = make_mpi_indexrec_type();

        int my_n = counts[0];
        std::vector<IndexRec> my_idx(my_n);

        BENCH_START(distribute_index);
        MPI_Scatterv(idx_root, counts.data(), displs.data(), MPI_IndexRec,
                     my_idx.data(), my_n, MPI_IndexRec,
                     0, MPI_COMM_WORLD);
        BENCH_STOP(distribute_index);

        std::free(idx_root);

        // Phase 3: local sort on every rank (OpenMP tasks)
        BENCH_START(local_sort);
        if (opt.n_threads > 0) omp_set_num_threads(opt.n_threads);
        #pragma omp parallel
        {
            #pragma omp single nowait
            mergesort_task(my_idx.data(), 0, my_idx.empty()?0:my_idx.size()-1, opt.cutoff);
        }
        BENCH_STOP(local_sort);

        // Phase 4: log2(P) pairwise merge tree (index only)
        BENCH_START(distributed_merge);
        std::vector<IndexRec> partner_idx, merged;
        for (int s=0; (1<<s)<P; ++s) {
            int partner = rank ^ (1<<s);
            if (partner >= P) continue;
            bool i_receive = ( (rank & ((1<<(s+1))-1)) == 0 ) && (rank < partner);

            int my_sz = static_cast<int>(my_idx.size());
            int pr_sz = 0;
            MPI_Sendrecv(&my_sz, 1, MPI_INT, partner, 100+s,
                         &pr_sz,  1, MPI_INT, partner, 100+s,
                         MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            if (i_receive) {
                partner_idx.resize(pr_sz);
                if (pr_sz) MPI_Recv(partner_idx.data(), pr_sz, MPI_IndexRec, partner, 200+s, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                merge_index_vec(my_idx, partner_idx, merged);
                my_idx.swap(merged);
                std::vector<IndexRec>().swap(merged);
                std::vector<IndexRec>().swap(partner_idx);
            } else {
                if (my_sz) MPI_Send(my_idx.data(), my_sz, MPI_IndexRec, partner, 200+s, MPI_COMM_WORLD);
                my_idx.clear(); my_idx.shrink_to_fit();
                break;
            }
            MPI_Barrier(MPI_COMM_WORLD); // simple & clear
        }
        BENCH_STOP(distributed_merge);

        // Phase 5: final rewrite (rank 0 only)
        BENCH_START(rewrite_sorted);
        // rewrite_sorted_mmap frees(idx) -> pass a malloc'd buffer
        IndexRec* final_idx = (IndexRec*)std::malloc(my_idx.size()*sizeof(IndexRec));
        if (!final_idx) { std::fprintf(stderr,"[r0] malloc failed\n"); MPI_Abort(MPI_COMM_WORLD,3); }
        std::memcpy(final_idx, my_idx.data(), my_idx.size()*sizeof(IndexRec));

        const std::string sorted_file = "files/sorted_" +
            std::to_string(opt.n_records) + "_" + std::to_string(opt.payload_max) + ".bin";

        if (!rewrite_sorted_mmap(unsorted_file, sorted_file, final_idx, my_idx.size())) {
            std::fprintf(stderr,"[r0] rewrite_sorted_mmap failed\n");
            MPI_Abort(MPI_COMM_WORLD,4);
        }
        BENCH_STOP(rewrite_sorted);

        // Phase 6: verify (will unlink the file in your utils)
        BENCH_START(check_if_sorted);
        if (!check_if_sorted_mmap(sorted_file, N_total)) {
            std::fprintf(stderr,"[r0] check_if_sorted_mmap FAILED\n");
            MPI_Abort(MPI_COMM_WORLD,5);
        }
        BENCH_STOP(check_if_sorted);

        BENCH_STOP(total_time);
        MPI_Type_free(&MPI_IndexRec);
    } else {
        // non-root
        MPI_Bcast(&N_total, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

        uint64_t s_idx = (N_total * rank)     / P;
        uint64_t e_idx = (N_total * (rank+1)) / P;
        int my_n = static_cast<int>(e_idx - s_idx);

        std::vector<IndexRec> my_idx(my_n);
        MPI_Datatype MPI_IndexRec = make_mpi_indexrec_type();

        BENCH_START(distribute_index);
        MPI_Scatterv(nullptr, nullptr, nullptr, MPI_IndexRec,
                     my_idx.data(), my_n, MPI_IndexRec,
                     0, MPI_COMM_WORLD);
        BENCH_STOP(distribute_index);

        BENCH_START(local_sort);
        if (opt.n_threads > 0) omp_set_num_threads(opt.n_threads);
        #pragma omp parallel
        {
            #pragma omp single nowait
            mergesort_task(my_idx.data(), 0, my_idx.empty()?0:my_idx.size()-1, opt.cutoff);
        }
        BENCH_STOP(local_sort);

        BENCH_START(distributed_merge);
        std::vector<IndexRec> partner_idx, merged;
        for (int s=0; (1<<s)<P; ++s) {
            int partner = rank ^ (1<<s);
            if (partner >= P) continue;
            bool i_receive = ( (rank & ((1<<(s+1))-1)) == 0 ) && (rank < partner);

            int my_sz = static_cast<int>(my_idx.size());
            int pr_sz = 0;
            MPI_Sendrecv(&my_sz, 1, MPI_INT, partner, 100+s,
                         &pr_sz,  1, MPI_INT, partner, 100+s,
                         MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            if (i_receive) {
                partner_idx.resize(pr_sz);
                if (pr_sz) MPI_Recv(partner_idx.data(), pr_sz, MPI_IndexRec, partner, 200+s, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                merge_index_vec(my_idx, partner_idx, merged);
                my_idx.swap(merged);
                std::vector<IndexRec>().swap(merged);
                std::vector<IndexRec>().swap(partner_idx);
            } else {
                if (my_sz) MPI_Send(my_idx.data(), my_sz, MPI_IndexRec, partner, 200+s, MPI_COMM_WORLD);
                my_idx.clear(); my_idx.shrink_to_fit();
                break;
            }
            MPI_Barrier(MPI_COMM_WORLD);
        }
        BENCH_STOP(distributed_merge);

        BENCH_STOP(total_time);
        MPI_Type_free(&MPI_IndexRec);
    }

    MPI_Finalize();
    return 0;
}
