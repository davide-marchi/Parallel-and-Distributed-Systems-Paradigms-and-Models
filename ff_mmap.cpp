// ff_mmap.cpp — Task-graph MergeSort with FastFlow farm+feedback + overlapped index build
// -----------------------------------------------------------------------------
//  Overlap strategy:
//  • Emitter sends a special BuildIndex task FIRST.
//  • One worker runs build_index_mmap(…, notify_every=cutoff, gate) progressively.
//  • Other workers can start on MergeSort leaves but will gate (wait_once)
//    until g_gate.filled >= R+1, then sort. Merges need no extra waits.
// -----------------------------------------------------------------------------
//  Build:
//      g++ -std=c++20 -O3 -Wall -ffast-math -pthread -I./fastflow -MMD -MP ff_mmap.cpp -o bin/fastflow
// -----------------------------------------------------------------------------

#include "utils.hpp"
#include <ff/ff.hpp>
#include <ff/farm.hpp>

using namespace ff;

/*============================== Shared state ===============================*/
static IndexRec*     g_base          = nullptr;
static std::string   g_unsorted_file;
static std::size_t   g_N             = 0;
static int           g_notify_every  = 0;     // we use opt.cutoff
static ProgressGate  g_gate;                  // from utils.hpp

/*=============================== Task model ================================*/
struct Task {
    enum Kind { Sort, Merge, BuildIndex } kind;
    std::size_t left, mid, right;
    Task*       parent;            // nullptr only for root
    bool        is_ready = false;  // emitter uses this to detect "both children done"
};

struct Emitter;  // fwd
static void build_tasks(std::size_t, std::size_t, Task*, std::size_t, Emitter*);

/*=============================== Emitter ==================================*/
struct Emitter : ff_node_t<Task> {
    Emitter(std::size_t N, std::size_t cutoff) : N(N), cutoff(cutoff) {}

    Task* svc(Task* in) override {
        if (in == nullptr)             // FastFlow’s wake-up dummy
            return GO_ON;

        // We only ever get Merge tasks back here (parents). BuildIndex returns GO_ON from worker.
        if (!in->is_ready) {           // first child finished
            in->is_ready = true;
        } else {
            Task* parent = in->parent;
            ff_send_out(in);           // schedule the merge on workers
            if (!parent)               // root merge enqueued
                return EOS;            // close the stream
        }
        return GO_ON;
    }

    int svc_init() override {
        // 0) Send the progressive index builder task FIRST (ensures one worker runs it)
        auto* b = new Task{ Task::BuildIndex, 0, 0, 0, /*parent=*/nullptr, /*is_ready=*/false };
        ff_send_out(b);

        // 1) Build full mergesort task tree (leaves as Sort, internal nodes as Merge)
        build_tasks(0, N - 1, /*parent=*/nullptr, cutoff, this);  // root auto-freed later
        return 0;
    }

private:
    std::size_t N;
    std::size_t cutoff;
};

/*========================= Build full binary tree ==========================*/
static void build_tasks(std::size_t l, std::size_t r, Task* parent,
                        std::size_t cutoff, Emitter* emitter) {
    const std::size_t span = r - l + 1;
    if (span <= cutoff) {
        // Leaf → sort directly (gated in worker before sort_records)
        emitter->ff_send_out(new Task{ Task::Sort, l, 0, r, /*parent=*/parent, /*is_ready=*/false });
        return;
    }
    std::size_t m = (l + r) / 2;
    // Internal node → create a Merge task; it will be enqueued after both children return
    Task* t = new Task{ Task::Merge, l, m, r, /*parent=*/parent, /*is_ready=*/false };
    build_tasks(l,   m, t, cutoff, emitter);
    build_tasks(m+1, r, t, cutoff, emitter);
}

/*================================ Worker ===================================*/
struct Worker : ff_node_t<Task> {
    Task* svc(Task* t) override {
        switch (t->kind) {
            case Task::BuildIndex: {
                // Progressive index builder; notifies g_gate every g_notify_every elements (and at end).
                build_index_mmap(g_unsorted_file, g_base, g_N, g_notify_every, &g_gate);
                delete t;
                return GO_ON;  // nothing to return to emitter
            }

            case Task::Sort: {
                // Wait until the slice [L..R] has been fully indexed, then sort in place.
                const std::size_t L = t->left, R = t->right;
                g_gate.wait_until(R + 1);
                sort_records(g_base + L, R - L + 1);
                Task* parent = t->parent;
                delete t;
                return parent; // notify emitter that this child is done
            }

            case Task::Merge: {
                merge_records(g_base, t->left, t->mid, t->right);
                Task* parent = t->parent;
                delete t;
                return parent; // bubble up
            }
        }
        // Should never get here
        delete t;
        return GO_ON;
    }
};

/*================================= Main ====================================*/
int main(int argc, char** argv)
{
    Params opt = parse_argv(argc, argv);

    BENCH_START(total_time);

    // Phase 1 – streaming generation --------------------------------------
    BENCH_START(generate_unsorted);
    std::string unsorted_file = generate_unsorted_file_mmap(opt.n_records, opt.payload_max);
    BENCH_STOP(generate_unsorted);

    // Phase 2+3 – overlap index build + sort -------------------------------
    BENCH_START(index_plus_sort);

    const int nthreads = opt.n_threads > 0 ? opt.n_threads : ff_numCores();

    if (nthreads <= 1) {
        // sequential fallback: build index normally, then std::sort (as before)
        IndexRec* idx = build_index_mmap(unsorted_file, opt.n_records); // uses allocating overload
        sort_records(idx, opt.n_records);
        // stash into globals only so the rest of the file (Phase 4/5) remains identical
        g_base = idx;
    } else {
        // Allocate index with malloc because rewrite_sorted_mmap() will free(idx)
        IndexRec* idx = static_cast<IndexRec*>(std::malloc(opt.n_records * sizeof(IndexRec)));
        if (!idx) { std::perror("malloc"); std::exit(1); }

        // Set shared state for workers
        g_base          = idx;
        g_unsorted_file = unsorted_file;
        g_N             = opt.n_records;
        g_notify_every  = opt.cutoff;        // wake frequency
        g_gate.reset();

        // Farm: 1 emitter + (nthreads-1) workers (your original sizing)
        Emitter emitter(opt.n_records, opt.cutoff);
        std::vector<ff_node*> workers;
        for (int i = 0; i < nthreads - 1; ++i) workers.push_back(new Worker());

        ff_farm farm;
        farm.add_emitter(&emitter);
        farm.add_workers(workers);
        farm.remove_collector();
        farm.wrap_around();

        if (farm.run_and_wait_end() < 0) {
            error("FastFlow execution failed\n");
            return 1;
        }
        for (auto* w : workers) delete w;
    }

    BENCH_STOP(index_plus_sort);

    // Phase 4 – rewrite sorted file ---------------------------------------
    BENCH_START(rewrite_sorted);
    rewrite_sorted_mmap(unsorted_file, "files/sorted_"
                        + std::to_string(opt.n_records) + "_"
                        + std::to_string(opt.payload_max) + ".bin", g_base, opt.n_records);
    BENCH_STOP(rewrite_sorted);

    // Phase 5 – verify -----------------------------------------------------
    BENCH_START(check_if_sorted);
    check_if_sorted_mmap("files/sorted_"
                         + std::to_string(opt.n_records) + "_"
                         + std::to_string(opt.payload_max) + ".bin", opt.n_records);
    BENCH_STOP(check_if_sorted);

    BENCH_STOP(total_time);
    return 0;
}
