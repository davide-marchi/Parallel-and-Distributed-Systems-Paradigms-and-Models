// fastflow.cpp — Task‑graph merge‑sort with FastFlow farm+feedback
// -----------------------------------------------------------------------------
//  Build (example rule in Makefile):
//      CXXFLAGS += -I./fastflow -pthread -std=c++20 -O3 -Wall -ffast-math
//      bin/fastflow: fastflow.cpp utils.o …
//
//  Run:
//      ./bin/fastflow  -n 1000000  -p 256  -t 8  -c 4096
// -----------------------------------------------------------------------------
//  Algorithm recap
//  ───────────────
//  1. Build the full binary merge‑sort tree once.  Leaves where
//        (segment_len ≤ cutoff)  →  Task::is_sort == true.
//  2. Push all leaves to a FastFlow farm (workers).  Each worker either:
//        • sorts its segment (sort_records)  or
//        • merges two sorted halves (merge_records).
//     When done it returns the pointer to its *parent* task (or nullptr if
//     it just completed the root).
//  3. The farm is in *wrap‑around* mode, so completion messages arrive back at
//     the emitter, which decrements parent->remain; when that reaches 0 the
//     emitter submits the parent (merge) task.  When it receives nullptr it
//     emits EOS, terminating the farm.
//  4. No collector stage is needed.
// -----------------------------------------------------------------------------

#include "utils.hpp"
#include <ff/ff.hpp>
#include <ff/farm.hpp>
#include <vector>
#include <atomic>

using namespace ff;

/*---------------------------------------------------------------------------*/
/*  Task object                                                              */
/*---------------------------------------------------------------------------*/
struct Task {
    std::size_t left;      // inclusive
    std::size_t mid;       // split point (meaningful only for merge)
    std::size_t right;     // inclusive
    bool        is_sort;   // true ⇒ sort_records, false ⇒ merge_records
    Task*       parent;    // nullptr for root
    std::atomic<int> remain{0}; // children not yet finished (for merge)
};

static Record* g_base = nullptr;         // data array shared by workers

/*---------------------------------------------------------------------------*/
/*  Recursively create tasks                                                 */
/*---------------------------------------------------------------------------*/
static Task* build_tasks(std::size_t left, std::size_t right,
                         Task* parent, std::size_t cutoff,
                         std::vector<Task*>& ready,
                         std::vector<Task*>& arena)
{
    std::size_t len = right - left + 1;
    Task* t = new Task{left, 0, right, /*is_sort*/ false, parent};
    arena.push_back(t);

    if (len <= cutoff) {
        t->is_sort = true;
        ready.push_back(t);
        return t;
    }

    std::size_t mid = (left + right) / 2;
    t->mid    = mid;
    t->remain = 2;

    build_tasks(left,     mid, t, cutoff, ready, arena);
    build_tasks(mid + 1, right, t, cutoff, ready, arena);
    return t;
}

/*---------------------------------------------------------------------------*/
/*  Worker node (typed)                                                      */
/*---------------------------------------------------------------------------*/
struct Worker : ff_node_t<Task> {
    Task* svc(Task* task) override {
        if (task->is_sort) {
            sort_records(g_base + task->left, task->right - task->left + 1);
        } else {
            merge_records(g_base, task->left, task->mid, task->right);
        }
        return task->parent;           // may be nullptr (root)
    }
};

/*---------------------------------------------------------------------------*/
/*  Emitter / feedback handler                                               */
/*---------------------------------------------------------------------------*/
struct Emitter : ff_monode_t<Task> {
    explicit Emitter(const std::vector<Task*>& init) : initial(init) {}

    Task* svc(Task* in) override {
        if (in == nullptr) {                // first activation → push leaves
            for (Task* t : initial)
                ff_send_out(t, -1, 0);      // -1 chan id, ondemand sched
            return GO_ON;
        }

        if (in == nullptr) return EOS;      // (shouldn’t happen)
        if (in == (Task*)nullptr) return EOS; // root completed

        if (--in->remain == 0)
            ff_send_out(in, -1, 0);
        return GO_ON;
    }
private:
    const std::vector<Task*>& initial;
};

/*---------------------------------------------------------------------------*/
/*  Main                                                                     */
/*---------------------------------------------------------------------------*/
int main(int argc, char** argv)
{
    Params opt = parse_argv(argc, argv);

    const std::size_t N      = opt.n_records;
    const std::size_t cutoff = opt.cutoff ? opt.cutoff : 8192;
    const int nthreads       = opt.n_threads > 0 ? opt.n_threads : ff_numCores();

    Record* data = alloc_random_records(N, opt.payload_max);
    g_base        = data;

    // Build task DAG
    std::vector<Task*> ready;     // leaves ready to run
    std::vector<Task*> arena;     // all tasks for later delete
    Task* root = build_tasks(0, N - 1, nullptr, cutoff, ready, arena);
    (void)root;                   // used for termination only

    // FastFlow farm
    Emitter emitter(ready);

    std::vector<ff_node*> workers;
    workers.reserve(nthreads);
    for (int i = 0; i < nthreads; ++i)
        workers.push_back(new Worker());

    ff_farm farm;
    farm.add_emitter(&emitter);
    farm.add_workers(workers);
    farm.remove_collector();
    farm.wrap_around();
    farm.set_scheduling_ondemand(2); // 2‑slot ondemand (good default)

    BENCH_START(parallel_fastflow_merge_sort);
    if (farm.run_and_wait_end() < 0) {
        error("FastFlow execution failed\n");
        return 1;
    }
    BENCH_STOP(parallel_fastflow_merge_sort);

    check_if_sorted(data, N);

    // Release resources
    for (auto* w : workers) delete w;
    for (Task* t : arena) delete t;
    release_records(data, N);
    return 0;
}
