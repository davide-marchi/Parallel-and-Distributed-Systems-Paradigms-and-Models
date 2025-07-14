// fastflow.cpp — Parallel merge‑sort using FastFlow with feedback
// -----------------------------------------------------------------------------
//  Strategy (task‑graph merge‑sort)
//  ───────────────────────────────
//  • Build the *entire* binary merge‑sort tree up‑front.  Each node is a Task.
//    −  Leaf  (is_sort==true):  sub‑array length ≤ cutoff  →  sort_records.
//    −  Internal (is_sort==false): merges two halves when both children finish.
//  • Initial work list = all leaves; they’re pushed to a FastFlow farm.
//  • Workers return a pointer to *parent* when they finish.  The farm has a
//    feedback channel (wrap_around).  The emitter decrements parent->remain;
//    when it reaches 0 the parent task is submitted.  When the root returns
//    nullptr the emitter terminates.
//
//  Build / Run
//  ───────────
//      make fastflow           # see Makefile
//      ./bin/fastflow -n 1000000 -p 256 -t 8 -c 4096
// -----------------------------------------------------------------------------

#include <ff/farm.hpp>
#include <atomic>
#include <vector>
#include "utils.hpp"

using namespace ff;

/*---------------------------------------------------------------------------*/
/*  Task structure                                                            */
/*---------------------------------------------------------------------------*/
struct Task {
    std::size_t left;      // inclusive
    std::size_t mid;       // split point (valid only for merge tasks)
    std::size_t right;     // inclusive
    bool        is_sort;   // true: sort_records; false: merge_records
    Task*       parent;    // nullptr for the root
    std::atomic<int> remain{0}; // children left to finish (only for merge)
};

/* Global pointer to the record array so workers can access it --------------*/
static Record* g_base = nullptr;

/*---------------------------------------------------------------------------*/
/*  Recursively build the task tree                                          */
/*---------------------------------------------------------------------------*/
static Task* build_tasks(std::size_t left, std::size_t right,
                         Task* parent, std::size_t cutoff,
                         std::vector<Task*>& leaves,
                         std::vector<Task*>& pool) {
    std::size_t len = right - left + 1;
    Task* node      = new Task{left, 0, right, /*is_sort*/ false, parent};
    pool.push_back(node);

    if (len <= cutoff) {
        node->is_sort = true;
        leaves.push_back(node);
        return node;
    }

    std::size_t mid = (left + right) / 2;
    node->mid       = mid;
    node->remain    = 2;                 // wait for both children

    build_tasks(left, mid, node, cutoff, leaves, pool);
    build_tasks(mid + 1, right, node, cutoff, leaves, pool);
    return node;
}

/*---------------------------------------------------------------------------*/
/*  Emitter with feedback                                                     */
/*---------------------------------------------------------------------------*/
class Emitter : public ff_node {
public:
    Emitter(const std::vector<Task*>& initial_leaves) : leaves(initial_leaves) {}

    int svc_init() override {
        // Push all initial leaf tasks into the farm
        for (Task* t : leaves) ff_send_out(t);
        return 0;
    }

    void* svc(void* msg) override {
        Task* parent = static_cast<Task*>(msg); // nullptr ⇒ root finished
        if (!parent) return EOS;                // terminate pipeline

        if (--parent->remain == 0) {
            ff_send_out(parent);                // schedule merge task
        }
        return GO_ON;
    }
private:
    const std::vector<Task*>& leaves;
};

/*---------------------------------------------------------------------------*/
/*  Worker                                                                    */
/*---------------------------------------------------------------------------*/
class Worker : public ff_node {
public:
    void* svc(void* ptr) override {
        Task* t = static_cast<Task*>(ptr);
        if (t->is_sort) {
            sort_records(g_base + t->left, t->right - t->left + 1);
        } else {
            merge_records(g_base, t->left, t->mid, t->right);
        }
        return t->parent; // may be nullptr (root)
    }
};

/*---------------------------------------------------------------------------*/
/*  Main driver                                                               */
/*---------------------------------------------------------------------------*/
int main(int argc, char** argv) {
    Params opt = parse_argv(argc, argv);
    const std::size_t N      = opt.n_records;
    const std::size_t cutoff = opt.cutoff > 0 ? opt.cutoff : 8192;
    const int nthreads       = opt.n_threads > 0 ? opt.n_threads : ff_numCores();

    Record* data = alloc_random_records(N, opt.payload_max);
    g_base        = data;

    /* Build task graph ------------------------------------------------------*/
    std::vector<Task*> leaves;           // tasks ready at time‑0
    std::vector<Task*> pool;             // to own all Task pointers for cleanup
    Task* root = build_tasks(0, N - 1, nullptr, cutoff, leaves, pool);

    /* FastFlow farm with feedback -------------------------------------------*/
    Emitter E(leaves);
    std::vector<std::unique_ptr<ff_node>> W;
    for (int i = 0; i < nthreads; ++i) W.emplace_back(std::make_unique<Worker>());

    ff_Farm<> farm;
    farm.add_emitter(&E);
    farm.add_workers(W);
    farm.wrap_around();      // enable feedback channel (workers → emitter)
    farm.remove_collector(); // no collector stage

    bench_start("fastflow");
    if (farm.run_and_wait_end() < 0) {
        error("FastFlow farm run failed\n");
        return 1;
    }
    bench_end("fastflow");

    check_if_sorted(data, N);

    /* Clean up --------------------------------------------------------------*/
    for (Task* t : pool) delete t;
    release_records(data, N);
    return 0;
}
