// fastflow.cpp — Task‑graph MergeSort with FastFlow farm+feedback
// -----------------------------------------------------------------------------
//  Termination policy (v4)
//  • Workers free their task and normally return *parent*.
//  • When the root finishes, that worker returns **this‑>EOS** — the special
//    FastFlow end‑of‑stream token.  Unlike nullptr, EOS is actively delivered
//    on feedback channels, so the emitter can stop immediately.
//  • No extra sentinel, no global variables, no arena.
// -----------------------------------------------------------------------------
//  Build:
//      g++ -std=c++17 -O2 -Wall -ffast-math -pthread -I./fastflow
//          fastflow.cpp utils.o -o bin/fastflow
// -----------------------------------------------------------------------------

#include <ff/ff.hpp>
#include <ff/farm.hpp>
#include "utils.hpp"
#include <vector>

using namespace ff;

struct Task {
    std::size_t left, mid, right;
    bool        is_sort;
    Task*       parent;            // nullptr only for root
    bool        is_ready = false;  // for feedback
};

struct Emitter;                       // forward-declare
static Task* build_tasks(std::size_t, std::size_t, Task*, std::size_t, Emitter*);

static Record* g_base = nullptr;

/* Emitter -----------------------------------------------------------------*/
struct Emitter : ff_node_t<Task> {
    Emitter(std::size_t N, std::size_t cutoff) : N(N), cutoff(cutoff) {}

    Task* svc(Task* in) override {
        if (in == nullptr)             // FastFlow’s wake-up dummy
            return GO_ON;

        if (!in->is_ready){             // first children done
            in->is_ready = true; // mark as ready when returnd again
        }
        else {
            Task* parent = in->parent;
            ff_send_out(in);               // schedule merge
            if (!parent) { // root task enqueued
                return EOS;  // send EOS downstream
            }
        }
        return GO_ON;
    }

    int svc_init() override {
        build_tasks(0, N - 1, nullptr, cutoff, this); // root auto‑freed
        return 0;
    }

private:
    std::size_t N;
    std::size_t cutoff;
};

/* Build full binary task tree ---------------------------------------------*/
static Task* build_tasks(std::size_t l, std::size_t r, Task* parent,
                         std::size_t cutoff, Emitter* emitter) {
    Task* t = new Task{l,0,r,false,parent};
    if (r - l + 1 <= cutoff) {          // leaf → sort directly
        t->is_sort = true;
        emitter->ff_send_out(t);        // push leaf node directly
        return t;
    }
    std::size_t m = (l + r) / 2;
    t->mid    = m;
    build_tasks(l,   m, t, cutoff, emitter);
    build_tasks(m+1, r, t, cutoff, emitter);
    return t;
}



/* Worker ------------------------------------------------------------------*/
struct Worker : ff_node_t<Task> {
    Task* svc(Task* t) override {
        if (t->is_sort)
            sort_records(g_base + t->left, t->right - t->left + 1);
        else
            merge_records(g_base, t->left, t->mid, t->right);

        Task* parent = t->parent;   // capture before delete
        delete t;                   // free current task (root included)
        return parent;
    }
};

/* Main --------------------------------------------------------------------*/
int main(int argc, char** argv) {
    Params p = parse_argv(argc, argv);

    const std::size_t N      = p.n_records;
    const std::size_t cutoff = p.cutoff;
    const int nthreads       = p.n_threads > 0 ? p.n_threads : ff_numCores();

    Record* data = alloc_random_records(N, p.payload_max);
    g_base = data;

    BENCH_START(ff_farm_sort);
    Emitter emitter(N, cutoff);
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
    BENCH_STOP(ff_farm_sort);

    check_if_sorted(data, N);

    for (auto* w : workers) delete w;
    release_records(data, N);
    return 0;
}