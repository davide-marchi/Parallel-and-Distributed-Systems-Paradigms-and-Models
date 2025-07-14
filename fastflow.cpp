// fastflow.cpp — Task‑graph MergeSort using FastFlow farm+feedback (debug version)
// -----------------------------------------------------------------------------
//  This build fixes termination: FastFlow does **not** deliver bare nullptr
//  across channels, it interprets it as EOS at the *worker* side, so the
//  emitter never receives that message.  We now use a dedicated static
//  sentinel `EOS_TASK` to signal completion of the root task.
// -----------------------------------------------------------------------------
//  Compile:
//      g++ -std=c++17 -O2 -Wall -ffast-math -pthread -I./fastflow \
//          fastflow.cpp utils.o -o bin/fastflow
// -----------------------------------------------------------------------------

#include <ff/ff.hpp>
#include <ff/farm.hpp>
#include <atomic>
#include <vector>
#include <iostream>
#include "utils.hpp"

using namespace ff;

/*---------------------------------------------------------------------------*/
/*  Task node                                                                 */
/*---------------------------------------------------------------------------*/
struct Task {
    std::size_t left;
    std::size_t mid;
    std::size_t right;
    bool        is_sort;
    Task*       parent;
    std::atomic<int> remain{0};
};

static Record* g_base = nullptr;
static Task    EOS_TASK;               // sentinel passed from workers to emitter

/*---------------------------------------------------------------------------*/
/*  Build binary task tree                                                   */
/*---------------------------------------------------------------------------*/
static Task* build_tasks(std::size_t left, std::size_t right,
                         Task* parent, std::size_t cutoff,
                         std::vector<Task*>& ready,
                         std::vector<Task*>& arena)
{
    Task* node = new Task{left, 0, right, /*is_sort*/ false, parent};
    arena.push_back(node);

    if (right - left + 1 <= cutoff) {
        node->is_sort = true;
        ready.push_back(node);
        return node;
    }

    std::size_t mid = (left + right) / 2;
    node->mid    = mid;
    node->remain = 2;

    build_tasks(left,     mid, node, cutoff, ready, arena);
    build_tasks(mid + 1, right, node, cutoff, ready, arena);
    return node;
}

/*---------------------------------------------------------------------------*/
/*  Worker                                                                   */
/*---------------------------------------------------------------------------*/
struct Worker : ff_node_t<Task> {
    Task* svc(Task* task) override {
        const int id = ff_getThreadID();
        if (task->is_sort) {
            std::cout << "[Worker " << id << "] sort  (" << task->left << "," << task->right << ")\n";
            sort_records(g_base + task->left, task->right - task->left + 1);
        } else {
            std::cout << "[Worker " << id << "] merge (" << task->left << "," << task->mid << "," << task->right << ")\n";
            merge_records(g_base, task->left, task->mid, task->right);
        }
        // Signal completion: root -> EOS_TASK, others -> parent
        return task->parent ? task->parent : &EOS_TASK;
    }
};

/*---------------------------------------------------------------------------*/
/*  Emitter                                                                  */
/*---------------------------------------------------------------------------*/
struct Emitter : ff_node_t<Task> {
    explicit Emitter(const std::vector<Task*>& init) : initial(init) {}

    Task* svc(Task* in) override {
        if (!started) {
            started = true;
            std::cout << "[Emitter] pushing " << initial.size() << " leaf tasks\n";
            for (Task* t : initial) ff_send_out(t);
            return GO_ON;
        }

        if (in == &EOS_TASK) {
            std::cout << "[Emitter] root completed → EOS\n";
            return EOS;
        }

        int left = --in->remain;
        std::cout << "[Emitter] parent (" << in->left << "," << in->right << ") remain=" << left << "\n";
        if (left == 0) {
            std::cout << "[Emitter] scheduling merge (" << in->left << "," << in->right << ")\n";
            ff_send_out(in);
        }
        return GO_ON;
    }
private:
    bool started = false;
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

    std::cout << "=========== FastFlow merge‑sort debug run ===========\n";
    std::cout << "Records    : " << N              << "\n";
    std::cout << "Payload max: " << opt.payload_max << "\n";
    std::cout << "Cutoff     : " << cutoff         << "\n";
    std::cout << "Threads    : " << nthreads       << "\n";

    Record* data = alloc_random_records(N, opt.payload_max);
    g_base        = data;

    // Build task graph
    std::vector<Task*> leaves;
    std::vector<Task*> arena;
    build_tasks(0, N - 1, nullptr, cutoff, leaves, arena);

    std::cout << "Leaves     : " << leaves.size() << "\n";
    std::cout << "Total tasks: " << arena.size()  << "\n";

    // FastFlow farm creation
    Emitter emitter(leaves);
    std::vector<ff_node*> workers;
    workers.reserve(nthreads);
    for (int i = 0; i < nthreads; ++i) workers.push_back(new Worker());

    ff_farm farm;
    farm.add_emitter(&emitter);
    farm.add_workers(workers);
    farm.remove_collector();
    farm.wrap_around();

    std::cout << "[Main] starting farm…\n";
    if (farm.run_and_wait_end() < 0) {
        error("FastFlow execution failed\n");
        return 1;
    }

    std::cout << "[Main] verifying…\n";
    check_if_sorted(data, N);
    std::cout << "[Main] array is sorted ✔️\n";

    // Cleanup
    for (auto* w : workers) delete w;
    for (Task* t : arena)   delete t;
    release_records(data, N);
    return 0;
}
