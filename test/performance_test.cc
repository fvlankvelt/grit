#include <filesystem>
#include <iostream>

#include "graph.h"

#define N_BATCHES 1000
#define N_VERTICES_PER_BATCH 1000

using namespace std;

void TestPerformance() {
    std::filesystem::remove_all("/tmp/graphdb");
    Storage storage("/tmp/graphdb");

    Graph graph(storage);
    uint64_t log_idx = 1;
    auto started = chrono::duration_cast<chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
    std::cout << "Creating vertices and edges" << std::endl;
    int num_created = 0;
    for (int i = 0; i < N_BATCHES; i++) {
        WriteContext ctx(storage.db, log_idx++);
        auto txn = graph.OpenForWrite(ctx);
        for (ulong j = 0; j < N_VERTICES_PER_BATCH; j++) {
            ulong id = N_BATCHES * j + i;
            std::stringstream ss;
            ss << "label-" << j;
            VertexId vertex_id = {"comp", id};
            txn->AddVertex(vertex_id, ctx);
            txn->AddLabel(vertex_id, ss.str(), ctx);
            txn->AddLabel(vertex_id, "common", ctx);
            if (i > 0) {
                VertexId other_id = {"comp", 100 * (i - 1) + j};
                txn->AddEdge("sibling", vertex_id, other_id, ctx);
            }
            num_created++;
        }
        txn->Commit(ctx);
    }
    auto vertices_created = chrono::duration_cast<chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    auto duration = vertices_created - started;
    std::cout << "Created " << num_created << " vertices in " << N_BATCHES << " batches, " << duration / N_BATCHES <<
            " per batch" << std::endl;

    {
        WriteContext ctx(storage.db, log_idx++);
        auto slicer = graph.OpenSlicer(ctx);
        slicer->SliceIndex("common", ctx);
    }
    auto sliced = chrono::duration_cast<chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    std::cout << "Sliced common label (" << num_created << " vertices) in " << (sliced - vertices_created) << std::endl;

    std::cout << "Time-travelling" << std::endl;
    int num_results = 0;
    for (int i = 0; i < N_BATCHES; i++) {
        auto txn = graph.OpenForRead(2 * i + 2);
        std::stringstream ss;
        ss << "label-" << i;
        std::shared_ptr<IndexVertexIterator> vertices(txn->GetVerticesByLabel(ss.str(), "comp"));
        while (vertices->Valid()) {
            num_results++;
            vertices->Next();
        }
    }

    auto index_queried = chrono::duration_cast<chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    duration = index_queried - sliced;
    std::cout << "Queried " << num_results << " vertices in " << N_BATCHES << " queries: " << duration / N_BATCHES <<
            " per query" << std::endl;
}

int main() {
    TestPerformance();
}
