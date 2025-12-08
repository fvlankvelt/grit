#include <filesystem>
#include <iostream>

#include "graph.h"

#define N_TXNS 1000
#define N_VERTICES_PER_TXN 1000

int main() {
    std::filesystem::remove_all("/tmp/graphdb");
    Graph graph("/tmp/graphdb");

    auto started = chrono::duration_cast<chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
    std::cout << "Creating vertices and edges" << std::endl;
    int num_created = 0;
    for (int i = 0; i < N_TXNS; i++) {
        auto txn = graph.OpenForWrite();
        for (int j = 0; j < N_VERTICES_PER_TXN; j++) {
            int id = 100 * j + i;
            std::stringstream ss;
            ss << "label-" << j;
            VertexId vertex_id = {"comp", id};
            txn->AddVertex(vertex_id);
            txn->AddLabel(vertex_id, ss.str());
            txn->AddLabel(vertex_id, "common");
            if (i > 0) {
                VertexId other_id = {"comp", 100 * (i - 1) + j};
                txn->AddEdge("sibling", vertex_id, other_id);
            }
            num_created++;
        }
        txn->Commit();
    }
    auto vertices_created = chrono::duration_cast<chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
    auto duration = vertices_created - started;
    std::cout << "Created " << num_created << " vertices in " << duration / N_TXNS << " per txn" << std::endl;

    std::cout << "Time-travelling" << std::endl;
    int num_results = 0;
    for (int i = 0; i < N_TXNS; i++) {
        auto txn = graph.OpenForRead(2 * i + 2);
        std::stringstream ss;
        ss << "label-" << i;
        std::shared_ptr<IndexVertexIterator> vertices(txn->GetVerticesByLabel(ss.str(), "comp"));
        while (vertices->Valid()) {
            num_results++;
            vertices->Next();
        }
    }

    auto index_queried = chrono::duration_cast<chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
    duration = index_queried - vertices_created;
    std::cout << "Queried " << num_results << " vertices in " << N_TXNS << " queries: " << duration/ N_TXNS << " per query" << std::endl;
}
