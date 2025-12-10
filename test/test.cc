
#include <filesystem>

#include "graph.h"

void testLocal() {
    ulong log_idx = 0;

    std::filesystem::remove_all("/tmp/graphdb");
    Graph graph("/tmp/graphdb");
    std::unique_ptr<WriteTransaction> createVertex(graph.OpenForWrite(log_idx++));
    VertexId vertexId = {"c", 1};
    createVertex->AddVertex(vertexId, log_idx++);
    createVertex->AddLabel(vertexId, "ye-label", log_idx++);
    uint64_t createVertexTx = createVertex->Commit(log_idx++);

    std::cout << "Fetching vertices by label" << std::endl;
    {
        std::set<std::string> expected;
        expected.insert("c:1");

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead());
        std::shared_ptr vertices(
            read->GetVerticesByLabel("ye-label", "c"));
        while (vertices->Valid()) {
            const VertexId& id = vertices->Get();
            std::stringstream ss;
            ss << id.type << ":" << id.id;
            found.insert(ss.str());
            vertices->Next();
        }

        assert(found == expected);
    }

    std::unique_ptr<WriteTransaction> addEdge(graph.OpenForWrite(log_idx++));
    VertexId otherId = {"c", 2};
    addEdge->AddVertex(otherId, log_idx++);
    addEdge->AddEdge("peer", vertexId, otherId, log_idx++);
    uint64_t addEdgeTx = addEdge->Commit(log_idx++);

    std::cout << "Fetching edges from vertex" << std::endl;
    {
        std::set<std::string> expected;
        expected.insert("c:2");

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead());
        std::shared_ptr edges(read->GetEdges(vertexId, "peer", OUT));
        while (edges->Valid()) {
            const Edge& edge = edges->Get();
            std::stringstream ss;
            ss << edge.otherId.type << ":" << edge.otherId.id;
            found.insert(ss.str());
            edges->Next();
        }

        assert(found == expected);
    }

    std::cout << "Fetching vertices by type - timetravel to initial" << std::endl;
    {
        std::set<std::string> expected;
        expected.insert("c:1");

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead(createVertexTx));
        std::shared_ptr bytype(read->GetVerticesByType("c"));
        while (bytype->Valid()) {
            const VertexId& id = bytype->Get();
            std::stringstream ss;
            ss << id.type << ":" << id.id;
            found.insert(ss.str());
            bytype->Next();
        }

        assert(found == expected);
    }

    std::unique_ptr<WriteTransaction> removeEdge(graph.OpenForWrite(log_idx++));
    removeEdge->RemoveEdge("peer", vertexId, otherId, log_idx++);
    removeEdge->Commit(log_idx++);

    std::cout << "Fetching edges from vertex - timetravel to before edge removal" << std::endl;
    {
        std::set<std::string> expected;
        expected.insert("c:2");

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead(addEdgeTx));
        std::shared_ptr edges(read->GetEdges(vertexId, "peer", OUT));
        while (edges->Valid()) {
            const Edge& edge = edges->Get();
            std::stringstream ss;
            ss << edge.otherId.type << ":" << edge.otherId.id;
            found.insert(ss.str());
            edges->Next();
        }

        assert(found == expected);
    }

    std::cout << "Fetching edges from vertex - after edge removal" << std::endl;
    {
        std::set<std::string> expected;

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead());
        std::shared_ptr edges(read->GetEdges(vertexId, "peer", OUT));
        while (edges->Valid()) {
            const Edge& edge = edges->Get();
            std::stringstream ss;
            ss << edge.otherId.type << ":" << edge.otherId.id;
            found.insert(ss.str());
            edges->Next();
        }

        assert(found == expected);
    }

    std::unique_ptr<WriteTransaction> addEdgeRollback(graph.OpenForWrite(log_idx++));
    addEdgeRollback->AddEdge("peer", vertexId, otherId, log_idx++);
    addEdgeRollback->Rollback(log_idx++);

    std::cout << "Fetching edges from vertex - after edge addition & rollback" << std::endl;
    {
        std::set<std::string> expected;

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead());
        std::shared_ptr edges(read->GetEdges(vertexId, "peer", OUT));
        while (edges->Valid()) {
            const Edge& edge = edges->Get();
            std::stringstream ss;
            ss << edge.otherId.type << ":" << edge.otherId.id;
            found.insert(ss.str());
            edges->Next();
        }

        assert(found == expected);
    }
}

int main() {
    testLocal();
}