#include <filesystem>

#include "graph.h"

void testLocal() {
    std::filesystem::remove_all("/tmp/graphdb");
    Storage storage("/tmp/graphdb");

    Graph graph(storage);

    VertexId vertexId = {"c", 1};

    ulong log_idx = 1;
    uint64_t createVertexTx;
    {
        WriteContext ctx(storage.db, log_idx);
        std::unique_ptr<WriteTransaction> createVertex(graph.OpenForWrite(ctx));
        createVertex->AddVertex(vertexId, ctx);
        createVertex->AddLabel(vertexId, "ye-label", ctx);
        createVertex->Commit(ctx);
        createVertexTx = createVertex->GetTxId();
    }

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

    VertexId otherId = {"c", 2};

    uint64_t addEdgeTx;
    {
        WriteContext edgeCtx(storage.db, log_idx++);
        std::unique_ptr<WriteTransaction> addEdge(graph.OpenForWrite(edgeCtx));
        addEdge->AddVertex(otherId, edgeCtx);
        addEdge->AddEdge("peer", vertexId, otherId, edgeCtx);
        addEdge->Commit(edgeCtx);
        addEdgeTx = addEdge->GetTxId();
    }

    std::cout << "Fetching edges from vertex" << std::endl;
    {
        std::set<std::string> expected;
        expected.insert("c:2");

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead());
        std::shared_ptr edges(read->GetEdges(vertexId, "peer", OUT));
        while (edges->Valid()) {
            const Edge &edge = edges->Get();
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
            const VertexId &id = bytype->Get();
            std::stringstream ss;
            ss << id.type << ":" << id.id;
            found.insert(ss.str());
            bytype->Next();
        }

        assert(found == expected);
    }

    {
        WriteContext removeEdgeCtx(storage.db, log_idx++);
        std::unique_ptr<WriteTransaction> removeEdge(graph.OpenForWrite(removeEdgeCtx));
        removeEdge->RemoveEdge("peer", vertexId, otherId, removeEdgeCtx);
        removeEdge->Commit(removeEdgeCtx);
    }

    std::cout << "Fetching edges from vertex - timetravel to before edge removal" << std::endl;
    {
        std::set<std::string> expected;
        expected.insert("c:2");

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead(addEdgeTx));
        std::shared_ptr edges(read->GetEdges(vertexId, "peer", OUT));
        while (edges->Valid()) {
            const Edge &edge = edges->Get();
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
            const Edge &edge = edges->Get();
            std::stringstream ss;
            ss << edge.otherId.type << ":" << edge.otherId.id;
            found.insert(ss.str());
            edges->Next();
        }

        assert(found == expected);
    }

    {
        WriteContext rollbackCtx(storage.db, log_idx++);
        std::unique_ptr<WriteTransaction> addEdgeRollback(graph.OpenForWrite(rollbackCtx));
        addEdgeRollback->AddEdge("peer", vertexId, otherId, rollbackCtx);
        addEdgeRollback->Rollback(rollbackCtx);
    }

    std::cout << "Fetching edges from vertex - after edge addition & rollback" << std::endl;
    {
        std::set<std::string> expected;

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead());
        std::shared_ptr edges(read->GetEdges(vertexId, "peer", OUT));
        while (edges->Valid()) {
            const Edge &edge = edges->Get();
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
