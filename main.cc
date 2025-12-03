#include <cassert>
#include <iostream>
#include <memory>
#include <sstream>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"

#include "model.h"
#include "service.h"
#include "transaction.h"
#include "graph.h"

int main() {
    // std::filesystem::remove_all("/tmp/graphdb");
    Graph graph("/tmp/graphdb");
    std::unique_ptr<WriteTransaction> createVertex(graph.OpenForWrite());
    VertexId vertexId = {"c", 1};
    createVertex->AddVertex(vertexId);
    createVertex->AddLabel(vertexId, "ye-label");
    uint64_t createVertexTx = createVertex->Commit();

    std::cout << "Fetching vertices by label" << std::endl;
    {
        std::set<std::string> expected;
        expected.insert("c:1");

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead());
        std::unique_ptr<IndexVertexIterator> vertices(
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

    std::unique_ptr<WriteTransaction> addEdge(graph.OpenForWrite());
    VertexId otherId = {"c", 2};
    addEdge->AddVertex(otherId);
    addEdge->AddEdge("peer", vertexId, otherId);
    uint64_t addEdgeTx = addEdge->Commit();

    std::cout << "Fetching edges from vertex" << std::endl;
    {
        std::set<std::string> expected;
        expected.insert("c:2");

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead());
        std::unique_ptr<EdgeIterator> edges(read->GetEdges(vertexId, "peer", OUT));
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
        std::unique_ptr<VertexIterator> bytype(read->GetVerticesByType("c"));
        while (bytype->Valid()) {
            const VertexId& id = bytype->Get();
            std::stringstream ss;
            ss << id.type << ":" << id.id;
            found.insert(ss.str());
            bytype->Next();
        }

        assert(found == expected);
    }

    std::unique_ptr<WriteTransaction> removeEdge(graph.OpenForWrite());
    removeEdge->RemoveEdge("peer", vertexId, otherId);
    removeEdge->Commit();

    std::cout << "Fetching edges from vertex - timetravel to before edge removal" << std::endl;
    {
        std::set<std::string> expected;
        expected.insert("c:2");

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead(addEdgeTx));
        std::unique_ptr<EdgeIterator> edges(read->GetEdges(vertexId, "peer", OUT));
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
        std::unique_ptr<EdgeIterator> edges(read->GetEdges(vertexId, "peer", OUT));
        while (edges->Valid()) {
            const Edge& edge = edges->Get();
            std::stringstream ss;
            ss << edge.otherId.type << ":" << edge.otherId.id;
            found.insert(ss.str());
            edges->Next();
        }

        assert(found == expected);
    }

    std::unique_ptr<WriteTransaction> addEdgeRollback(graph.OpenForWrite());
    addEdgeRollback->AddEdge("peer", vertexId, otherId);
    addEdgeRollback->Rollback();

    std::cout << "Fetching edges from vertex - after edge addition & rollback" << std::endl;
    {
        std::set<std::string> expected;

        std::set<std::string> found;
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead());
        std::unique_ptr<EdgeIterator> edges(read->GetEdges(vertexId, "peer", OUT));
        while (edges->Valid()) {
            const Edge& edge = edges->Get();
            std::stringstream ss;
            ss << edge.otherId.type << ":" << edge.otherId.id;
            found.insert(ss.str());
            edges->Next();
        }

        assert(found == expected);
    }

    auto raftIdStr = std::getenv("RAFT_ID");
    int raftId = raftIdStr ? atoi(raftIdStr) : 0;

    std::vector<RaftPeer> peers;
    auto raftPeerStr = std::getenv("RAFT_PEERS");
    if (raftPeerStr) {
        std::string allPeersStr = raftPeerStr;
        while (!allPeersStr.empty()) {
            int comma = allPeersStr.find(",");
            std::string peerStr = comma > 0 ? allPeersStr.substr(0, comma) : allPeersStr;
            allPeersStr = comma > 0 ? allPeersStr.substr(comma + 1) : "";

            int colon = peerStr.find(':');
            int peerId = std::stoi(peerStr.substr(0, colon));
            peers.push_back({peerId, peerStr.substr(colon + 1)});
        }
    }

    RunServer(8000, 8001, raftId, peers);
}
