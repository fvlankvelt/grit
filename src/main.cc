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
    auto grpcPortStr = std::getenv("GRPC_PORT");
    int grpcPort = grpcPortStr ? std::stoi(grpcPortStr) : 0;
    auto raftPortStr = std::getenv("RAFT_PORT");
    int raftPort = raftPortStr ? std::stoi(raftPortStr) : 0;
    auto raftIdStr = std::getenv("RAFT_ID");
    int raftId = raftIdStr ? atoi(raftIdStr) : 0;
    auto storagePathStr = std::getenv("STORAGE_PATH");
    std::string storagePath = storagePathStr ? storagePathStr : "/tmp/graphdb";

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
            std::cout << " ADDING PEER " << peerId << " @ " << peerStr.substr(colon + 1) << std::endl;
            peers.push_back({peerId, peerStr.substr(colon + 1)});
        }
    }

    RunServer(grpcPort, raftPort, raftId, peers, storagePath);
}
