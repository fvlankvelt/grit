
#include "storage.h"

#include <filesystem>

#include "graph.h"

void TestSlicing() {
    std::filesystem::remove_all("/tmp/graphdb");
    Storage storage("/tmp/graphdb");

    Graph graph(storage);

    std::cout << "Initializing data" << std::endl;
    uint64_t log_idx = 1;
    {
        WriteContext ctx(storage.db, log_idx++);
        auto tx = graph.OpenForWrite(ctx);
        tx->AddVertex({"c", 1}, ctx);
        tx->AddLabel({"c", 1}, "label", ctx);
        tx->AddVertex({"c", 2}, ctx);
        tx->AddLabel({"c", 2}, "label", ctx);
        tx->Commit(ctx);
    }

    std::cout << "Opening txn with some mutations" << std::endl;
    {
        WriteContext ctx(storage.db, log_idx++);
        auto tx = graph.OpenForWrite(ctx);
        tx->RemoveVertex({"c", 1}, ctx);
        tx->AddVertex({"c", 3}, ctx);
    }

    std::cout << "Slicing" << std::endl;
    {
        WriteContext ctx(storage.db, log_idx++);
        auto slicer = graph.OpenSlicer(ctx);
        slicer->SliceIndex("label", ctx);
    }

    std::cout << "More mutations" << std::endl;
    {
        WriteContext ctx(storage.db, log_idx++);
        auto tx = graph.OpenForWrite(ctx);
        tx->AddVertex({"c", 4}, ctx);
        tx->AddLabel({"c", 4}, "label", ctx);
        tx->RemoveVertex({"c", 1}, ctx);
        tx->Commit(ctx);
    }

    std::cout << "Fetching after slicing" << std::endl;
    {
        auto tx = graph.OpenForRead();
        auto vertices = tx->GetVerticesByLabel("label", "c");
        std::set<uint64_t> ids;
        while (vertices->Valid()) {
            ids.insert(vertices->Get().id);
            vertices->Next();
        }
        assert(ids.size() == 2);
        assert(ids.contains(2));
        assert(ids.contains(4));
    }
}

int main() {
    TestSlicing();
}