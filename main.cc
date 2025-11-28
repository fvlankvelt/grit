#include <cassert>
#include <iostream>
#include <memory>
#include <sstream>

#include "rocksdb/db.h"
#include "storage.pb.h"

struct VertexId {
    std::string type;
    long id;
};

enum Direction { IN, OUT };

struct Edge {
    VertexId vertexId;
    VertexId otherId;
    std::string label;
    Direction direction;
};

struct Storage {
    rocksdb::DB* db;
    rocksdb::ColumnFamilyHandle* _default;
    rocksdb::ColumnFamilyHandle* index;
    rocksdb::ColumnFamilyHandle* vertices;
    rocksdb::ColumnFamilyHandle* edges;
    // rocksdb::ColumnFamilyHandle* labels;
};

template <class T>
class EntryIterator {
   public:
    EntryIterator(rocksdb::Iterator* upstream, const std::string& prefix)
        : upstream(upstream), prefixStr(prefix), prefix(prefixStr) {}

    const T& Get() const { return current; }

    bool Valid() const { return valid && upstream->Valid(); }

    void Next() {
        upstream->Next();
        Update();
    }

   protected:
    virtual void populate() = 0;

    void Update() {
        if (upstream->Valid()) {
            if (CheckValid()) {
                populate();
            } else {
                valid = false;
            }
        }
    }

    bool CheckValid() const {
        return upstream->key().starts_with(prefix);
    }

    std::string prefixStr;
    rocksdb::Slice prefix;
    bool valid;
    T current;
    std::unique_ptr<rocksdb::Iterator> upstream;
};

class VertexIterator : public EntryIterator<VertexId> {
   public:
    VertexIterator(rocksdb::Iterator* upstream, const std::string& prefix)
        : EntryIterator(upstream, prefix) {
        upstream->Seek(prefix);
        Update();
    }

   protected:
    void populate() {
        assert(upstream->Valid());

        storage::VertexKey key;
        key.ParseFromString(upstream->key().ToStringView());

        current = VertexId{
            key.type(),
            key.id(),
        };
    }
};

class IndexVertexIterator : public EntryIterator<VertexId> {
   public:
    IndexVertexIterator(rocksdb::Iterator* upstream, const std::string& prefix)
        : EntryIterator(upstream, prefix) {
        upstream->Seek(prefix);
        Update();
    }

   protected:
    void populate() {
        assert(upstream->Valid());

        storage::IndexKey key;
        key.ParseFromString(upstream->key().ToStringView());

        current = VertexId{
            key.vertex().type(),
            key.vertex().id(),
        };
    }
};

class EdgeIterator : public EntryIterator<Edge> {
   public:
    EdgeIterator(rocksdb::Iterator* upstream, const std::string& prefix)
        : EntryIterator(upstream, prefix) {
        upstream->Seek(prefix);
        Update();
    }

   protected:
    void populate() {
        assert(upstream->Valid());
        storage::EdgeKey key;
        key.ParseFromString(upstream->key().ToStringView());

        current = Edge{
            VertexId{key.vertex().type(), key.vertex().id()},
            VertexId{key.other().type(), key.other().id()},
            key.label(),
            key.direction() == storage::Direction::IN ? IN : OUT};
    }
};

class ReadTransaction {
   public:
    ReadTransaction(const Storage& storage, long txId)
        : storage(storage), tx(rocksdb::EncodeU64Ts(1, &str_ts)) {
        readOptions.timestamp = &tx;
        readOptions.total_order_seek = true;
    }

    VertexIterator* GetVerticesByType(const std::string& type) {
        storage::VertexKey key;
        key.set_type(type);
        std::string firstKey = key.SerializeAsString();

        storage::VertexByType vbt;
        vbt.set_type(type);
        std::string trimmed = vbt.SerializeAsString();

        return new VertexIterator(
            storage.db->NewIterator(readOptions, storage.vertices), firstKey.substr(0, trimmed.size()));
    }

    IndexVertexIterator* GetVerticesByLabel(const std::string& label, const std::string& type) {
        storage::IndexKey key;
        key.set_label(label);
        key.mutable_vertex()->set_type(type);
        key.mutable_vertex()->set_id(0);
        std::string firstKey = key.SerializeAsString();

        storage::IndexVertexByType ivbt;
        ivbt.set_label(label);
        ivbt.mutable_vertexbytype()->set_type(type);
        std::string trimmed = ivbt.SerializeAsString();

        return new IndexVertexIterator(
            storage.db->NewIterator(readOptions, storage.index),
            firstKey.substr(0, trimmed.size()));
    }

    EdgeIterator* GetEdges(
        const VertexId& vertexId, const std::string& label, const Direction direction) {
        storage::EdgeKey edge;
        edge.mutable_vertex()->set_type(vertexId.type);
        edge.mutable_vertex()->set_id(vertexId.id);
        edge.set_label(label);
        edge.set_direction(direction == IN ? storage::Direction::IN : storage::Direction::OUT);
        std::string firstKey = edge.SerializeAsString();

        storage::EdgeByLabel ebl;
        ebl.mutable_vertex()->set_type(vertexId.type);
        ebl.mutable_vertex()->set_id(vertexId.id);
        ebl.set_label(label);
        ebl.set_direction(direction == IN ? storage::Direction::IN : storage::Direction::OUT);
        std::string trimmed = ebl.SerializeAsString();

        return new EdgeIterator(
            storage.db->NewIterator(readOptions, storage.edges),
            firstKey.substr(0, trimmed.size()));
    }

   protected:
    rocksdb::ReadOptions readOptions;
    std::string str_ts;
    rocksdb::Slice tx;
    const Storage& storage;
};

class WriteTransaction : ReadTransaction {
   public:
    WriteTransaction(const Storage& storage, long txId) : ReadTransaction(storage, txId) {}

    void AddVertex(const VertexId& vertexId) {
        storage::VertexKey key;
        key.set_type(vertexId.type);
        key.set_id(vertexId.id);

        storage.db->Put(wo, storage.vertices, key.SerializeAsString(), tx, rocksdb::Slice("a"));
    }

    void AddLabel(const VertexId& vertexId, const std::string label) {
        storage::IndexKey key;
        key.set_label(label);
        storage::VertexKey* vertexKey = key.mutable_vertex();
        vertexKey->set_type(vertexId.type);
        vertexKey->set_id(vertexId.id);

        storage.db->Put(wo, storage.index, key.SerializeAsString(), tx, rocksdb::Slice("a"));
    }

    void AddEdge(const std::string& label, const VertexId& from, const VertexId& to) {
        AddEdgeWithDirection(label, OUT, from, to);
        AddEdgeWithDirection(label, IN, to, from);
    }

   private:
    void AddEdgeWithDirection(
        const std::string& label,
        const Direction direction,
        const VertexId& vertex,
        const VertexId& other) {
        storage::EdgeKey key;
        key.set_label(label);
        key.set_direction(direction == OUT ? storage::Direction::OUT : storage::Direction::IN);

        storage::VertexKey* vertexKey = key.mutable_vertex();
        vertexKey->set_type(vertex.type);
        vertexKey->set_id(vertex.id);

        storage::VertexKey* otherKey = key.mutable_other();
        otherKey->set_type(other.type);
        otherKey->set_id(other.id);

        storage.db->Put(wo, storage.edges, key.SerializeAsString(), tx, rocksdb::Slice("a"));
    }

    rocksdb::WriteOptions wo;
};

class Graph {
   public:
    Graph(const std::string& path) {
        std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;

        rocksdb::Options options;
        options.create_if_missing = true;
        options.create_missing_column_families = true;

        descriptors.push_back(rocksdb::ColumnFamilyDescriptor());

        rocksdb::ColumnFamilyOptions cfOptions(options);
        cfOptions.comparator = rocksdb::BytewiseComparatorWithU64Ts();
        descriptors.push_back(rocksdb::ColumnFamilyDescriptor("index", cfOptions));
        descriptors.push_back(rocksdb::ColumnFamilyDescriptor("vertices", cfOptions));
        descriptors.push_back(rocksdb::ColumnFamilyDescriptor("edges", cfOptions));

        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        rocksdb::Status status =
            rocksdb::DB::Open(options, path, descriptors, &handles, &storage.db);
        assert(status.ok());

        storage._default = handles[0];
        storage.index = handles[1];
        storage.vertices = handles[2];
        storage.edges = handles[3];
    }

    ~Graph() {
        storage.db->DestroyColumnFamilyHandle(storage.edges);
        storage.db->DestroyColumnFamilyHandle(storage.vertices);
        storage.db->DestroyColumnFamilyHandle(storage.index);
        storage.db->DestroyColumnFamilyHandle(storage._default);
    }

    ReadTransaction* OpenForRead(long txId) { return new ReadTransaction(storage, txId); }

    WriteTransaction* OpenForWrite(long txId) { return new WriteTransaction(storage, txId); }

   private:
    Storage storage;
};

int main() {
    Graph graph("/tmp/graphdb");
    std::unique_ptr<WriteTransaction> write(graph.OpenForWrite(1));
    VertexId vertexId = {"component", 1};
    write->AddVertex(vertexId);
    write->AddLabel(vertexId, "ye-label");

    std::unique_ptr<ReadTransaction> read(graph.OpenForRead(2));

    std::cout << "Fetching vertices by label" << std::endl;
    std::unique_ptr<IndexVertexIterator> vertices(
        read->GetVerticesByLabel("ye-label", "component"));
    while (vertices->Valid()) {
        const VertexId& id = vertices->Get();
        std::cout << id.type << ":" << id.id << std::endl;
        vertices->Next();
    }

    VertexId otherId = {"component", 2};
    write->AddVertex(otherId);
    write->AddEdge("peer", vertexId, otherId);

    std::cout << "Fetching edges from vertex" << std::endl;
    std::unique_ptr<EdgeIterator> edges(
        read->GetEdges(vertexId, "peer", OUT));
    while (edges->Valid()) {
        const Edge& edge = edges->Get();
        std::cout << edge.otherId.type << ":" << edge.otherId.id << std::endl;
        edges->Next();
    }

    std::cout << "Fetching vertices by type" << std::endl;
    std::unique_ptr<VertexIterator> bytype(
        read->GetVerticesByType("component"));
    while (bytype->Valid()) {
        const VertexId& id = bytype->Get();
        std::cout << id.type << ":" << id.id << std::endl;
        bytype->Next();
    }

    /*
    rocksdb::DB* db;
    std::string ts_0;
    std::string ts_2;
    std::string ts_4;
    rocksdb::WriteOptions wo;
    db->Put(
        wo,
        db->DefaultColumnFamily(),
        rocksdb::Slice("key"),
        rocksdb::EncodeU64Ts(0, &ts_0),
        rocksdb::Slice("a"));
    db->Put(
        wo,
        db->DefaultColumnFamily(),
        rocksdb::Slice("key"),
        rocksdb::EncodeU64Ts(2, &ts_2),
        rocksdb::Slice("b"));
    db->Delete(
        wo, db->DefaultColumnFamily(), rocksdb::Slice("key"), rocksdb::EncodeU64Ts(4, &ts_4));
    */

    /**
     * Query over Time
     * - iterator with iter_start_ts only returns updates within the time-range
     *   results are from most recent to oldest
     * - other iterator returns the view at the start of the interval
     */
    /*
    std::string ts_1;
    std::string ts_5;
    rocksdb::Slice from(rocksdb::EncodeU64Ts(1, &ts_1));
    rocksdb::Slice to(rocksdb::EncodeU64Ts(5, &ts_5));
    {
        rocksdb::ReadOptions readOptions;
        readOptions.timestamp = &to;
        readOptions.iter_start_ts = &from;
        rocksdb::Iterator* iter = db->NewIterator(readOptions);
        iter->SeekToFirst();
        while (iter->Valid()) {
            // std::cout << "key: " << iter->key().data() << " => " << iter->value().data() <<
            // std::endl;
            std::cout << "key: " << iter->key().ToString() << " => " << iter->value().ToString()
                      << std::endl;
            iter->Next();
        }
    }
    {
        rocksdb::ReadOptions readOptions;
        readOptions.timestamp = &from;
        rocksdb::Iterator* iter = db->NewIterator(readOptions);
        iter->SeekToFirst();
        while (iter->Valid()) {
            // std::cout << "key: " << iter->key().data() << " => " << iter->value().data() <<
            // std::endl;
            std::cout << "key: " << iter->key().ToString() << " => " << iter->value().ToString()
                      << std::endl;
            iter->Next();
        }
    }
    */
}
