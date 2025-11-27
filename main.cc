#include <cassert>
#include <iostream>
#include <memory>
#include <sstream>

#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "storage.pb.h"

struct VertexId {
    std::string type;
    long id;
};

struct IndexVertexEntry {
    std::string label;
    VertexId vertexId;
};

struct Storage {
    std::unique_ptr<rocksdb::DB> db;
    rocksdb::ColumnFamilyHandle * _default;
    rocksdb::ColumnFamilyHandle * index;
    rocksdb::ColumnFamilyHandle * vertices;
    // rocksdb::ColumnFamilyHandle* labels;
    // rocksdb::ColumnFamilyHandle* edges;
};

class VertexIterator {
   public:
    VertexIterator(rocksdb::Iterator* upstream) : upstream(upstream) {
        upstream->SeekToFirst();
    }

    const VertexId& operator*() const { return current; }

    const VertexId& Get() const { return current; }

    bool Valid() const { return upstream->Valid(); }

    void Next() {
        upstream->Next();
        if (upstream->Valid()) {
            populate();
        }
    }

   protected:
    virtual void populate() = 0;

    VertexId current;
    std::unique_ptr<rocksdb::Iterator> upstream;
};

class IndexVertexIterator : public VertexIterator {
   public:
    IndexVertexIterator(rocksdb::Iterator* upstream) : VertexIterator(upstream) {
        if (upstream->Valid()) {
            populate();
        }
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

class ReadTransaction {
   public:
    ReadTransaction(const Storage& storage, long txId)
        : storage(storage), tx(rocksdb::EncodeU64Ts(1, &str_ts)) {}

    VertexIterator* GetVertices(std::string label, std::string type) {
        rocksdb::ReadOptions readOptions;
        readOptions.timestamp = &tx;
        return new IndexVertexIterator(
            storage.db->NewIterator(readOptions, storage.index));
    }

   protected:
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

        rocksdb::WriteOptions wo;
        storage.db->Put(
            wo, storage.vertices, key.SerializeAsString(), tx, rocksdb::Slice("a"));
    }

    void AddLabel(const VertexId& vertexId, const std::string label) {
        storage::IndexKey key;
        key.set_label(label);
        storage::VertexKey * vertexKey = key.mutable_vertex();
        vertexKey->set_type(vertexId.type);
        vertexKey->set_id(vertexId.id);

        rocksdb::WriteOptions wo;
        storage.db->Put(
            wo, storage.index, key.SerializeAsString(), tx, rocksdb::Slice("a"));
    }
};

class Graph {
   public:
    Graph(const std::string& path) {
        std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;

        rocksdb::Options options;
        options.create_if_missing = true;
        options.create_missing_column_families = true;

        descriptors.push_back(rocksdb::ColumnFamilyDescriptor());

        rocksdb::ColumnFamilyOptions indexOptions(options);
        indexOptions.comparator = rocksdb::BytewiseComparatorWithU64Ts();
        descriptors.push_back(rocksdb::ColumnFamilyDescriptor("index", indexOptions));

        rocksdb::ColumnFamilyOptions vertexOptions(options);
        vertexOptions.comparator = rocksdb::BytewiseComparatorWithU64Ts();
        descriptors.push_back(rocksdb::ColumnFamilyDescriptor("vertices", vertexOptions));

        // options.comparator = rocksdb::BytewiseComparatorWithU64Ts();
        // options.compaction_style = rocksdb::kCompactionStyleLevel;

        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        rocksdb::Status status =
            rocksdb::DB::Open(options, path, descriptors, &handles, &storage.db);
        std::cout << status.ToString() << std::endl;
        assert(status.ok());

        storage._default = handles[0];
        storage.index = handles[1];
        storage.vertices = handles[2];
    }

    ~Graph() {
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
    std::unique_ptr<VertexIterator> vertices(read->GetVertices("ye-label", "component"));
    while (vertices->Valid()) {
        const VertexId& id = vertices->Get();
        std::cout << id.type << ":" << id.id << std::endl;
        vertices->Next();
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
