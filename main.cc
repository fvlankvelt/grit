#include <cassert>
#include <iostream>
#include <memory>
#include <sstream>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "storage.pb.h"

#define MAX_N_OPERANDS 100

enum TransactionException { TX_IS_READONLY, TX_CONFLICT, TX_INVALIDATED, TX_NOT_IN_PROGRESS };

struct VertexId {
    std::string type;
    long id;
};

constexpr bool operator<(const VertexId& a, const VertexId& b) {
    int cmp = a.type.compare(b.type);
    return cmp == 0 ? a.id < b.id : cmp < 0;
}

class Transaction {
   public:
    Transaction(
        uint64_t txId, std::set<uint64_t> inProgress, std::set<uint64_t> invalid, bool readOnly)
        : txId(txId), inProgress(inProgress), invalidTxIds(invalid), readOnly(readOnly) {
        fluidTxId = txId;
        for (auto it = inProgress.begin(); it != inProgress.end(); it++) {
            if (*it < fluidTxId) {
                fluidTxId = *it;
            }
        }
    }

    void Touch(const VertexId& key) {
        if (readOnly) {
            throw TX_IS_READONLY;
        }
        touched.insert(key);
    }

    uint64_t GetTxId() const { return txId; }

    uint64_t GetFluidTxId() const { return fluidTxId; }

    bool IsExcluded(uint64_t otherTxId) const {
        return otherTxId > txId || inProgress.find(otherTxId) != inProgress.end() ||
               invalidTxIds.find(otherTxId) != invalidTxIds.end();
    }

    friend class TransactionManager;

   private:
    // last transaction that can be read
    uint64_t txId;
    // start of in-progress transaction window, there are no in-progress txns before it.
    uint64_t fluidTxId;

    bool readOnly;
    std::set<uint64_t> inProgress;
    std::set<uint64_t> invalidTxIds;
    std::set<VertexId> touched;
};

class TransactionManager {
   public:
    std::shared_ptr<Transaction> Open() {
        uint64_t txId = ++lastTxId;
        inProgress.insert(txId);
        return std::shared_ptr<Transaction>(new Transaction(txId, inProgress, invalid, false));
    }

    std::shared_ptr<Transaction> OpenForRead(uint64_t txId = -1) {
        uint64_t tx = txId == -1 ? ++lastTxId : txId;
        return std::shared_ptr<Transaction>(new Transaction(tx, inProgress, invalid, true));
    }

    void Commit(const Transaction& txn) {
        uint64_t txId = txn.GetTxId();
        if (inProgress.find(txId) == inProgress.end()) {
            throw TX_NOT_IN_PROGRESS;
        }

        try {
            // validate txn against all transactions from its in-progress set
            // that have been committed since its start.
            for (auto it = txn.inProgress.begin(); it != txn.inProgress.end(); it++) {
                uint64_t inPTxId = *it;
                if (inProgress.find(inPTxId) != inProgress.end() ||
                    invalid.find(inPTxId) != invalid.end()) {
                    continue;
                }
                if (recent.find(inPTxId) == recent.end()) {
                    // transaction references a committed transaction, but that has
                    // expired already.  So we cannot check if it conflicts.
                    throw TX_INVALIDATED;
                } else {
                    const std::set<VertexId> ref = recent.find(inPTxId)->second;
                    if (Conflict(ref, txn.touched)) {
                        throw TX_CONFLICT;
                    }
                }
            }

            // similar for transactions that started later, but that have already
            // been committed.
            for (auto it = recent.begin(); it != recent.end(); it++) {
                if (it->first > txId && Conflict(it->second, txn.touched)) {
                    throw TX_CONFLICT;
                }
            }
        } catch (TransactionException te) {
            inProgress.erase(txId);
            invalid.insert(txId);
            throw te;
        }

        // hurray!  No conflicts detected
        recent.insert(std::pair(txId, txn.touched));
        inProgress.erase(txId);
    }

    void Rollback(uint64_t txId) {
        inProgress.erase(txId);
        invalid.insert(txId);
    }

    /**
     * Remove from memory all committed transactions from before expireTx.
     * This will abort any transactions that started before expireTx.
     */
    void Advance(uint64_t expireTx) {
        for (auto it = recent.begin(); it != recent.end();) {
            uint64_t txId = it->first;
            if (txId < expireTx) {
                it = recent.erase(it);
            } else {
                ++it;
            }
        }
    }

    bool IsInvalid(uint64_t txId) const { return invalid.find(txId) != invalid.end(); }

   private:
    bool Conflict(const std::set<VertexId>& a, const std::set<VertexId>& b) {
        std::set<VertexId> out;
        std::set_intersection(
            a.begin(), a.end(), b.begin(), b.end(), std::inserter(out, out.begin()));
        return out.begin() != out.end();
    }

    uint64_t lastTxId = 0;
    std::map<uint64_t, std::set<VertexId>> recent;
    std::set<uint64_t> inProgress;

    // all rolled back and invalidated transactions - this set can be pruned
    // by a scan (GC) over all data; any updates from invalidated txns are removed
    // on a compaction.
    std::set<uint64_t> invalid;
};

/**
 * Merge updates to a key, filtering out those created by invalid transactions.
 * Should only be used on time ranges that exclude the "active window" as in-progress
 * transactions are still undecided.
 */
/*
class TxMergeOperator : public rocksdb::MergeOperator {
   public:
    TxMergeOperator(TransactionManager& txMgr) : txMgr(txMgr) {}

    const char* Name() const { return "TxMergeOperator"; }

    bool FullMergeV2(
        const MergeOperationInput& merge_in, MergeOperationOutput* merge_out) const override {
        merge_out->new_value = merge_in.existing_value->ToStringView();
        for (auto it = merge_in.operand_list.begin(); it != merge_in.operand_list.end(); it++) {
            storage::MergeValue value;
            value.ParseFromString(it->ToStringView());

            if (!txMgr.IsInvalid(value.txid())) {
                merge_out->new_value = it->data();
                break;
            }
        }
        return true;
    }

   private:
    TransactionManager& txMgr;
};
*/

/*
class TxCompactionFilter : public rocksdb::CompactionFilter {
   public:
    TxCompactionFilter(TransactionManager& txMgr) : txMgr(txMgr) {}

    const char* Name() const { return "TxCompactionFilter"; }

    // The table file creation process invokes this method before adding a kv to
    // the table file. A return value of false indicates that the kv should be
    // preserved in the new table file and a return value of true indicates
    // that this key-value should be removed (that is, converted to a tombstone).
    // The application can inspect the existing value of the key and make decision
    // based on it.
    //
    // Key-Values that are results of merge operation during table file creation
    // are not passed into this function. Currently, when you have a mix of Put()s
    // and Merge()s on a same key, we only guarantee to process the merge operands
    // through the `CompactionFilter`s. Put()s might be processed, or might not.
    //
    // When the value is to be preserved, the application has the option
    // to modify the existing_value and pass it back through new_value.
    // value_changed needs to be set to true in this case.
    bool Filter(
        int level,
        const rocksdb::Slice& key,
        const rocksdb::Slice& existing_value,
        std::string* new_value,
        bool* value_changed) const {
        storage::MergeValue value;
        value.ParseFromString(existing_value.ToStringView());

        // what to do in this case?
        assert(!txMgr.IsInvalid(value.txid()));
        return value.action() == storage::DELETE;
    }

    // The table file creation process invokes this method on every merge operand.
    // If this method returns true, the merge operand will be ignored and not
    // written out in the new table file.
    bool FilterMergeOperand(
        int level, const rocksdb::Slice&  key , const rocksdb::Slice& operand) const {
        storage::MergeValue value;
        value.ParseFromString(operand.ToStringView());
        return txMgr.IsInvalid(value.txid());
    }

   private:
    TransactionManager& txMgr;
};
*/

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
    EntryIterator(
        rocksdb::Iterator* fluid,   // time-range iterator that includes in-progress txn data
                                    // (multiple updates per key)
        rocksdb::Iterator* frozen,  // time-travel iterator that only contains historic data
                                    // (one entry per key)
        const std::string& prefix,
        const std::shared_ptr<Transaction>& txn)
        : fluid(fluid), frozen(frozen), prefixStr(prefix), prefix(prefixStr), txn(txn) {
        comparator = rocksdb::BytewiseComparatorWithU64Ts();
    }

    const T& Get() const { return current; }

    bool Valid() const { return valid; }

    /**
     * Proceed to next key.  Skip older updates to a key and updates from excluded transactions.
     */
    void Next() {
        bool foundKey = false;
        while (!foundKey && ((fluid->Valid() && IsValidKey(fluid->key())) ||
                             (frozen->Valid() && IsValidKey(frozen->key())))) {
            int cmp;
            if ((fluid->Valid() && IsValidKey(fluid->key())) &&
                (frozen->Valid() && IsValidKey(frozen->key()))) {
                cmp = comparator->CompareWithoutTimestamp(
                    fluid->key(), true, frozen->key(), false);
            } else if (fluid->Valid() && IsValidKey(fluid->key())) {
                cmp = -1;
            } else {
                cmp = +1;
            }

            uint64_t keyTxId;
            storage::MergeValue value;
            if (cmp > 0) {
                DecodeU64Ts(frozen->timestamp(), &keyTxId);
                if (txn->IsExcluded(keyTxId)) {
                    frozen->Next();
                    continue;
                }
                currentKey = frozen->key().ToString();
                assert(frozen->key().ToString().compare(currentKey) == 0);
                value.ParseFromString(frozen->value().ToStringView());
            } else {
                DecodeU64Ts(fluid->timestamp(), &keyTxId);
                if (txn->IsExcluded(keyTxId)) {
                    fluid->Next();
                    continue;
                }
                // time-range iterator includes user-defined timestamp in key
                currentKey = FluidKey();
                value.ParseFromString(fluid->value().ToStringView());
            }

            // skip deleted keys
            if (value.action() == storage::PUT) {
                populate(currentKey);
                foundKey = true;
            }

            // skip all remaining entries for the same key
            while (fluid->Valid() && FluidKey().compare(currentKey) == 0) {
                fluid->Next();
            }
            while (frozen->Valid() && frozen->key().ToString().compare(currentKey) == 0) {
                frozen->Next();
            }
        }

        if (!foundKey) {
            valid = false;
        }
    }

   protected:
    virtual void populate(const rocksdb::Slice& keySlice) = 0;
    virtual std::string ToString(T value) = 0;

    bool valid = true;
    T current;

   private:
    bool IsValidKey(const rocksdb::Slice& key) const { return key.starts_with(prefix); }

    std::string FluidKey() const {
        return std::string(fluid->key().data(), fluid->key().size() - 16);
    }

    std::string currentKey;
    const std::shared_ptr<Transaction> txn;
    const rocksdb::Comparator* comparator;
    std::string prefixStr;
    rocksdb::Slice prefix;
    std::unique_ptr<rocksdb::Iterator> fluid;
    std::unique_ptr<rocksdb::Iterator> frozen;
};

class VertexIterator : public EntryIterator<VertexId> {
   public:
    VertexIterator(
        rocksdb::Iterator* fluid,
        rocksdb::Iterator* frozen,
        const std::string& prefix,
        const std::shared_ptr<Transaction>& txn)
        : EntryIterator(fluid, frozen, prefix, txn) {
        fluid->Seek(prefix);
        frozen->Seek(prefix);
        Next();
    }

   protected:
    void populate(const rocksdb::Slice& keySlice) {
        storage::VertexKey key;
        key.ParseFromString(keySlice.ToStringView());

        current = VertexId{
            key.type(),
            key.id(),
        };
    }
    std::string ToString(VertexId id) {
        std::stringstream ss;
        ss << id.type << ":" << id.id;
        return ss.str();
    }
};

class IndexVertexIterator : public EntryIterator<VertexId> {
   public:
    IndexVertexIterator(
        rocksdb::Iterator* fluid,
        rocksdb::Iterator* frozen,
        const std::string& prefix,
        const std::shared_ptr<Transaction>& txn)
        : EntryIterator(fluid, frozen, prefix, txn) {
        fluid->Seek(prefix);
        frozen->Seek(prefix);
        Next();
    }

   protected:
    void populate(const rocksdb::Slice& keySlice) {
        storage::IndexKey key;
        key.ParseFromString(keySlice.ToStringView());

        current = VertexId{
            key.vertex().type(),
            key.vertex().id(),
        };
    }
    std::string ToString(VertexId id) {
        std::stringstream ss;
        ss << id.type << ":" << id.id;
        return ss.str();
    }
};

class EdgeIterator : public EntryIterator<Edge> {
   public:
    EdgeIterator(
        rocksdb::Iterator* fluid,
        rocksdb::Iterator* frozen,
        const std::string& prefix,
        const std::shared_ptr<Transaction>& txn)
        : EntryIterator(fluid, frozen, prefix, txn) {
        fluid->Seek(prefix);
        frozen->Seek(prefix);
        Next();
    }

   protected:
    void populate(const rocksdb::Slice& keySlice) {
        storage::EdgeKey key;
        key.ParseFromString(keySlice.ToStringView());

        current = Edge{
            VertexId{key.vertex().type(), key.vertex().id()},
            VertexId{key.other().type(), key.other().id()},
            key.label(),
            key.direction() == storage::Direction::IN ? IN : OUT};
    }
    std::string ToString(Edge edge) {
        std::stringstream ss;
        ss << edge.direction << " " << edge.label << " " << edge.vertexId.id << " - "
           << edge.otherId.id;
        return ss.str();
    }
};

class ReadTransaction {
   public:
    ReadTransaction(const Storage& storage, const std::shared_ptr<Transaction>& txn)
        : storage(storage),
          txn(txn),
          tx(rocksdb::EncodeU64Ts(txn->GetTxId(), &str_ts)),
          start_tx(rocksdb::EncodeU64Ts(txn->GetFluidTxId(), &str_start_ts)) {
        fluidReadOptions.timestamp = &tx;
        fluidReadOptions.iter_start_ts = &start_tx;
        fluidReadOptions.total_order_seek = true;

        frozenReadOptions.timestamp = &start_tx;
        frozenReadOptions.total_order_seek = true;
    }

    VertexIterator* GetVerticesByType(const std::string& type) {
        storage::VertexKey key;
        key.set_type(type);
        std::string firstKey = key.SerializeAsString();

        storage::VertexByType vbt;
        vbt.set_type(type);
        std::string trimmed = vbt.SerializeAsString();

        return new VertexIterator(
            storage.db->NewIterator(fluidReadOptions, storage.vertices),
            storage.db->NewIterator(frozenReadOptions, storage.vertices),
            firstKey.substr(0, trimmed.size()),
            txn);
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
            storage.db->NewIterator(fluidReadOptions, storage.index),
            storage.db->NewIterator(frozenReadOptions, storage.index),
            firstKey.substr(0, trimmed.size()),
            txn);
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
            storage.db->NewIterator(fluidReadOptions, storage.edges),
            storage.db->NewIterator(frozenReadOptions, storage.edges),
            firstKey.substr(0, trimmed.size()),
            txn);
    }

   protected:
    rocksdb::ReadOptions fluidReadOptions;
    rocksdb::ReadOptions frozenReadOptions;

    std::string str_ts;
    rocksdb::Slice tx;
    std::string str_start_ts;
    rocksdb::Slice start_tx;
    std::shared_ptr<Transaction> txn;
    const Storage& storage;
};

class WriteTransaction : ReadTransaction {
   public:
    WriteTransaction(const Storage& storage, TransactionManager& txMgr)
        : ReadTransaction(storage, txMgr.Open()), txMgr(txMgr) {
        storage::MergeValue value;
        value.set_action(storage::PUT);
        value.set_txid(txn->GetTxId());
        valueStr = value.SerializeAsString();
    }

    void AddVertex(const VertexId& vertexId) {
        storage::VertexKey key;
        key.set_type(vertexId.type);
        key.set_id(vertexId.id);

        rocksdb::Status status =
            storage.db->Put(wo, storage.vertices, key.SerializeAsString(), tx, valueStr);
        assert(status.ok());
    }

    void AddLabel(const VertexId& vertexId, const std::string label) {
        storage::IndexKey key;
        key.set_label(label);
        storage::VertexKey* vertexKey = key.mutable_vertex();
        vertexKey->set_type(vertexId.type);
        vertexKey->set_id(vertexId.id);

        txn->Touch(vertexId);
        rocksdb::Status status =
            storage.db->Put(wo, storage.index, key.SerializeAsString(), tx, valueStr);
        assert(status.ok());
    }

    void AddEdge(const std::string& label, const VertexId& from, const VertexId& to) {
        txn->Touch(from);
        txn->Touch(to);
        AddEdgeWithDirection(label, OUT, from, to);
        AddEdgeWithDirection(label, IN, to, from);
    }

    uint64_t Commit() {
        txMgr.Commit(*txn.get());
        return txn->GetTxId();
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

        rocksdb::Status status =
            storage.db->Put(wo, storage.edges, key.SerializeAsString(), tx, valueStr);
        assert(status.ok());
    }

    std::string valueStr;
    TransactionManager& txMgr;
    rocksdb::WriteOptions wo;
};

class Graph {
   public:
    Graph(const std::string& path) {
        std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;

        rocksdb::Options options;
        options.create_if_missing = true;
        options.create_missing_column_families = true;
        // options.merge_operator.reset(new TxMergeOperator(txMgr));

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

    ReadTransaction* OpenForRead(uint64_t txId = -1) {
        return new ReadTransaction(storage, txMgr.OpenForRead(txId));
    }

    WriteTransaction* OpenForWrite() { return new WriteTransaction(storage, txMgr); }

   private:
    TransactionManager txMgr;
    Storage storage;
};

int main() {
    std::filesystem::remove_all("/tmp/graphdb");
    Graph graph("/tmp/graphdb");
    std::unique_ptr<WriteTransaction> write(graph.OpenForWrite());
    VertexId vertexId = {"c", 1};
    write->AddVertex(vertexId);
    write->AddLabel(vertexId, "ye-label");
    uint64_t writeTx = write->Commit();

    std::cout << "Fetching vertices by label" << std::endl;
    {
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead());
        std::unique_ptr<IndexVertexIterator> vertices(
            read->GetVerticesByLabel("ye-label", "c"));
        while (vertices->Valid()) {
            const VertexId& id = vertices->Get();
            std::cout << id.type << ":" << id.id << std::endl;
            vertices->Next();
        }
    }

    std::unique_ptr<WriteTransaction> writeMore(graph.OpenForWrite());
    VertexId otherId = {"c", 2};
    writeMore->AddVertex(otherId);
    writeMore->AddEdge("peer", vertexId, otherId);

    std::cout << "Fetching edges from vertex" << std::endl;
    {
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead());
        std::unique_ptr<EdgeIterator> edges(read->GetEdges(vertexId, "peer", OUT));
        while (edges->Valid()) {
            const Edge& edge = edges->Get();
            std::cout << edge.otherId.type << ":" << edge.otherId.id << std::endl;
            edges->Next();
        }
    }
    writeMore->Commit();

    std::cout << "Fetching vertices by type" << std::endl;
    {
        std::unique_ptr<ReadTransaction> read(graph.OpenForRead(writeTx));
        std::unique_ptr<VertexIterator> bytype(read->GetVerticesByType("c"));
        while (bytype->Valid()) {
            const VertexId& id = bytype->Get();
            std::cout << id.type << ":" << id.id << std::endl;
            bytype->Next();
        }
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
