#pragma once

#include <cassert>
#include <memory>
#include <sstream>

#include "graph.h"
#include "model.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "storage.pb.h"
#include "transaction.h"

using namespace std;

/**
 * Merge updates to a key, filtering out those created by invalid transactions.
 * Should only be used on time ranges that exclude the "active window" as in-progress
 * transactions are still undecided.
 */
class TxMergeOperator : public rocksdb::MergeOperator {
public:
    TxMergeOperator(const shared_ptr<TransactionManager> &txMgr) : txMgr(txMgr) {
    }

    const char *Name() const override { return "TxMergeOperator"; }

    bool FullMergeV2(
        const MergeOperationInput &merge_in, MergeOperationOutput *merge_out) const override {
        storage::MergeValue value;
        if (merge_in.existing_value) {
            merge_out->new_value = merge_in.existing_value->ToString(false);
        } else {
            value.set_action(storage::DELETE);
            value.set_txid(0);
            merge_out->new_value = value.SerializeAsString();
        }

        auto operands = merge_in.operand_list;
        for (auto it = operands.rbegin(); it != operands.rend(); it++) {
            value.ParseFromString(it->ToStringView());
            if (!txMgr->IsInvalid(value.txid())) {
                merge_out->new_value = it->ToString();
                break;
            }
        }
        return true;
    }

private:
    std::shared_ptr<TransactionManager> txMgr;
};

class TxCompactionFilter : public rocksdb::CompactionFilter {
public:
    TxCompactionFilter(const std::shared_ptr<TransactionManager> &txMgr) : txMgr(txMgr) {
    }

    const char *Name() const override { return "TxCompactionFilter"; }

    bool Filter(
        int level,
        const rocksdb::Slice &key,
        const rocksdb::Slice &existing_value,
        std::string *new_value,
        bool *value_changed) const override {
        storage::MergeValue value;
        value.ParseFromString(existing_value.ToStringView());

        // what to do in this case?
        assert(!txMgr->IsInvalid(value.txid()));
        return value.action() == storage::DELETE;
    }

    bool FilterMergeOperand(
        int level, const rocksdb::Slice &key, const rocksdb::Slice &operand) const override {
        storage::MergeValue value;
        value.ParseFromString(operand.ToStringView());
        return txMgr->IsInvalid(value.txid());
    }

private:
    std::shared_ptr<TransactionManager> txMgr;
};

struct Storage {
    rocksdb::DB *db;
    rocksdb::ColumnFamilyHandle *_default;
    rocksdb::ColumnFamilyHandle *index;
    rocksdb::ColumnFamilyHandle *vertices;
    rocksdb::ColumnFamilyHandle *edges;
    rocksdb::ColumnFamilyHandle *labels;
};

template<class T>
class EntryIterator {
public:
    EntryIterator(
        rocksdb::Iterator *fluid, // time-range iterator that includes in-progress txn data
        // (multiple updates per key)
        rocksdb::Iterator *frozen, // time-travel iterator that only contains historic data
        // (one entry per key)
        const std::string &prefix,
        const std::shared_ptr<Transaction> &txn)
        : txn(txn), prefixStr(prefix), prefixSlice(prefixStr), fluid(fluid), frozen(frozen) {
        comparator = rocksdb::BytewiseComparatorWithU64Ts();
    }

    virtual ~EntryIterator() = default;

    const T &Get() const { return current; }

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
                value.ParseFromString(frozen->value().ToStringView());
                keyTxId = value.txid();
                DecodeU64Ts(fluid->timestamp(), &keyTxId);
                if (txn->IsExcluded(keyTxId)) {
                    frozen->Next();
                    continue;
                }
                currentKey = frozen->key().ToString();
                assert(frozen->key().ToString().compare(currentKey) == 0);
            } else {
                value.ParseFromString(fluid->value().ToStringView());
                keyTxId = value.txid();
                if (txn->IsExcluded(keyTxId)) {
                    fluid->Next();
                    continue;
                }
                // time-range iterator includes user-defined timestamp in key
                // strip it to filter out subsequent entries with the same key
                currentKey = FluidKey();
            }

            // don't report deleted keys
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
    virtual void populate(const rocksdb::Slice &keySlice) = 0;

    virtual std::string ToString(T value) = 0;

    bool valid = true;
    T current;

private:
    bool IsValidKey(const rocksdb::Slice &key) const { return key.starts_with(prefixSlice); }

    std::string FluidKey() const {
        return {fluid->key().data(), fluid->key().size() - 8};
    }

    std::string currentKey;
    const std::shared_ptr<Transaction> txn;
    const rocksdb::Comparator *comparator;
    std::string prefixStr;
    rocksdb::Slice prefixSlice;
    std::unique_ptr<rocksdb::Iterator> fluid;
    std::unique_ptr<rocksdb::Iterator> frozen;
};

class VertexIterator : public EntryIterator<VertexId> {
public:
    VertexIterator(
        rocksdb::Iterator *fluid,
        rocksdb::Iterator *frozen,
        const std::string &prefix,
        const std::shared_ptr<Transaction> &txn)
        : EntryIterator(fluid, frozen, prefix, txn) {
        fluid->Seek(prefix);
        frozen->Seek(prefix);
        Next();
    }

protected:
    void populate(const rocksdb::Slice &keySlice) {
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
        rocksdb::Iterator *fluid,
        rocksdb::Iterator *frozen,
        const std::string &prefix,
        const std::shared_ptr<Transaction> &txn)
        : EntryIterator(fluid, frozen, prefix, txn) {
        fluid->Seek(prefix);
        frozen->Seek(prefix);
        Next();
    }

protected:
    void populate(const rocksdb::Slice &keySlice) {
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

class LabelIterator : public EntryIterator<std::string> {
public:
    LabelIterator(
        rocksdb::Iterator *fluid,
        rocksdb::Iterator *frozen,
        const std::string &prefix,
        const std::shared_ptr<Transaction> &txn)
        : EntryIterator(fluid, frozen, prefix, txn) {
        fluid->Seek(prefix);
        frozen->Seek(prefix);
        Next();
    }

protected:
    void populate(const rocksdb::Slice &keySlice) {
        storage::LabelKey key;
        key.ParseFromString(keySlice.ToStringView());
        current = key.label();
    }

    std::string ToString(std::string current) { return current; }
};

class EdgeIterator : public EntryIterator<Edge> {
public:
    EdgeIterator(
        rocksdb::Iterator *fluid,
        rocksdb::Iterator *frozen,
        const std::string &prefix,
        const std::shared_ptr<Transaction> &txn)
        : EntryIterator(fluid, frozen, prefix, txn) {
        fluid->Seek(prefix);
        frozen->Seek(prefix);
        Next();
    }

protected:
    void populate(const rocksdb::Slice &keySlice) {
        storage::EdgeKey key;
        key.ParseFromString(keySlice.ToStringView());
        current = Edge{
            VertexId{key.vertex().type(), key.vertex().id()},
            VertexId{key.other().type(), key.other().id()},
            key.label(),
            key.direction() == storage::Direction::IN ? IN : OUT
        };
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
    ReadTransaction(const Storage &storage, const std::shared_ptr<Transaction> &txn)
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

    virtual bool IsReadOnly() { return true; }

    uint64_t GetTxId() const { return txn->GetTxId(); }

    VertexIterator *GetVerticesByType(const std::string &type) {
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

    IndexVertexIterator *GetVerticesByLabel(const std::string &label, const std::string &type) {
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

    LabelIterator *GetLabels(const VertexId &vertexId) const {
        storage::LabelKey key;
        key.mutable_vertex()->set_type(vertexId.type);
        key.mutable_vertex()->set_id(vertexId.id);
        std::string firstKey = key.SerializeAsString();

        storage::LabelByVertex ivbt;
        ivbt.mutable_vertex()->set_type(vertexId.type);
        ivbt.mutable_vertex()->set_id(vertexId.id);
        std::string trimmed = ivbt.SerializeAsString();

        return new LabelIterator(
            storage.db->NewIterator(fluidReadOptions, storage.labels),
            storage.db->NewIterator(frozenReadOptions, storage.labels),
            firstKey.substr(0, trimmed.size()),
            txn);
    }

    EdgeIterator *GetEdges(
        const VertexId &vertexId, const std::string &label, const Direction direction) const {
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

    EdgeIterator *GetEdges(const VertexId &vertexId) const {
        storage::EdgeKey edge;
        edge.mutable_vertex()->set_type(vertexId.type);
        edge.mutable_vertex()->set_id(vertexId.id);
        std::string firstKey = edge.SerializeAsString();

        storage::EdgeByVertex ebv;
        ebv.mutable_vertex()->set_type(vertexId.type);
        ebv.mutable_vertex()->set_id(vertexId.id);
        std::string trimmed = ebv.SerializeAsString();

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
    const Storage &storage;
};

class WriteTransaction : public ReadTransaction {
public:
    WriteTransaction(const Storage &storage, const std::shared_ptr<TransactionManager> &txMgr)
        : ReadTransaction(storage, txMgr->Open()), txMgr(txMgr) {
        storage::MergeValue value;
        value.set_action(storage::PUT);
        value.set_txid(txn->GetTxId());
        putStr = value.SerializeAsString();
        value.set_action(storage::DELETE);
        deleteStr = value.SerializeAsString();
    }

    virtual bool IsReadOnly() { return false; }

    void AddVertex(const VertexId &vertexId) const {
        storage::VertexKey key;
        key.set_type(vertexId.type);
        key.set_id(vertexId.id);

        rocksdb::Status status =
                storage.db->Merge(wo, storage.vertices, key.SerializeAsString(), tx, putStr);
        assert(status.ok());
    }

    void RemoveVertex(const VertexId &vertexId) const {
        txn->Touch(vertexId);

        storage::VertexKey key;
        key.set_type(vertexId.type);
        key.set_id(vertexId.id);

        const std::unique_ptr<EdgeIterator> edges(GetEdges(vertexId));
        while (edges->Valid()) {
            const Edge &edge = edges->Get();
            if (edge.direction == OUT) {
                RemoveEdge(edge.label, edge.vertexId, edge.otherId);
            } else {
                RemoveEdge(edge.label, edge.otherId, edge.vertexId);
            }
            edges->Next();
        }

        const std::unique_ptr<LabelIterator> labels(GetLabels(vertexId));
        while (labels->Valid()) {
            const std::string &label = labels->Get();
            RemoveLabel(vertexId, label);
            labels->Next();
        }

        const rocksdb::Status status =
                storage.db->Merge(wo, storage.vertices, key.SerializeAsString(), tx, deleteStr);
        assert(status.ok());
    }

    void AddLabel(const VertexId &vertexId, const std::string &label) const {
        storage::IndexKey indexKey;
        indexKey.set_label(label);
        storage::VertexKey *vertexKey = indexKey.mutable_vertex();
        vertexKey->set_type(vertexId.type);
        vertexKey->set_id(vertexId.id);

        txn->Touch(vertexId);
        rocksdb::Status status =
                storage.db->Merge(wo, storage.index, indexKey.SerializeAsString(), tx, putStr);
        assert(status.ok());

        storage::LabelKey labelKey;
        labelKey.set_label(label);
        vertexKey = labelKey.mutable_vertex();
        vertexKey->set_type(vertexId.type);
        vertexKey->set_id(vertexId.id);

        status =
                storage.db->Merge(wo, storage.labels, labelKey.SerializeAsString(), tx, putStr);
        assert(status.ok());
    }

    void RemoveLabel(const VertexId &vertexId, const std::string label) const {
        storage::IndexKey key;
        key.set_label(label);
        storage::VertexKey *vertexKey = key.mutable_vertex();
        vertexKey->set_type(vertexId.type);
        vertexKey->set_id(vertexId.id);

        txn->Touch(vertexId);
        rocksdb::Status status =
                storage.db->Merge(wo, storage.index, key.SerializeAsString(), tx, deleteStr);
        assert(status.ok());

        storage::LabelKey labelKey;
        labelKey.set_label(label);
        vertexKey = labelKey.mutable_vertex();
        vertexKey->set_type(vertexId.type);
        vertexKey->set_id(vertexId.id);

        status =
                storage.db->Merge(wo, storage.labels, labelKey.SerializeAsString(), tx, deleteStr);
    }

    void AddEdge(const std::string &label, const VertexId &from, const VertexId &to) const {
        txn->Touch(from);
        txn->Touch(to);
        AddEdgeWithDirection(label, OUT, from, to);
        AddEdgeWithDirection(label, IN, to, from);
    }

    void RemoveEdge(const std::string &label, const VertexId &from, const VertexId &to) const {
        txn->Touch(from);
        txn->Touch(to);
        RemoveEdgeWithDirection(label, OUT, from, to);
        RemoveEdgeWithDirection(label, IN, to, from);
    }

    uint64_t Commit() const {
        txMgr->Commit(*txn.get());
        return txn->GetTxId();
    }

    void Rollback() const { txMgr->Rollback(txn->GetTxId()); }

private:
    void AddEdgeWithDirection(
        const std::string &label,
        const Direction direction,
        const VertexId &vertex,
        const VertexId &other) const {
        storage::EdgeKey key;
        key.set_label(label);
        key.set_direction(direction == OUT ? storage::Direction::OUT : storage::Direction::IN);

        storage::VertexKey *vertexKey = key.mutable_vertex();
        vertexKey->set_type(vertex.type);
        vertexKey->set_id(vertex.id);

        storage::VertexKey *otherKey = key.mutable_other();
        otherKey->set_type(other.type);
        otherKey->set_id(other.id);

        rocksdb::Status status =
                storage.db->Merge(wo, storage.edges, key.SerializeAsString(), tx, putStr);
        assert(status.ok());
    }

    void RemoveEdgeWithDirection(
        const std::string &label,
        const Direction direction,
        const VertexId &vertex,
        const VertexId &other) const {
        storage::EdgeKey key;
        key.set_label(label);
        key.set_direction(direction == OUT ? storage::Direction::OUT : storage::Direction::IN);

        storage::VertexKey *vertexKey = key.mutable_vertex();
        vertexKey->set_type(vertex.type);
        vertexKey->set_id(vertex.id);

        storage::VertexKey *otherKey = key.mutable_other();
        otherKey->set_type(other.type);
        otherKey->set_id(other.id);

        rocksdb::Status status =
                storage.db->Merge(wo, storage.edges, key.SerializeAsString(), tx, deleteStr);
        assert(status.ok());
    }

    std::string putStr;
    std::string deleteStr;
    std::shared_ptr<TransactionManager> txMgr;
    rocksdb::WriteOptions wo;
};

class Graph {
public:
    Graph(const std::string &path) : txMgr(make_shared<TransactionManager>()) {
        rocksdb::Options options;
        options.create_if_missing = true;
        options.create_missing_column_families = true;

        rocksdb::ColumnFamilyOptions cfOptions(options);
        cfOptions.comparator = rocksdb::BytewiseComparatorWithU64Ts();
        cfOptions.merge_operator.reset(new TxMergeOperator(txMgr));
        compactionFilter.reset(new TxCompactionFilter(txMgr));
        cfOptions.compaction_filter = compactionFilter.get();

        std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
        descriptors.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, cfOptions));
        descriptors.push_back(rocksdb::ColumnFamilyDescriptor("index", cfOptions));
        descriptors.push_back(rocksdb::ColumnFamilyDescriptor("vertices", cfOptions));
        descriptors.push_back(rocksdb::ColumnFamilyDescriptor("edges", cfOptions));
        descriptors.push_back(rocksdb::ColumnFamilyDescriptor("labels", cfOptions));

        std::vector<rocksdb::ColumnFamilyHandle *> handles;
        rocksdb::Status status =
                rocksdb::DB::Open(options, path, descriptors, &handles, &storage.db);
        assert(status.ok());

        storage._default = handles[0];
        storage.index = handles[1];
        storage.vertices = handles[2];
        storage.edges = handles[3];
        storage.labels = handles[4];

        txMgr->Initialize(storage.db);
    }

    ~Graph() {
        storage.db->DestroyColumnFamilyHandle(storage.edges);
        storage.db->DestroyColumnFamilyHandle(storage.vertices);
        storage.db->DestroyColumnFamilyHandle(storage.index);
        storage.db->DestroyColumnFamilyHandle(storage._default);
        storage.db->Close();
    }

    ReadTransaction *OpenForRead(uint64_t txId = 0) {
        return new ReadTransaction(storage, txMgr->OpenForRead(txId));
    }

    WriteTransaction *OpenForWrite() { return new WriteTransaction(storage, txMgr); }

    friend class Service;

private:
    rocksdb::DB *GetDB() const {
        return storage.db;
    }

    std::shared_ptr<TransactionManager> txMgr;
    Storage storage;
    std::unique_ptr<rocksdb::CompactionFilter> compactionFilter;
};
