#pragma once

#include <cassert>
#include <memory>
#include <ranges>
#include <sstream>

#include "encoding.h"
#include "graph.h"
#include "model.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "transaction.h"

using namespace std;

const std::string ToHex(const rocksdb::Slice &slice) {
    std::stringstream ss;
    ss << "0x";
    for (int i = 0; i < slice.size(); i++) {
        ss << std::format("{:02x}", slice.data()[i]);
    }
    return ss.str();
}

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
        MergeValue value;
        encoding(existing_value).get_merge(value);

        // what to do in this case?
        assert(!txMgr->IsInvalid(value.txId));
        return value.action == DELETE;
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
        const rocksdb::Slice &prefix,
        const std::shared_ptr<Transaction> &txn)
        : txn(txn), prefixStr(prefix.data(), prefix.size()), prefixSlice(prefixStr), fluid(fluid), frozen(frozen) {
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
        while (!foundKey) {
            bool fluidValid = fluid->Valid() && IsValidKey(fluid->key());
            bool frozenValid = frozen->Valid() && IsValidKey(frozen->key());
            int cmp;
            if (!fluidValid && !frozenValid) {
                break;
            }
            if (fluidValid && frozenValid) {
                cmp = comparator->CompareWithoutTimestamp(
                    fluid->key(), true, frozen->key(), false);
            } else if (fluidValid) {
                cmp = -1;
            } else {
                cmp = +1;
            }

            uint64_t keyTxId;
            MergeValue value;
            if (cmp > 0) {
                encoding(frozen->value()).get_merge(value);
                keyTxId = value.txId;
                if (txn->IsExcluded(keyTxId)) {
                    frozen->Next();
                    continue;
                }
                currentKey = frozen->key().ToString();
                assert(frozen->key().ToString().compare(currentKey) == 0);
            } else {
                encoding(fluid->value()).get_merge(value);
                keyTxId = value.txId;
                if (txn->IsExcluded(keyTxId)) {
                    fluid->Next();
                    continue;
                }
                // time-range iterator includes user-defined timestamp in key
                // strip it to filter out subsequent entries with the same key
                currentKey = FluidKey();
            }

            // don't report deleted keys
            if (value.action == PUT) {
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
    bool IsValidKey(const rocksdb::Slice &key) const {
        return key.starts_with(prefixSlice);
    }

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
        const rocksdb::Slice &prefix,
        const std::shared_ptr<Transaction> &txn)
        : EntryIterator(fluid, frozen, prefix, txn) {
        fluid->Seek(prefix);
        frozen->Seek(prefix);
        Next();
    }

protected:
    void populate(const rocksdb::Slice &keySlice) {
        encoding(keySlice).get_vertex(current);
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
        const rocksdb::Slice &prefix,
        const std::shared_ptr<Transaction> &txn)
        : EntryIterator(fluid, frozen, prefix, txn) {
        fluid->Seek(prefix);
        frozen->Seek(prefix);
        Next();
    }

protected:
    void populate(const rocksdb::Slice &keySlice) {
        std::string label;
        encoding(keySlice)
                .get_string(label)
                .get_vertex(current);
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
        const rocksdb::Slice &prefix,
        const std::shared_ptr<Transaction> &txn)
        : EntryIterator(fluid, frozen, prefix, txn) {
        fluid->Seek(prefix);
        frozen->Seek(prefix);
        Next();
    }

protected:
    void populate(const rocksdb::Slice &keySlice) {
        encoding(keySlice).get_string(current);
    }

    std::string ToString(std::string current) { return current; }
};

class EdgeIterator : public EntryIterator<Edge> {
public:
    EdgeIterator(
        rocksdb::Iterator *fluid,
        rocksdb::Iterator *frozen,
        const rocksdb::Slice &prefix,
        const std::shared_ptr<Transaction> &txn)
        : EntryIterator(fluid, frozen, prefix, txn) {
        fluid->Seek(prefix);
        frozen->Seek(prefix);
        Next();
    }

protected:
    void populate(const rocksdb::Slice &keySlice) {
        encoding(keySlice).get_edge(current);
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
        : storage(storage), txn(txn) {
        // TODO: Optimize this - only need to read until last entry that could have been written by txn
        log_window_end = rocksdb::EncodeU64Ts(-1, &log_end_str);
        log_window_start = rocksdb::EncodeU64Ts(txn->GetLogWindowStart(), &log_start_str);
        fluidReadOptions.timestamp = &log_window_end;
        fluidReadOptions.iter_start_ts = &log_window_start;
        fluidReadOptions.total_order_seek = true;
        frozenReadOptions.timestamp = &log_window_start;
        frozenReadOptions.total_order_seek = true;
    }

    virtual ~ReadTransaction() = default;

    virtual bool IsReadOnly() { return true; }

    uint64_t GetTxId() const { return txn->GetTxId(); }

    std::shared_ptr<VertexIterator> GetVerticesByType(const std::string &type) const {
        return std::make_shared<VertexIterator>(
            storage.db->NewIterator(fluidReadOptions, storage.vertices),
            storage.db->NewIterator(frozenReadOptions, storage.vertices),
            encoding().put_string(type).ToSlice(),
            txn);
    }

    std::shared_ptr<IndexVertexIterator> GetVerticesByLabel(const std::string &label, const std::string &type) const {
        return std::make_shared<IndexVertexIterator>(
            storage.db->NewIterator(fluidReadOptions, storage.index),
            storage.db->NewIterator(frozenReadOptions, storage.index),
            encoding().put_string(label).put_string(type).ToSlice(),
            txn);
    }

    std::shared_ptr<LabelIterator> GetLabels(const VertexId &vertexId) const {
        return std::make_shared<LabelIterator>(
            storage.db->NewIterator(fluidReadOptions, storage.labels),
            storage.db->NewIterator(frozenReadOptions, storage.labels),
            encoding().put_vertex(vertexId).ToSlice(),
            txn);
    }

    std::shared_ptr<EdgeIterator> GetEdges(
        const VertexId &vertexId, const std::string &label, const Direction direction) const {
        int size = 10;
        std::vector<rocksdb::PinnableSlice> values(size);
        int n_operands;
        rocksdb::GetMergeOperandsOptions options;
        options.continue_cb = [](const rocksdb::Slice &slice) -> bool { return true; };
        storage.db->GetMergeOperands(fluidReadOptions, storage.edges, "", values.data(), &options, &n_operands);
        return std::make_shared<EdgeIterator>(
            storage.db->NewIterator(fluidReadOptions, storage.edges),
            storage.db->NewIterator(frozenReadOptions, storage.edges),
            encoding().put_vertex(vertexId).put_string(label).put_direction(direction).ToSlice(),
            txn);
    }

    std::shared_ptr<EdgeIterator> GetEdges(const VertexId &vertexId) const {
        return std::make_shared<EdgeIterator>(
            storage.db->NewIterator(fluidReadOptions, storage.edges),
            storage.db->NewIterator(frozenReadOptions, storage.edges),
            encoding().put_vertex(vertexId).ToSlice(),
            txn);
    }

protected:
    rocksdb::ReadOptions fluidReadOptions;
    rocksdb::ReadOptions frozenReadOptions;

    std::string log_start_str;
    rocksdb::Slice log_window_end;
    std::string log_end_str;
    rocksdb::Slice log_window_start;
    std::shared_ptr<Transaction> txn;
    const Storage &storage;
};

class WriteTransaction : public ReadTransaction {
public:
    WriteTransaction(const Storage &storage, const std::shared_ptr<TransactionManager> &txMgr,
                     std::shared_ptr<Transaction> txn)
        : ReadTransaction(storage, txn), txMgr(txMgr) {
        MergeValue put;
        put.action = PUT;
        put.txId = txn->GetTxId();
        putEnc = encoding().put_merge(put);
        MergeValue del;
        del.action = DELETE;
        del.txId = txn->GetTxId();
        delEnc = encoding().put_merge(del);
    }

    virtual bool IsReadOnly() { return false; }

    void AddVertex(const VertexId &vertexId, const ulong raft_log_idx) const {
        Put(storage.vertices, raft_log_idx, encoding().put_vertex(vertexId).ToSlice());
        txMgr->SaveTransaction(*txn, raft_log_idx);
    }

    void RemoveVertex(const VertexId &vertexId, const ulong raft_log_idx) const {
        txn->Touch(vertexId);

        const std::shared_ptr edges(GetEdges(vertexId));
        while (edges->Valid()) {
            const Edge &edge = edges->Get();
            if (edge.direction == OUT) {
                RemoveEdge(edge.label, edge.vertexId, edge.otherId, raft_log_idx);
            } else {
                RemoveEdge(edge.label, edge.otherId, edge.vertexId, raft_log_idx);
            }
            edges->Next();
        }

        const std::shared_ptr labels(GetLabels(vertexId));
        while (labels->Valid()) {
            const std::string &label = labels->Get();
            RemoveLabel(vertexId, label, raft_log_idx);
            labels->Next();
        }

        Delete(storage.vertices, raft_log_idx, encoding().put_vertex(vertexId).ToSlice());

        txMgr->SaveTransaction(*txn, raft_log_idx);
    }

    void AddLabel(const VertexId &vertexId, const std::string &label, const ulong raft_log_idx) const {
        txn->Touch(vertexId);
        Put(storage.index, raft_log_idx,
            encoding().put_string(label).put_vertex(vertexId).ToSlice()
        );
        Put(storage.labels, raft_log_idx,
            encoding().put_vertex(vertexId).put_string(label).ToSlice()
        );
        txMgr->SaveTransaction(*txn, raft_log_idx);
    }

    void RemoveLabel(const VertexId &vertexId, const std::string label, const ulong raft_log_idx) const {
        txn->Touch(vertexId);
        Delete(storage.index, raft_log_idx,
               encoding().put_string(label).put_vertex(vertexId).ToSlice()
        );
        Delete(storage.labels, raft_log_idx,
               encoding().put_vertex(vertexId).put_string(label).ToSlice()
        );
        txMgr->SaveTransaction(*txn, raft_log_idx);
    }

    void AddEdge(const std::string &label, const VertexId &from, const VertexId &to, const ulong raft_log_idx) const {
        txn->Touch(from);
        txn->Touch(to);
        AddEdgeWithDirection(label, OUT, from, to, raft_log_idx);
        AddEdgeWithDirection(label, IN, to, from, raft_log_idx);
        txMgr->SaveTransaction(*txn, raft_log_idx);
    }

    void RemoveEdge(const std::string &label, const VertexId &from, const VertexId &to,
                    const ulong raft_log_idx) const {
        txn->Touch(from);
        txn->Touch(to);
        RemoveEdgeWithDirection(label, OUT, from, to, raft_log_idx);
        RemoveEdgeWithDirection(label, IN, to, from, raft_log_idx);
        txMgr->SaveTransaction(*txn, raft_log_idx);
    }

    uint64_t Commit(ulong raft_log_idx) const {
        txMgr->Commit(*txn.get(), raft_log_idx);
        return txn->GetTxId();
    }

    void Rollback(ulong raft_log_idx) const { txMgr->Rollback(txn->GetTxId(), raft_log_idx); }

private:
    void AddEdgeWithDirection(
        const std::string &label,
        const Direction direction,
        const VertexId &vertex,
        const VertexId &other,
        ulong raft_log_idx) const {
        Put(storage.edges, raft_log_idx,
            encoding().put_edge({vertex, other, label, direction}).ToSlice()
        );
    }

    void RemoveEdgeWithDirection(
        const std::string &label,
        const Direction direction,
        const VertexId &vertex,
        const VertexId &other,
        ulong raft_log_idx) const {
        Delete(storage.edges, raft_log_idx,
               encoding().put_edge({vertex, other, label, direction}).ToSlice()
        );
    }

    void Put(rocksdb::ColumnFamilyHandle *handle, ulong raft_log_idx, const rocksdb::Slice &key) const {
        std::string logStr;
        rocksdb::Status status = storage.db->Put(wo, handle, key, rocksdb::EncodeU64Ts(raft_log_idx, &logStr),
                                                 putEnc.ToSlice());
        assert(status.ok());
    }

    void Delete(rocksdb::ColumnFamilyHandle *handle, ulong raft_log_idx, const rocksdb::Slice &key) const {
        std::string logStr;
        rocksdb::Status status = storage.db->Put(wo, handle, key, rocksdb::EncodeU64Ts(raft_log_idx, &logStr),
                                                 delEnc.ToSlice());
        assert(status.ok());
    }

    encoding putEnc;
    encoding delEnc;
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
        storage.db->DestroyColumnFamilyHandle(storage.labels);
        storage.db->DestroyColumnFamilyHandle(storage.edges);
        storage.db->DestroyColumnFamilyHandle(storage.vertices);
        storage.db->DestroyColumnFamilyHandle(storage.index);
        storage.db->DestroyColumnFamilyHandle(storage._default);
        storage.db->Close();
    }

    ReadTransaction *OpenForRead(uint64_t txId = 0) {
        return new ReadTransaction(storage, txMgr->OpenForRead(txId));
    }

    WriteTransaction *OpenForWrite(ulong raft_log_idx) {
        return new WriteTransaction(storage, txMgr, txMgr->OpenForWrite(raft_log_idx));
    }

    friend class StateMachine;

private:
    const Storage &GetStorage() const {
        return storage;
    }

    std::shared_ptr<TransactionManager> txMgr;
    Storage storage;
    std::unique_ptr<rocksdb::CompactionFilter> compactionFilter;
};
