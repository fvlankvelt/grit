#pragma once

#include <cassert>
#include <memory>
#include <sstream>

#include "encoding.h"
#include "graph.h"
#include "model.h"
#include "storage.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "transaction.h"

using namespace std;

inline const std::string ToHex(const rocksdb::Slice &slice) {
    std::stringstream ss;
    ss << "0x";
    for (int i = 0; i < slice.size(); i++) {
        ss << std::format("{:02x}", slice.data()[i]);
    }
    return ss.str();
}

template<class T>
class EntryIterator {
public:
    EntryIterator(
        rocksdb::Iterator *upstream,
        const rocksdb::Slice &prefix,
        const std::shared_ptr<Transaction> &txn)
        : txn(txn), prefixStr(prefix.data(), prefix.size()), prefixSlice(prefixStr), upstream(upstream) {
        comparator = rocksdb::BytewiseComparatorWithU64Ts();
        {
            MergeGuard guard(txn);
            upstream->Seek(prefix);
        }
    }

    virtual ~EntryIterator() = default;

    const T &Get() const { return current; }

    bool Valid() const { return valid; }

    /**
     * Proceed to next key.  Skip older updates to a key and updates from excluded transactions.
     */
    void Next() {
        MergeGuard guard(txn);

        bool foundKey = false;
        while (!foundKey && upstream->Valid() && IsValidKey(upstream->key())) {
            storage::ItemState value;
            value.ParseFromString(upstream->value().ToString());
            currentKey = upstream->key().ToString();
            assert(upstream->key().ToString().compare(currentKey) == 0);

            // don't report deleted keys
            if (value.action() == storage::PUT) {
                populate(currentKey);
                foundKey = true;
            }

            // skip all remaining entries for the same key
            while (upstream->Valid() && upstream->key().ToString().compare(currentKey) == 0) {
                upstream->Next();
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

    std::string currentKey;
    const std::shared_ptr<Transaction> txn;
    const rocksdb::Comparator *comparator;
    std::string prefixStr;
    rocksdb::Slice prefixSlice;
    std::unique_ptr<rocksdb::Iterator> upstream;
};

class VertexIterator : public EntryIterator<VertexId> {
public:
    VertexIterator(
        rocksdb::Iterator *upstream,
        const rocksdb::Slice &prefix,
        const std::shared_ptr<Transaction> &txn)
        : EntryIterator(upstream, prefix, txn) {
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
        rocksdb::Iterator *upstream,
        const rocksdb::Slice &prefix,
        const std::shared_ptr<Transaction> &txn)
        : EntryIterator(upstream, prefix, txn) {
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
        rocksdb::Iterator *upstream,
        const rocksdb::Slice &prefix,
        const std::shared_ptr<Transaction> &txn)
        : EntryIterator(upstream, prefix, txn) {
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
        rocksdb::Iterator *upstream,
        const rocksdb::Slice &prefix,
        const std::shared_ptr<Transaction> &txn)
        : EntryIterator(upstream, prefix, txn) {
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
        log_window_end = rocksdb::EncodeU64Ts(txn->GetLastLogIdx(), &log_end_str);
        readOptions.timestamp = &log_window_end;
        readOptions.total_order_seek = true;
    }

    virtual ~ReadTransaction() = default;

    virtual bool IsReadOnly() { return true; }

    uint64_t GetTxId() const { return txn->GetTxId(); }

    std::shared_ptr<VertexIterator> GetVerticesByType(const std::string &type) const {
        return std::make_shared<VertexIterator>(
            storage.db->NewIterator(readOptions, storage.vertices),
            encoding().put_string(type).ToSlice(),
            txn);
    }

    std::shared_ptr<IndexVertexIterator> GetVerticesByLabel(const std::string &label, const std::string &type) const {
        return std::make_shared<IndexVertexIterator>(
            storage.db->NewIterator(readOptions, storage.index),
            encoding().put_string(label).put_string(type).ToSlice(),
            txn);
    }

    std::shared_ptr<LabelIterator> GetLabels(const VertexId &vertexId) const {
        return std::make_shared<LabelIterator>(
            storage.db->NewIterator(readOptions, storage.labels),
            encoding().put_vertex(vertexId).ToSlice(),
            txn);
    }

    std::shared_ptr<EdgeIterator> GetEdges(
        const VertexId &vertexId, const std::string &label, const Direction direction) const {
        return std::make_shared<EdgeIterator>(
            storage.db->NewIterator(readOptions, storage.edges),
            encoding().put_vertex(vertexId).put_string(label).put_direction(direction).ToSlice(),
            txn);
    }

    std::shared_ptr<EdgeIterator> GetEdges(const VertexId &vertexId) const {
        return std::make_shared<EdgeIterator>(
            storage.db->NewIterator(readOptions, storage.edges),
            encoding().put_vertex(vertexId).ToSlice(),
            txn);
    }

protected:
    rocksdb::ReadOptions readOptions;

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
        storage::ItemOperation put;
        put.set_action(storage::PUT);
        put.set_txid(txn->GetTxId());
        putSlice = put.SerializeAsString();

        storage::ItemOperation del;
        del.set_action(storage::DELETE);
        del.set_txid(txn->GetTxId());
        delSlice = del.SerializeAsString();
    }

    virtual bool IsReadOnly() { return false; }

    void AddVertex(const VertexId &vertexId, WriteContext &ctx) const {
        Put(storage.vertices, ctx, encoding().put_vertex(vertexId).ToSlice());
        txMgr->Touch(*txn, ctx, vertexId);
    }

    void RemoveVertex(const VertexId &vertexId, WriteContext &ctx) const {
        const std::shared_ptr edges(GetEdges(vertexId));
        while (edges->Valid()) {
            const Edge &edge = edges->Get();
            if (edge.direction == OUT) {
                RemoveEdge(edge.label, edge.vertexId, edge.otherId, ctx);
            } else {
                RemoveEdge(edge.label, edge.otherId, edge.vertexId, ctx);
            }
            edges->Next();
        }

        const std::shared_ptr labels(GetLabels(vertexId));
        while (labels->Valid()) {
            const std::string &label = labels->Get();
            RemoveLabel(vertexId, label, ctx);
            labels->Next();
        }

        Delete(storage.vertices, ctx, encoding().put_vertex(vertexId).ToSlice());

        txMgr->Touch(*txn, ctx, vertexId);
    }

    void AddLabel(const VertexId &vertexId, const std::string &label, WriteContext &ctx) const {
        Put(storage.index, ctx,
            encoding().put_string(label).put_vertex(vertexId).ToSlice()
        );
        Put(storage.labels, ctx,
            encoding().put_vertex(vertexId).put_string(label).ToSlice()
        );
        txMgr->Touch(*txn, ctx, vertexId);
    }

    void RemoveLabel(const VertexId &vertexId, const std::string label, WriteContext &ctx) const {
        Delete(storage.index, ctx,
               encoding().put_string(label).put_vertex(vertexId).ToSlice()
        );
        Delete(storage.labels, ctx,
               encoding().put_vertex(vertexId).put_string(label).ToSlice()
        );
        txMgr->Touch(*txn, ctx, vertexId);
    }

    void AddEdge(const std::string &label, const VertexId &from, const VertexId &to, WriteContext &ctx) const {
        AddEdgeWithDirection(label, OUT, from, to, ctx);
        AddEdgeWithDirection(label, IN, to, from, ctx);
        txMgr->Touch(*txn, ctx, from);
        txMgr->Touch(*txn, ctx, to);
    }

    void RemoveEdge(const std::string &label, const VertexId &from, const VertexId &to, WriteContext& ctx) const {
        RemoveEdgeWithDirection(label, OUT, from, to, ctx);
        RemoveEdgeWithDirection(label, IN, to, from, ctx);
        txMgr->Touch(*txn, ctx, from);
        txMgr->Touch(*txn, ctx, to);
    }

    void Commit(WriteContext &ctx) const {
        txMgr->Commit(*txn.get(), ctx);
    }

    void Rollback(WriteContext &ctx) const { txMgr->Rollback(txn->GetTxId(), ctx); }

private:
    void AddEdgeWithDirection(
        const std::string &label,
        const Direction direction,
        const VertexId &vertex,
        const VertexId &other,
        WriteContext& ctx) const {
        Put(storage.edges, ctx,
            encoding().put_edge({vertex, other, label, direction}).ToSlice()
        );
    }

    void RemoveEdgeWithDirection(
        const std::string &label,
        const Direction direction,
        const VertexId &vertex,
        const VertexId &other,
        WriteContext &ctx) const {
        Delete(storage.edges, ctx,
               encoding().put_edge({vertex, other, label, direction}).ToSlice()
        );
    }

    void Put(rocksdb::ColumnFamilyHandle *handle, WriteContext &ctx, const rocksdb::Slice &key) const {
        rocksdb::Status status = ctx.wb.Merge(handle, key, ctx.ts, putSlice);
        assert(status.ok());
    }

    void Delete(rocksdb::ColumnFamilyHandle *handle, WriteContext &ctx, const rocksdb::Slice &key) const {
        rocksdb::Status status = ctx.wb.Merge(handle, key, ctx.ts, delSlice);
        assert(status.ok());
    }

    std::string putSlice;
    std::string delSlice;
    std::shared_ptr<TransactionManager> txMgr;
};

class Graph {
public:
    explicit Graph(const Storage &storage) : storage(storage), txMgr(storage.GetTransactionManager()) {
    }

    ReadTransaction *OpenForRead(uint64_t txId = 0) {
        return new ReadTransaction(storage, txMgr->OpenForRead(txId));
    }

    WriteTransaction *OpenForWrite(WriteContext &ctx) {
        return new WriteTransaction(storage, txMgr, txMgr->OpenForWrite(ctx));
    }

    friend class StateMachine;

private:
    const Storage &GetStorage() const {
        return storage;
    }

    std::shared_ptr<TransactionManager> txMgr;
    const Storage &storage;
};
