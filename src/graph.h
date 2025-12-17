#pragma once

#include <cassert>
#include <memory>
#include <queue>
#include <sstream>
#include <utility>

#include "encoding.h"
#include "graph.h"
#include "model.h"
#include "storage.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "transaction.h"

template<class T>
class EntryIterator {
public:
    EntryIterator(
        rocksdb::Iterator *upstream,
        const rocksdb::Slice &prefix,
        const std::shared_ptr<Transaction> &txn)
        : txn(txn), prefixStr(prefix.data(), prefix.size()), prefixSlice(prefixStr), upstream(upstream) {
        {
            ItemStateContextGuard guard(txn);
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
        ItemStateContextGuard guard(txn);

        bool foundKey = false;
        while (!foundKey && upstream->Valid() && IsValidKey(upstream->key())) {
            storage::ItemState value;
            value.ParseFromString(upstream->value().ToString());
            currentKey = upstream->key().ToString();
            assert(upstream->key().ToString() == currentKey);

            // don't report deleted keys
            if (value.action() == storage::PUT) {
                populate(currentKey);
                foundKey = true;
            }

            upstream->Next();
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
    void populate(const rocksdb::Slice &keySlice) override {
        encoding(keySlice).get_vertex(current);
    }

    std::string ToString(VertexId id) override {
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
    void populate(const rocksdb::Slice &keySlice) override {
        std::string label;
        encoding(keySlice)
                .get_string(label)
                .get_vertex(current);
    }

    std::string ToString(VertexId id) override {
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
    void populate(const rocksdb::Slice &keySlice) override {
        VertexId vid;
        encoding(keySlice).get_vertex(vid).get_string(current);
    }

    std::string ToString(std::string current) override { return current; }
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
    void populate(const rocksdb::Slice &keySlice) override {
        encoding(keySlice).get_edge(current);
    }

    std::string ToString(Edge edge) override {
        std::stringstream ss;
        ss << edge.direction << " " << edge.label << " " << edge.vertexId.id << " - "
                << edge.otherId.id;
        return ss.str();
    }
};

struct SlicingStats {
    SlicingStats() : numKeys(0), numOperations(0) {
    }

    int32_t numKeys;
    int32_t numOperations;
};

template<class T>
constexpr bool operator<(const std::pair<T, SlicingStats> &a, const std::pair<T, SlicingStats> &b) {
    // We want the biggest bang for the buck.  So keys with most operations to compact should remain.
    // There is a cost for writing more keys though.
    //
    // The top of the priority queue is the key with the least improvement.  (so it can be easily dropped)
    // For the priority sorting, keys to retain should therefore be *smaller* than the ones with less operations.
    return a.second.numOperations - 3 * a.second.numKeys > b.second.numOperations - 3 * b.second.numKeys ;
}

template<class T>
class SlicingCollector {
public:
    explicit SlicingCollector(int topN) : top_n_(topN) {
    }

    virtual ~SlicingCollector() = default;

    std::vector<std::pair<T, SlicingStats> > Collect(const std::shared_ptr<rocksdb::Iterator> &iter) {
        std::priority_queue<std::pair<T, SlicingStats> > items;

        bool first = true;
        T prev;
        SlicingStats stats;
        while (iter->Valid()) {
            const T item = map(iter->key());
            if (first || !equal(item, prev)) {
                if (!first) {
                    items.push(std::pair(item, stats));
                    if (items.size() > top_n_) {
                        items.pop();
                    }
                }
                first = false;
                stats = SlicingStats();
                prev = item;
            }
            storage::ItemState state;
            state.ParseFromString(iter->value().ToString());

            stats.numKeys++;
            stats.numOperations += state.numoperands();

            iter->Next();
        }
        if (!first) {
            items.push(std::pair(prev, stats));
            if (items.size() > top_n_) {
                items.pop();
            }
        }

        std::vector<std::pair<T, SlicingStats> > results (items.size());
        while (!items.empty()) {
            results.emplace_back(items.top());
            items.pop();
        }
        return results;
    }

    virtual T map(const rocksdb::Slice &key) const = 0;

    virtual bool equal(const T &a, const T &b) const = 0;

private:
    int top_n_;
};

class VertexSlicingCollector : public SlicingCollector<VertexId> {
public:
    explicit VertexSlicingCollector(int topN = 100) : SlicingCollector(topN) {
    }

    VertexId map(const rocksdb::Slice &key) const override {
        VertexId vid;
        encoding(key).get_vertex(vid);
        return vid;
    }

    bool equal(const VertexId &a, const VertexId &b) const override {
        return a.type == b.type && a.id == b.id;
    }
};

class LabelSlicingCollector : public SlicingCollector<std::string> {
public:
    explicit LabelSlicingCollector(int topN = 100) : SlicingCollector(topN) {
    }

    std::string map(const rocksdb::Slice &key) const override {
        std::string label;
        encoding(key).get_string(label);
        return label;
    }

    bool equal(const std::string &a, const std::string &b) const override {
        return a == b;
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

    // collect the top labels with the most churn - candidates for slicing
    std::vector<std::pair<std::string, SlicingStats> > GetTopLabelsByChurn() const {
        LabelSlicingCollector collector;
        return collector.Collect(
            std::shared_ptr<rocksdb::Iterator>(storage.db->NewIterator(readOptions, storage.index)));
    }

    // collect the top vertices with the most churn of labels - candidates for slicing
    std::vector<std::pair<VertexId, SlicingStats> > GetTopVerticesByLabelChurn() const {
        VertexSlicingCollector collector;
        return collector.Collect(
            std::shared_ptr<rocksdb::Iterator>(storage.db->NewIterator(readOptions, storage.labels)));
    }

    // collect the top labels with the most churn of edges - candidates for slicing
    std::vector<std::pair<VertexId, SlicingStats> > GetTopVerticesByEdgeChurn() const {
        VertexSlicingCollector collector;
        return collector.Collect(
            std::shared_ptr<rocksdb::Iterator>(storage.db->NewIterator(readOptions, storage.edges)));
    }

protected:
    rocksdb::ReadOptions readOptions;

    rocksdb::Slice log_window_end;
    std::string log_end_str;
    std::shared_ptr<Transaction> txn;
    const Storage &storage;
};

class WriteTransaction : public ReadTransaction {
public:
    WriteTransaction(const Storage &storage, const std::shared_ptr<TransactionManager> &txMgr,
                     const std::shared_ptr<Transaction> &txn)
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

    bool IsReadOnly() override { return false; }

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

    void RemoveLabel(const VertexId &vertexId, const std::string &label, WriteContext &ctx) const {
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

    void RemoveEdge(const std::string &label, const VertexId &from, const VertexId &to, WriteContext &ctx) const {
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
        WriteContext &ctx) const {
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

/*
 * Slicing is done outside a transactional context.  Since it Puts and Deletes keys, these cannot be filtered out later
 * by the merge operator.  (they form the starting point of the list of operands)  Slicing achieves effectively the same
 * as a compaction - but on a finer grained level.
 */
class Slicer {
public:
    explicit Slicer(const Storage &storage) : storage(storage) {
    }

    void SliceIndex(const std::string &label, WriteContext &ctx) const {
        SlicePrefix(storage.index, encoding().put_string(label).ToString(), ctx);
    }

    void SliceLabels(const VertexId &vid, WriteContext &ctx) const {
        SlicePrefix(storage.labels, encoding().put_vertex(vid).ToString(), ctx);
    }

    void SliceEdges(const VertexId &vid, WriteContext &ctx) const {
        SlicePrefix(storage.edges, encoding().put_vertex(vid).ToString(), ctx);
    }

private:
    void SlicePrefix(rocksdb::ColumnFamilyHandle *cf, const std::string &prefix, WriteContext &ctx) const {
        rocksdb::ReadOptions readOptions;
        std::string tsStr;
        rocksdb::Slice tsSlice = rocksdb::EncodeU64Ts(-1, &tsStr);
        readOptions.timestamp = &tsSlice;

        rocksdb::Iterator *upstream = storage.db->NewIterator(readOptions, cf);
        upstream->Seek(prefix);
        while (upstream->Valid() && upstream->key().starts_with(prefix)) {
            storage::ItemState value;
            value.ParseFromString(upstream->value().ToString());
            const rocksdb::Slice &currentKey = upstream->key();
            if (value.action() == storage::PUT || value.putsinprogress_size() > 0) {
                // TODO: only PUT when number of updates passes a threshold?
                ctx.wb.Put(cf, currentKey, ctx.ts, upstream->value().ToString());
            } else {
                ctx.wb.Delete(cf, currentKey, ctx.ts);
            }
            upstream->Next();
        }
    }

    const Storage &storage;
};

class Graph {
public:
    explicit Graph(const Storage &storage) : txMgr(storage.GetTransactionManager()), storage(storage) {
    }

    std::shared_ptr<ReadTransaction> OpenForRead(uint64_t txId = 0) {
        return std::make_shared<ReadTransaction>(storage, txMgr->OpenForRead(txId));
    }

    std::shared_ptr<WriteTransaction> OpenForWrite(WriteContext &ctx) {
        return std::make_shared<WriteTransaction>(storage, txMgr, txMgr->OpenForWrite(ctx));
    }

    std::shared_ptr<Slicer> OpenSlicer(WriteContext& ctx) {
        txMgr->OpenSlicer(ctx);
        return std::make_shared<Slicer>(storage);
    }

    friend class StateMachine;

private:
    const Storage &GetStorage() const {
        return storage;
    }

    std::shared_ptr<TransactionManager> txMgr;
    const Storage &storage;
};
