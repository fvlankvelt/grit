#pragma once

#include <algorithm>
#include <cinttypes>
#include <map>
#include <memory>
#include <set>
#include <utility>
#include <shared_mutex>

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>

#include "model.h"
#include "encoding.h"
#include "storage.pb.h"

enum TransactionException { TX_IS_READONLY, TX_CONFLICT, TX_INVALIDATED, TX_NOT_IN_PROGRESS };

class WriteContext {
public:
    explicit WriteContext(rocksdb::DB *db, uint64_t log_idx)
        : db(db), raft_log_idx(log_idx), wb(rocksdb::WriteBatch()) {
        ts = rocksdb::EncodeU64Ts(log_idx, &tsStr);
    }

    WriteContext(const WriteContext &) = delete;

    ~WriteContext() {
        db->Write(rocksdb::WriteOptions(), &wb);
    }

    rocksdb::WriteBatch wb;
    uint64_t raft_log_idx;
    rocksdb::Slice ts;

private:
    rocksdb::DB *db;
    std::string tsStr;
};


class ItemStateContext {
public:
    virtual ~ItemStateContext() = default;

    virtual bool IsExcluded(uint64_t otherTxId) const = 0;

    static std::shared_ptr<ItemStateContext> Get(const std::shared_ptr<ItemStateContext> &defaultContext) {
        return threadContext == nullptr ? defaultContext : threadContext;
    }

    static void Set(const std::shared_ptr<ItemStateContext> &context) {
        threadContext = context;
    }

private:
    inline static thread_local std::shared_ptr<ItemStateContext> threadContext;
};

class Transaction : public ItemStateContext {
public:
    Transaction(
        const uint64_t txId,
        const uint64_t lastLogIdx,
        const std::set<uint64_t> &inProgress,
        bool readOnly
    ) : txId(txId), lastLogIdx(lastLogIdx), readOnly(readOnly), inProgress(inProgress) {
    }

    uint64_t GetTxId() const { return txId; }

    uint64_t GetLastLogIdx() const { return lastLogIdx; }

    bool IsExcluded(uint64_t otherTxId) const override {
        return otherTxId > txId || inProgress.find(otherTxId) != inProgress.end();
    }

    friend class TransactionManager;

private:
    void Touch(const VertexId &key) {
        if (readOnly) {
            throw TX_IS_READONLY;
        }
        touched.insert(key);
    }

    // last transaction that can be read
    uint64_t txId;

    // last logIdx that can be read
    uint64_t lastLogIdx;

    bool readOnly;
    std::set<uint64_t> inProgress;
    std::set<VertexId> touched;
};

class TransactionManager : public ItemStateContext {
public:
    TransactionManager() {
    }

    void Initialize(rocksdb::DB *db, rocksdb::ColumnFamilyHandle *cf) {
        db_ = db;
        tx_cf = cf;

        std::string txStr;
        rocksdb::Slice txSlice(rocksdb::EncodeU64Ts(-1, &txStr));
        rocksdb::ReadOptions options;
        options.timestamp = &txSlice;

        std::string strState;
        auto status = db->Get(options, cf, "tx_mgr:state", &strState);
        if (status.IsNotFound()) {
            return;
        }
        assert(status.ok());

        storage::TransactionManagerState state;
        state.ParseFromString(strState);
        readTxId = state.lasttxid();
        invalid.clear();
        for (const unsigned long &txId: state.invalid()) {
            invalid.insert(txId);
        }

        for (const unsigned long &txId: state.committed()) {
            std::stringstream ss;
            ss << "tx:" << txId;
            std::string value;
            auto status = db->Get(options, tx_cf,ss.str(), &value);
            if (status.ok()) {
                storage::Transaction tx;
                tx.ParseFromString(value);
                std::set<VertexId> touched;
                auto vertex_keys = tx.touched();
                for (auto &vertex_key: vertex_keys) {
                    VertexId vid;
                    encoding(vertex_key).get_vertex(vid);
                    touched.insert(vid);
                }
                recent.insert(std::pair(txId, touched));
            } else {
                recent.insert(std::pair(txId, std::set<VertexId>()));
            }
        }

        ulong last_log_idx;
        auto txWithStarts = GetInProgress(readTxId, last_log_idx);
        inProgress.clear();
        for (auto &txId: txWithStarts) {
            inProgress.insert(std::pair(txId, LoadTransaction(txId)));
        }
    }

    std::shared_ptr<Transaction> OpenForRead(uint64_t txId = 0) const {
        std::shared_lock db_lock(db_mutex);
        if (txId == 0) {
            std::set<uint64_t> progress;
            for (const auto &inProgres: inProgress) {
                progress.insert(inProgres.first);
            }
            return std::make_shared<Transaction>(readTxId, readLastLogIdx, progress, true);
        } else {
            ulong raft_log_idx;
            std::set<uint64_t> progress = GetInProgress(txId, raft_log_idx);
            return std::make_shared<Transaction>(txId, raft_log_idx, progress, true);
        }
    }

    std::shared_ptr<Transaction> OpenForWrite(WriteContext &ctx) {
        std::unique_lock db_lock(db_mutex);

        std::set<uint64_t> mapping;
        for (const auto &inProgres: inProgress) {
            mapping.insert(inProgres.first);
        }
        readTxId++;
        std::shared_ptr<Transaction> tx = std::make_shared<Transaction>(readTxId, -1, mapping, false);
        // want to be able to read anything that's been written in this transaction
        // this InProgress record will be overwritten when the transaction is committed
        SaveInProgress(readTxId, -1, ctx, mapping);

        inProgress.insert(std::pair(tx->GetTxId(), tx));
        readTxId++;
        SaveCurrentInProgress(ctx);

        WriteState(ctx);
        return tx;
    }

    void OpenSlicer(WriteContext &ctx) {
        std::unique_lock db_lock(db_mutex);
        readLastLogIdx = ctx.raft_log_idx;
    }

    std::shared_ptr<Transaction> GetWriteTxn(uint64_t txId) {
        std::shared_lock db_lock(db_mutex);
        auto found = inProgress.find(txId);
        if (found != inProgress.end()) {
            return found->second;
        } else {
            throw TX_NOT_IN_PROGRESS;
        }
    }

    bool IsInvalid(uint64_t txId) const {
        std::shared_lock lock(invalid_mutex);
        return invalid.contains(txId);
    }

    bool IsExcluded(uint64_t txId) const override {
        std::shared_lock lock(db_mutex);
        return inProgress.contains(txId);
    }

    void Commit(const Transaction &txn, WriteContext &ctx) {
        std::unique_lock db_lock(db_mutex);

        uint64_t txId = txn.GetTxId();
        if (inProgress.find(txId) == inProgress.end()) {
            throw TX_NOT_IN_PROGRESS;
        }

        try {
            // validate txn against all transactions from its in-progress set
            // that have been committed since its start.
            for (auto &inPTxId: txn.inProgress) {
                if (inProgress.find(inPTxId) != inProgress.end() || IsInvalid(inPTxId)) {
                    continue;
                }
                if (recent.find(inPTxId) == recent.end()) {
                    // transaction references a committed transaction, but that has
                    // expired already.  So we cannot check if it conflicts.
                    throw TX_INVALIDATED;
                } else {
                    if (const std::set<VertexId>& ref = recent.find(inPTxId)->second; Conflict(ref, txn.touched)) {
                        throw TX_CONFLICT;
                    }
                }
            }

            // similar for transactions that started later, but that have already
            // been committed.
            for (auto &it: recent) {
                if (it.first > txId && Conflict(it.second, txn.touched)) {
                    throw TX_CONFLICT;
                }
            }

            // hurray!  No conflicts detected
            recent.insert(std::pair(txId, txn.touched));
            readLastLogIdx = ctx.raft_log_idx;
            // subsequent reads from this transaction are limited to what's been written
            SaveInProgress(txId, readLastLogIdx, ctx, txn.inProgress);
            inProgress.erase(txId);
            readTxId++;
            // next read transaction can read
            SaveCurrentInProgress(ctx);
            WriteState(ctx);
        } catch (TransactionException te) {
            {
                std::unique_lock lock(invalid_mutex);
                invalid.insert(txId);
            }
            inProgress.erase(txId);
            SaveCurrentInProgress(ctx);
            WriteState(ctx);
            throw;
        }
    }

    void Rollback(uint64_t txId, WriteContext &ctx) {
        std::unique_lock db_lock(db_mutex);
        {
            std::unique_lock lock(invalid_mutex);
            invalid.insert(txId);
        }
        inProgress.erase(txId);
        SaveCurrentInProgress(ctx);
        WriteState(ctx);
    }

    void Touch(Transaction &txn, WriteContext &ctx, const VertexId &vid) {
        txn.Touch(vid);

        storage::TransactionOperation op;
        op.set_txid(txn.GetTxId());
        auto touched = op.mutable_touched();
        *touched = encoding().put_vertex(vid).ToString();

        std::stringstream ss;
        ss << "tx:" << txn.GetTxId();
        auto status = ctx.wb.Merge(tx_cf, ss.str(), ctx.ts, rocksdb::Slice(op.SerializeAsString()));
        assert(status.ok());
    }

    /**
     * Remove from memory all committed transactions from before expireTx.
     * This will abort any transactions that started before expireTx.
     */
    void Advance(uint64_t expireTx) {
    }

private:
    std::set<uint64_t> GetInProgress(uint64_t txId, ulong &raft_log_idx) const {
        std::string tsStr;
        rocksdb::Slice ts(rocksdb::EncodeU64Ts(-1, &tsStr));

        rocksdb::ReadOptions options;
        options.timestamp = &ts;

        std::stringstream ss;
        ss << "tx_mgr:in_progress:" << txId;

        std::string value;
        auto status = db_->Get(options, tx_cf, ss.str(), &value);
        if (!status.ok()) {
            return std::set<uint64_t>();
        }

        storage::InProgress inp;
        inp.ParseFromString(value);

        raft_log_idx = inp.logidx();

        std::set<uint64_t> result;
        for (const auto &it: inp.txids()) {
            result.insert(it);
        }
        return result;
    }

    void SaveCurrentInProgress(WriteContext &ctx) const {
        std::set<uint64_t> txIds;
        for (auto &it: inProgress) {
            txIds.insert(it.first);
        }
        SaveInProgress(readTxId, readLastLogIdx, ctx, txIds);
    }

    void SaveInProgress(ulong txId, ulong read_log_idx, WriteContext &ctx, const std::set<uint64_t> &inProgress) const {
        std::stringstream ss;
        ss << "tx_mgr:in_progress:" << txId;

        storage::InProgress value;
        value.set_logidx(read_log_idx);
        for (auto &txId: inProgress) {
            value.add_txids(txId);
        }
        auto status = ctx.wb.Put(tx_cf, ss.str(), ctx.ts, value.SerializeAsString());
        assert(status.ok());
    }

    void WriteState(WriteContext &ctx) const {
        storage::TransactionManagerState state;
        state.set_lasttxid(readTxId);
        {
            std::shared_lock lock(invalid_mutex);
            for (unsigned long it: invalid) {
                state.add_invalid(it);
            }
        }
        for (const auto &it: recent) {
            state.add_committed(it.first);
        }

        auto status = ctx.wb.Put(tx_cf, "tx_mgr:state", ctx.ts, state.SerializeAsString());
        assert(status.ok());
    }

    std::shared_ptr<Transaction> LoadTransaction(uint64_t txId) {
        ulong raft_log_idx;
        std::set<uint64_t> mapping = GetInProgress(txId, raft_log_idx);

        std::string tsStr;
        rocksdb::Slice ts(rocksdb::EncodeU64Ts(-1, &tsStr));
        rocksdb::ReadOptions readOptions;
        readOptions.timestamp = &ts;

        std::stringstream ss;
        ss << "tx:" << txId;

        std::string value;
        auto status = db_->Get(readOptions, tx_cf, ss.str(), &value);
        assert(status.ok());
        storage::Transaction state;
        state.ParseFromString(value);
        std::shared_ptr<Transaction> txn = std::make_shared<Transaction>(
            txId,
            -1,
            mapping,
            false
        );
        for (const auto &it: state.touched()) {
            VertexId vid;
            encoding(it).get_vertex(vid);
            txn->Touch(vid);
        }
        return txn;
    }


    bool Conflict(const std::set<VertexId> &a, const std::set<VertexId> &b) {
        std::set<VertexId> out;
        std::set_intersection(
            a.begin(), a.end(), b.begin(), b.end(), std::inserter(out, out.begin()));
        return out.begin() != out.end();
    }

    // guard for invalid list - it is used a lot from merge operator to validate entries
    mutable std::shared_mutex invalid_mutex;
    // guard for rest of state
    mutable std::shared_mutex db_mutex;

    rocksdb::DB *db_;
    rocksdb::ColumnFamilyHandle *tx_cf;

    uint64_t readTxId = 1;
    uint64_t readLastLogIdx;
    std::map<uint64_t, std::set<VertexId> > recent;
    std::map<uint64_t, std::shared_ptr<Transaction> > inProgress;

    // all rolled back and invalidated transactions - this set can be pruned
    // by a scan (GC) over all data; any updates from invalidated txns are removed
    // on a compaction.
    std::set<uint64_t> invalid;
};
