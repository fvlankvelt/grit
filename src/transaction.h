#pragma once

#include <algorithm>
#include <cinttypes>
#include <map>
#include <memory>
#include <set>
#include <utility>
#include <shared_mutex>

#include "model.h"
#include "storage.pb.h"

enum TransactionException { TX_IS_READONLY, TX_CONFLICT, TX_INVALIDATED, TX_NOT_IN_PROGRESS };

class TransactionContext {
public:
    TransactionContext() = default;

    virtual ~TransactionContext() = default;

    virtual bool IsInvalid(uint64_t txId) const = 0;
};

class Transaction {
public:
    Transaction(
        const TransactionContext &ctx,
        const uint64_t txId,
        const uint64_t liveStart,
        const std::map<uint64_t, uint64_t>& inProgressWithStart,
        bool readOnly
    ) : ctx(ctx), txId(txId), readOnly(readOnly), liveStart(liveStart) {
        for (auto &it: inProgressWithStart) {
            inProgress.insert(it.first);
            if (it.second < this->liveStart) {
                this->liveStart = it.second;
            }
        }
    }

    void Touch(const VertexId &key) {
        if (readOnly) {
            throw TX_IS_READONLY;
        }
        touched.insert(key);
    }

    uint64_t GetTxId() const { return txId; }

    uint64_t GetLogWindowStart() const { return liveStart; }

    bool IsExcluded(uint64_t otherTxId) const {
        return otherTxId > txId || inProgress.find(otherTxId) != inProgress.end() || ctx.IsInvalid(otherTxId);
    }

    friend class TransactionManager;

private:
    const TransactionContext &ctx;

    // last transaction that can be read
    uint64_t txId;

    // start of in-progress transaction window, there are no in-progress txns before it.
    // All transactions that started after it have a start logIdx bigger than this.
    uint64_t liveStart;

    bool readOnly;
    std::set<uint64_t> inProgress;
    std::set<VertexId> touched;
};

class TransactionManager : public TransactionContext {
public:
    TransactionManager() : db_(nullptr) {
    }

    void Initialize(rocksdb::DB *db) {
        db_ = db;

        std::string txStr;
        rocksdb::Slice txSlice(rocksdb::EncodeU64Ts(-1, &txStr));
        rocksdb::ReadOptions options;
        options.timestamp = &txSlice;

        std::string strState;
        auto status = db->Get(options, "tx_mgr:state", &strState);
        if (status.IsNotFound()) {
            SaveInProgress(0);
            return;
        }
        assert(status.ok());

        storage::TransactionManagerState state;
        state.ParseFromString(strState);
        lastWriteTxId = state.lasttxid();
        invalid.clear();
        auto inv = state.invalid();
        for (unsigned long &it: inv) {
            invalid.insert(it);
        }

        auto com = state.committed();
        for (unsigned long &it: com) {
            std::stringstream ss;
            ss << "tx:" << it;
            std::string value;
            auto status = db->Get(options, ss.str(), &value);
            assert(status.ok());

            storage::Transaction tx;
            tx.ParseFromString(value);
            std::set<VertexId> touched;
            auto vertex_keys = tx.touched();
            for (auto &vertex_key: vertex_keys) {
                VertexId vid;
                encoding(vertex_key).get_vertex(vid);
                touched.insert(vid);
            }
            recent.insert(std::pair(tx.txid(), touched));
        }

        ulong last_log_idx;
        auto txWithStarts = GetInProgress(lastWriteTxId, last_log_idx);
        inProgress.clear();
        for (auto &txWithStart: txWithStarts) {
            uint64_t txId = txWithStart.first;
            inProgress.insert(std::pair(txId, LoadTransaction(txId)));
        }
    }

    std::shared_ptr<Transaction> OpenForRead(uint64_t txId = 0) const {
        std::shared_lock db_lock(db_mutex);
        std::shared_lock lock(invalid_mutex);
        if (txId == 0) {
            std::map<uint64_t, uint64_t> progress;
            for (const auto &inProgres: inProgress) {
                progress.insert(std::pair(inProgres.first, inProgres.second->GetLogWindowStart()));
            }
            return std::make_shared<Transaction>(*this, lastWriteTxId, lastRaftLogIdx, progress, true);
        } else {
            ulong raft_log_idx;
            std::map<uint64_t, uint64_t> progress = GetInProgress(txId, raft_log_idx);
            return std::make_shared<Transaction>(*this, txId, raft_log_idx, progress, true);
        }
    }

    std::shared_ptr<Transaction> OpenForWrite(ulong raft_log_idx) {
        std::unique_lock db_lock(db_mutex);

        std::map<uint64_t, uint64_t> mapping;
        for (const auto &inProgres: inProgress) {
            mapping.insert(std::pair(inProgres.first, inProgres.second->GetLogWindowStart()));
        }
        // want to be able to read anything that's been written
        std::shared_ptr<Transaction> tx = std::make_shared<Transaction>(*this, lastWriteTxId, lastRaftLogIdx, mapping, false);
        AddInProgress(tx, raft_log_idx);
        WriteState(raft_log_idx);
        return tx;
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

    bool IsInvalid(uint64_t txId) const override {
        std::shared_lock lock(invalid_mutex);
        return invalid.find(txId) != invalid.end();
    }

    void Commit(const Transaction &txn, ulong raft_log_idx) {
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
                    const std::set<VertexId> ref = recent.find(inPTxId)->second;
                    if (Conflict(ref, txn.touched)) {
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
            RemoveInProgress(txId, raft_log_idx);
            WriteState(raft_log_idx);
        } catch (TransactionException te) {
            {
                std::unique_lock lock(invalid_mutex);
                invalid.insert(txId);
            }
            RemoveInProgress(txId, raft_log_idx);
            WriteState(raft_log_idx);
            throw;
        }
    }

    void Rollback(uint64_t txId, ulong raft_log_idx) {
        std::unique_lock db_lock(db_mutex);
        {
            std::unique_lock lock(invalid_mutex);
            invalid.insert(txId);
        }
        RemoveInProgress(txId, raft_log_idx);
        WriteState(raft_log_idx);
    }

    void SaveTransaction(const Transaction &txn, ulong raft_log_idx) {
        storage::Transaction state;
        state.set_txid(txn.GetTxId());
        for (const auto &vid: txn.touched) {
            std::string *vertexKey = state.add_touched();
            *vertexKey = encoding().put_vertex(vid).ToString();
        }

        std::string tsStr;
        rocksdb::Slice ts(rocksdb::EncodeU64Ts(raft_log_idx, &tsStr));

        std::stringstream ss;
        ss << "tx:" << txn.GetTxId();
        auto status = db_->Put(rocksdb::WriteOptions(), ss.str(), ts, state.SerializeAsString());
        assert(status.ok());
    }

    /**
     * Remove from memory all committed transactions from before expireTx.
     * This will abort any transactions that started before expireTx.
     */
    void Advance(uint64_t expireTx) {
    }

private:
    std::map<uint64_t, uint64_t> GetInProgress(uint64_t txId, ulong &raft_log_idx) const {
        std::string tsStr;
        rocksdb::Slice ts(rocksdb::EncodeU64Ts(-1, &tsStr));

        rocksdb::ReadOptions options;
        options.timestamp = &ts;

        std::stringstream ss;
        ss << "tx_mgr:in_progress:" << txId;

        std::string value;
        auto status = db_->Get(options, ss.str(), &value);
        assert(status.ok());

        storage::InProgress inp;
        inp.ParseFromString(value);

        raft_log_idx = inp.logidx();

        std::map<uint64_t, uint64_t> result;
        for (const auto &it: inp.txns()) {
            result.insert(std::pair(it.txid(), it.logidx()));
        }
        return result;
    }

    void AddInProgress(const std::shared_ptr<Transaction> &txn, ulong raft_log_idx) {
        inProgress.insert(std::pair(txn->GetTxId(), txn));
        SaveInProgress(raft_log_idx);
    }

    void RemoveInProgress(uint64_t txId, ulong raft_log_idx) {
        inProgress.erase(txId);
        SaveInProgress(raft_log_idx);
    }

    void SaveInProgress(uint64_t raft_log_idx) {
        lastRaftLogIdx = raft_log_idx;
        ulong txId = ++lastWriteTxId;
        std::stringstream ss;
        ss << "tx_mgr:in_progress:" << txId;

        std::string tsStr;
        rocksdb::Slice ts(rocksdb::EncodeU64Ts(raft_log_idx, &tsStr));

        storage::InProgress value;
        value.set_logidx(raft_log_idx);
        for (auto &inProgres: inProgress) {
            auto writeTx = value.add_txns();
            writeTx->set_txid(inProgres.first);
            writeTx->set_logidx(inProgres.second->GetLogWindowStart());
        }
        auto status = db_->Put(rocksdb::WriteOptions(), ss.str(), ts, value.SerializeAsString());
        assert(status.ok());
    }

    void WriteState(ulong raft_log_idx) const {
        storage::TransactionManagerState state;
        state.set_lasttxid(lastWriteTxId);
        {
            std::shared_lock lock(invalid_mutex);
            for (unsigned long it: invalid) {
                state.add_invalid(it);
            }
        }
        for (const auto &it: recent) {
            state.add_committed(it.first);
        }
        std::string logTs;
        rocksdb::Slice logSlice(rocksdb::EncodeU64Ts(raft_log_idx, &logTs));

        auto status = db_->Put(rocksdb::WriteOptions(), "tx_mgr:state", logSlice, state.SerializeAsString());
        assert(status.ok());
    }

    std::shared_ptr<Transaction> LoadTransaction(uint64_t txId) {
        ulong raft_log_idx;
        std::map<uint64_t, uint64_t> mapping = GetInProgress(txId, raft_log_idx);

        std::string tsStr;
        rocksdb::Slice ts(rocksdb::EncodeU64Ts(-1, &tsStr));
        rocksdb::ReadOptions readOptions;
        readOptions.timestamp = &ts;

        std::stringstream ss;
        ss << "tx:" << txId;

        std::string value;
        auto status = db_->Get(readOptions, ss.str(), &value);
        assert(status.ok());
        storage::Transaction state;
        state.ParseFromString(value);
        std::shared_ptr<Transaction> txn = std::make_shared<Transaction>(
            *this,
            txId,
            raft_log_idx,
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

    uint64_t lastWriteTxId = 1;
    uint64_t lastRaftLogIdx;
    std::map<uint64_t, std::set<VertexId> > recent;
    std::map<uint64_t, std::shared_ptr<Transaction> > inProgress;

    // all rolled back and invalidated transactions - this set can be pruned
    // by a scan (GC) over all data; any updates from invalidated txns are removed
    // on a compaction.
    std::set<uint64_t> invalid;
};
