#pragma once

#include <algorithm>
#include <cinttypes>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <utility>
#include <shared_mutex>

#include "model.h"
#include "storage.pb.h"

enum TransactionException { TX_IS_READONLY, TX_CONFLICT, TX_INVALIDATED, TX_NOT_IN_PROGRESS };

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

    void Touch(const VertexId &key) {
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
    TransactionManager(): db_(nullptr) {}

    std::shared_ptr<Transaction> OpenForRead(uint64_t txId = -1) const {
        std::shared_lock lock(mutex);
        uint64_t tx = txId == -1 ? lastWriteTxId + 1 : txId;
        return std::make_shared<Transaction>(tx, inProgress, invalid, true);
    }

    bool IsInvalid(uint64_t txId) const {
        std::shared_lock lock(mutex);
        return invalid.find(txId) != invalid.end();
    }

    void Initialize(rocksdb::DB* db) {
        db_ = db;
        rocksdb::ReadOptions options;
        std::string strState;
        auto status = db->Get(options, "tx_manager_state", &strState);
        if (status.IsNotFound()) {
            return;
        }
        assert(status.ok());

        storage::TransactionManagerState state;
        state.ParseFromString(strState);
        lastWriteTxId = state.lasttxid();
        auto inprogress = state.inprogress();
        for (auto it = inprogress.begin(); it != inprogress.end(); it++) {
            inProgress.insert(*it);
        }
        auto inv = state.invalid();
        for (auto it = inv.begin(); it != inv.end(); it++) {
            invalid.insert(*it);
        }

        auto com = state.committed();
        for (auto it = com.begin(); it != com.end(); it++) {
            std::stringstream ss;
            ss << "tx:" << *it;
            std::string value;
            auto status = db->Get(options, ss.str(), &value);
            assert(status.ok());

            storage::Transaction tx;
            tx.ParseFromString(value);
            std::set<VertexId> touched;
            auto vertex_keys = tx.touched();
            for (auto key = vertex_keys.begin(); key != vertex_keys.end(); key++) {
                touched.insert({key->type(), key->id()});
            }
            recent.insert(std::pair(tx.txid(), touched));
        }
    }

    std::shared_ptr<Transaction> Open() {
        std::unique_lock lock(mutex);
        lastWriteTxId += 2;
        uint64_t txId = lastWriteTxId;
        inProgress.insert(txId);
        WriteState();
        return std::make_shared<Transaction>(txId, inProgress, invalid, false);
    }

    void Commit(const Transaction &txn) {
        std::unique_lock lock(mutex);

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
            WriteState();
            throw;
        }

        // hurray!  No conflicts detected
        recent.insert(std::pair(txId, txn.touched));
        inProgress.erase(txId);

        WriteTransaction(txn);
        WriteState();
    }

    void Rollback(uint64_t txId) {
        std::unique_lock lock(mutex);

        inProgress.erase(txId);
        invalid.insert(txId);

        WriteState();
    }

    /**
     * Remove from memory all committed transactions from before expireTx.
     * This will abort any transactions that started before expireTx.
     */
    void Advance(uint64_t expireTx) {
        std::unique_lock lock(mutex);

        rocksdb::WriteOptions writeOptions;
        for (auto it = recent.begin(); it != recent.end();) {
            uint64_t txId = it->first;
            if (txId < expireTx) {
                std::stringstream ss;
                ss << "tx:" << txId;
                auto status = db_->Delete(writeOptions, ss.str());
                assert(status.ok());
                it = recent.erase(it);
            } else {
                ++it;
            }
        }

        WriteState();
    }

private:
    void WriteTransaction(const Transaction &txn) {
        storage::Transaction state;
        state.set_txid(txn.GetTxId());
        for (auto it = txn.touched.begin(); it != txn.touched.end(); it++) {
            const VertexId& vid = *it;
            storage::VertexKey* vertexKey = state.add_touched();
            vertexKey->set_type(vid.type);
            vertexKey->set_id(vid.id);
        }

        std::stringstream ss;
        ss << "tx:" << txn.GetTxId();
        auto status = db_->Put(rocksdb::WriteOptions(), ss.str(), state.SerializeAsString());
        assert(status.ok());
    }

    void WriteState() const {
        storage::TransactionManagerState state;
        state.set_lasttxid(lastWriteTxId);
        for (auto it = inProgress.begin(); it != inProgress.end(); it++) {
            state.add_inprogress(*it);
        }
        for (auto it = invalid.begin(); it != invalid.end(); it++) {
            state.add_invalid(*it);
        }
        for (auto it = recent.begin(); it != recent.end(); it++) {
            state.add_committed(it->first);
        }
        auto status = db_->Put(rocksdb::WriteOptions(), "tx_manager_state", state.SerializeAsString());
        assert(status.ok());
    }

    bool Conflict(const std::set<VertexId> &a, const std::set<VertexId> &b) {
        std::set<VertexId> out;
        std::set_intersection(
            a.begin(), a.end(), b.begin(), b.end(), std::inserter(out, out.begin()));
        return out.begin() != out.end();
    }

    mutable std::shared_mutex mutex;

    rocksdb::DB * db_;

    uint64_t lastWriteTxId = 0;
    std::map<uint64_t, std::set<VertexId> > recent;
    std::set<uint64_t> inProgress;

    // all rolled back and invalidated transactions - this set can be pruned
    // by a scan (GC) over all data; any updates from invalidated txns are removed
    // on a compaction.
    std::set<uint64_t> invalid;
};
