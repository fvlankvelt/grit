#pragma once

#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <utility>

#include "model.h"

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
