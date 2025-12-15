#pragma once

#include <cassert>
#include <memory>
#include <ranges>

#include "encoding.h"
#include "storage.pb.h"
#include "transaction.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"

class TransactionOperator : public rocksdb::MergeOperator {
public:
    const char *Name() const override { return "TransactionOperator"; }

    bool FullMergeV2(const MergeOperationInput &merge_in, MergeOperationOutput *merge_out) const override {
        storage::Transaction transaction;
        if (merge_in.existing_value) {
            auto value = merge_in.existing_value->ToString();
            transaction.ParseFromString(value);
        }
        std::set<std::string> ids;
        for (const auto &it: transaction.touched()) {
            ids.insert(it);
        }
        auto operands = merge_in.operand_list;
        for (auto &op: std::ranges::reverse_view(operands)) {
            storage::TransactionOperation top;
            top.ParseFromString(op.ToString());
            if (!ids.contains(top.touched())) {
                transaction.add_touched(op.ToString());
            }
        }
        merge_out->new_value = transaction.SerializeAsString();
        return true;
    }
};

inline thread_local std::shared_ptr<Transaction> mergeThreadContext;

class MergeGuard {
public:
    MergeGuard(const std::shared_ptr<Transaction> &txn) : txn(txn) {
        mergeThreadContext = txn;
    }

    ~MergeGuard() {
        mergeThreadContext = nullptr;
    }

private:
    std::shared_ptr<Transaction> txn;
};


/**
 * Merge updates to a key, filtering out those created by invalid transactions.
 * Should only be used on time ranges that exclude the "active window" as in-progress
 * transactions are still undecided.
 */
class MergeValueOperator : public rocksdb::MergeOperator {
public:
    MergeValueOperator(const std::shared_ptr<TransactionManager> &txMgr) : txMgr(txMgr) {
    }

    const char *Name() const override { return "TxMergeOperator"; }

    bool FullMergeV2(
        const MergeOperationInput &merge_in, MergeOperationOutput *merge_out) const override {
        MergeValue value;
        if (merge_in.existing_value) {
            merge_out->new_value = merge_in.existing_value->ToString(false);
        } else {
            value.action = DELETE;
            value.txId = 0;
            merge_out->new_value = encoding().put_merge(value).ToString();
        }

        auto operands = merge_in.operand_list;
        for (auto &operand: std::ranges::reverse_view(operands)) {
            encoding(operand).get_merge(value);
            if (mergeThreadContext != nullptr && mergeThreadContext->IsExcluded(value.txId)) {
                continue;
            }
            if (!txMgr->IsInvalid(value.txId)) {
                merge_out->new_value = operand.ToString();
                break;
            }
        }
        return true;
    }

private:
    std::shared_ptr<TransactionManager> txMgr;
};

class MergeValueFilter : public rocksdb::CompactionFilter {
public:
    MergeValueFilter(const std::shared_ptr<TransactionManager> &txMgr) : txMgr(txMgr) {
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

class Storage {
public:
    explicit Storage(const std::string &path) : txMgr(std::make_shared<TransactionManager>()) {
        rocksdb::Options options;
        options.create_if_missing = true;
        options.create_missing_column_families = true;
        options.comparator = rocksdb::BytewiseComparatorWithU64Ts();

        rocksdb::ColumnFamilyOptions defaultOptions(options);

        rocksdb::ColumnFamilyOptions transactionOptions(options);
        transactionOptions.merge_operator = std::make_shared<TransactionOperator>();

        rocksdb::ColumnFamilyOptions cfOptions(options);
        cfOptions.merge_operator = std::make_shared<MergeValueOperator>(txMgr);
        compactionFilter = std::make_shared<MergeValueFilter>(txMgr);
        cfOptions.compaction_filter = compactionFilter.get();

        std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
        descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, defaultOptions);
        descriptors.emplace_back("raft_log", defaultOptions);
        descriptors.emplace_back("transactions", transactionOptions);
        descriptors.emplace_back("index", cfOptions);
        descriptors.emplace_back("vertices", cfOptions);
        descriptors.emplace_back("edges", cfOptions);
        descriptors.emplace_back("labels", cfOptions);

        std::vector<rocksdb::ColumnFamilyHandle *> handles;
        rocksdb::Status status =
                rocksdb::DB::Open(options, path, descriptors, &handles, &db);
        assert(status.ok());

        _default = handles[0];
        raft_log = handles[1];
        transactions = handles[2];
        index = handles[3];
        vertices = handles[4];
        edges = handles[5];
        labels = handles[6];

        txMgr->Initialize(db, transactions);
    }

    ~Storage() {
        db->DestroyColumnFamilyHandle(labels);
        db->DestroyColumnFamilyHandle(edges);
        db->DestroyColumnFamilyHandle(vertices);
        db->DestroyColumnFamilyHandle(index);
        db->DestroyColumnFamilyHandle(transactions);
        db->DestroyColumnFamilyHandle(raft_log);
        db->DestroyColumnFamilyHandle(_default);
        db->Close();
    }

    const std::shared_ptr<TransactionManager>& GetTransactionManager() const {
        return txMgr;
    }

    rocksdb::DB *db;
    rocksdb::ColumnFamilyHandle *_default;
    rocksdb::ColumnFamilyHandle *raft_log;
    rocksdb::ColumnFamilyHandle *transactions;
    rocksdb::ColumnFamilyHandle *index;
    rocksdb::ColumnFamilyHandle *vertices;
    rocksdb::ColumnFamilyHandle *edges;
    rocksdb::ColumnFamilyHandle *labels;

    std::shared_ptr<TransactionManager> txMgr;
    std::shared_ptr<rocksdb::CompactionFilter> compactionFilter;
};
