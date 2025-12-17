#pragma once

#include <cassert>
#include <memory>
#include <ranges>

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

class ItemStateContextGuard {
public:
    explicit ItemStateContextGuard(const std::shared_ptr<ItemStateContext> &txn) {
        prev = ItemStateContext::Get(nullptr);
        ItemStateContext::Set(txn);
    }

    ~ItemStateContextGuard() {
        ItemStateContext::Set(prev);
    }

private:
    std::shared_ptr<ItemStateContext> prev;
};


/**
 * Merge updates to a key, filtering out those created by invalid transactions.
 * Should only be used on time ranges that exclude the "active window" as in-progress
 * transactions are still undecided.
 */
class ItemStateOperator : public rocksdb::MergeOperator {
public:
    ItemStateOperator(const std::shared_ptr<TransactionManager> &txMgr) : txMgr(txMgr) {
    }

    const char *Name() const override { return "TxMergeOperator"; }

    bool FullMergeV2(
        const MergeOperationInput &merge_in, MergeOperationOutput *merge_out) const override {
        std::shared_ptr<ItemStateContext> context = ItemStateContext::Get(txMgr);

        storage::ItemState state;
        storage::ItemAction action(storage::DELETE);

        bool found = false;
        auto operands = merge_in.operand_list;
        state.set_numoperands(operands.size());
        for (auto &operand: std::ranges::reverse_view(operands)) {
            storage::ItemOperation op;
            op.ParseFromString(operand.ToString());
            if (context->IsExcluded(op.txid())) {
                switch (op.action()) {
                    case storage::DELETE:
                        state.add_deletesinprogress(op.txid());
                        break;
                    case storage::PUT:
                        state.add_putsinprogress(op.txid());
                        break;
                    default:
                        break;
                }
                continue;
            }
            if (!found && !txMgr->IsInvalid(op.txid())) {
                action = op.action();
                found = true;
            }
        }

        if (merge_in.existing_value) {
            storage::ItemState existingState;
            auto strValue = merge_in.existing_value->ToString();
            existingState.ParseFromString(strValue);

            // one of the in-progress transactions may have committed since the snapshot was created
            for (const auto &txId: existingState.putsinprogress()) {
                if (context->IsExcluded(txId)) {
                    state.add_putsinprogress(txId);
                    continue;
                }
                if (!found && !txMgr->IsInvalid(txId)) {
                    found = true;
                    action = storage::PUT;
                }
            }
            for (const auto &txId: existingState.deletesinprogress()) {
                if (context->IsExcluded(txId)) {
                    state.add_deletesinprogress(txId);
                    continue;
                }
                if (!found && !txMgr->IsInvalid(txId)) {
                    found = true;
                    action = storage::DELETE;
                }
            }
            if (!found) {
                found = true;
                action = existingState.action();
            }
        }


        state.set_action(action);
        merge_out->new_value = state.SerializeAsString();
        return true;
    }

private:
    std::shared_ptr<TransactionManager> txMgr;
};

class ItemStateFilter : public rocksdb::CompactionFilter {
public:
    explicit ItemStateFilter(const std::shared_ptr<TransactionManager> &txMgr) : txMgr(txMgr) {
    }

    const char *Name() const override { return "TxCompactionFilter"; }

    bool Filter(
        int level,
        const rocksdb::Slice &key,
        const rocksdb::Slice &existing_value,
        std::string *new_value,
        bool *value_changed) const override {
        storage::ItemState state;
        state.ParseFromString(existing_value.ToString());
        return state.action() == storage::DELETE;
    }

    bool FilterMergeOperand(int /*level*/, const rocksdb::Slice & /*key*/,
                            const rocksdb::Slice &operand) const override {
        storage::ItemOperation op;
        op.ParseFromString(operand.ToString());
        return txMgr->IsInvalid(op.txid());
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
        cfOptions.merge_operator = std::make_shared<ItemStateOperator>(txMgr);
        compactionFilter = std::make_shared<ItemStateFilter>(txMgr);
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

    const std::shared_ptr<TransactionManager> &GetTransactionManager() const {
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
