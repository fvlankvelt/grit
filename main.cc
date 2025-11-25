#include <cassert>
#include <iostream>
#include <sstream>
#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"

class YeOper : public rocksdb::MergeOperator {
   public:
    explicit YeOper() : count(0) {}

    const char* Name() const override { return "YeOperator"; }

    bool FullMergeV2(
        const MergeOperationInput& merge_in, MergeOperationOutput* merge_out) const override {
        std::cout << "MERGING: " << merge_in.operand_list.size() << std::endl;
        const rocksdb::Slice * existing = merge_in.existing_value;

        std::stringstream ss;
        if (existing != nullptr) {
            ss << existing->data();
            ss << ":";
        }
        for (auto iter = merge_in.operand_list.begin(); iter != merge_in.operand_list.end(); iter++) {
            ss << (*iter).data();
            ss << ":";
        }
        merge_out->new_value = ss.str();
        std::cout << "OUT: " << merge_out->new_value << std::endl;
        return true;
    }

   private:
    int count;
};

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.merge_operator.reset(new YeOper());
    options.comparator = rocksdb::BytewiseComparatorWithU64Ts();
    options.compaction_style = rocksdb::kCompactionStyleLevel;
    rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/testdb", &db);
    assert(status.ok());

    std::string ts_0;
    std::string ts_2;
    rocksdb::WriteOptions wo;
    db->Put(wo, db->DefaultColumnFamily(), rocksdb::Slice("key"), rocksdb::EncodeU64Ts(0, &ts_0), rocksdb::Slice(""));
    db->Merge(wo, db->DefaultColumnFamily(), rocksdb::Slice("key"), rocksdb::EncodeU64Ts(0, &ts_0), rocksdb::Slice("a"));
    db->Merge(wo, db->DefaultColumnFamily(), rocksdb::Slice("key"), rocksdb::EncodeU64Ts(2, &ts_2), rocksdb::Slice("b"));

    {
    rocksdb::ReadOptions readOptions;
    int max_n_operands = 100;
    std::vector<rocksdb::PinnableSlice> values(max_n_operands);
    rocksdb::GetMergeOperandsOptions merge_operands_info;
    merge_operands_info.expected_max_number_of_operands = max_n_operands;
    merge_operands_info.continue_cb = [&](rocksdb::Slice value) {
      std::cout << "MORE: " << value.data() << std::endl;
      return false;
    };
    int num_operands_found = 0;
    status = db->GetMergeOperands(
        readOptions,
        db->DefaultColumnFamily(),
        rocksdb::Slice("key"),
        values.data(),
        &merge_operands_info,
        &num_operands_found);
    }

    std::string ts_1;
    rocksdb::Slice compact_ts(rocksdb::EncodeU64Ts(1, &ts_1));
    rocksdb::CompactRangeOptions cro;
    cro.full_history_ts_low = &compact_ts;
    cro.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
    status = db->CompactRange(cro, nullptr, nullptr);
    assert(status.ok());

    // status = db->Flush(rocksdb::FlushOptions());
    // assert(status.ok());

    // rocksdb::Slice iter_start(ToSlice(0));
    std::string ts_3;
    rocksdb::Slice timestamp(rocksdb::EncodeU64Ts(1, &ts_1));
    rocksdb::ReadOptions readOptions;
    readOptions.timestamp = &timestamp;
    // readOptions.iter_start_ts = &timestamp;
    rocksdb::Iterator * iter = db->NewIterator(readOptions);
    iter->SeekToFirst();
    while (iter->Valid()) {
        // std::cout << "key: " << iter->key().data() << " => " << iter->value().data() << std::endl;
        std::cout << "key: " << iter->key().ToString() << " => " << iter->value().ToString() << std::endl;
        iter->Next();
    }

}
