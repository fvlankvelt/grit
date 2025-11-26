#include <cassert>
#include <iostream>
#include <memory>
#include <sstream>

#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.comparator = rocksdb::BytewiseComparatorWithU64Ts();
    options.compaction_style = rocksdb::kCompactionStyleLevel;
    rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/testdb", &db);
    assert(status.ok());

    std::string ts_0;
    std::string ts_2;
    std::string ts_4;
    rocksdb::WriteOptions wo;
    db->Put(
        wo,
        db->DefaultColumnFamily(),
        rocksdb::Slice("key"),
        rocksdb::EncodeU64Ts(0, &ts_0),
        rocksdb::Slice("a"));
    db->Put(
        wo,
        db->DefaultColumnFamily(),
        rocksdb::Slice("key"),
        rocksdb::EncodeU64Ts(2, &ts_2),
        rocksdb::Slice("b"));
    db->Delete(
        wo, db->DefaultColumnFamily(), rocksdb::Slice("key"), rocksdb::EncodeU64Ts(4, &ts_4));

    /**
     * Query over Time
     * - iterator with iter_start_ts only returns updates within the time-range
     *   results are from most recent to oldest
     * - other iterator returns the view at the start of the interval
     */
    std::string ts_1;
    std::string ts_5;
    rocksdb::Slice from(rocksdb::EncodeU64Ts(1, &ts_1));
    rocksdb::Slice to(rocksdb::EncodeU64Ts(5, &ts_5));
    {
        rocksdb::ReadOptions readOptions;
        readOptions.timestamp = &to;
        readOptions.iter_start_ts = &from;
        rocksdb::Iterator* iter = db->NewIterator(readOptions);
        iter->SeekToFirst();
        while (iter->Valid()) {
            // std::cout << "key: " << iter->key().data() << " => " << iter->value().data() <<
            // std::endl;
            std::cout << "key: " << iter->key().ToString() << " => " << iter->value().ToString()
                      << std::endl;
            iter->Next();
        }
    }
    {
        rocksdb::ReadOptions readOptions;
        readOptions.timestamp = &from;
        rocksdb::Iterator* iter = db->NewIterator(readOptions);
        iter->SeekToFirst();
        while (iter->Valid()) {
            // std::cout << "key: " << iter->key().data() << " => " << iter->value().data() <<
            // std::endl;
            std::cout << "key: " << iter->key().ToString() << " => " << iter->value().ToString()
                      << std::endl;
            iter->Next();
        }
    }
}
