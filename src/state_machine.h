#pragma once


#include "api.grpc.pb.h"
#include "graph.h"
#include "in_memory_state_mgr.hxx"
#include "libnuraft/nuraft.hxx"
#include "libnuraft/state_machine.hxx"

#define SNAPSHOT_BUFFER_SIZE 16384

using namespace nuraft;

template<typename T>
void ReadBuffer(T &target, buffer &buffer) {
    buffer_serializer bs(buffer);
    size_t size = bs.get_u32();
    target.ParseFromArray(bs.get_raw(size), size);
}

template<typename T>
ptr<buffer> WriteBuffer(T &target) {
    size_t size = target.ByteSizeLong();
    ptr<buffer> ret = buffer::alloc(size + sizeof(uint32_t));
    buffer_serializer bs(ret);
    bs.put_u32(size);
    target.SerializeToArray(bs.get_raw(size), size);
    return ret;
}

class StateMachine : public state_machine {
public:
    explicit StateMachine(const ptr<Graph> &graph) : graph(graph) {
    }

    ptr<buffer> commit(const ulong log_idx, buffer &data) override {
        std::cout << "committing status " << log_idx << std::endl;

        // write a copy to the raft_log
        {
            size_t pos = data.pos();

            rocksdb::WriteOptions writeOptions;
            std::string txStr;
            rocksdb::Slice txSlice(rocksdb::EncodeU64Ts(log_idx, &txStr));
            size_t size = data.get_int();
            rocksdb::Slice logEntry(reinterpret_cast<const char *>(data.get_raw(size)), size);
            auto status = graph->GetStorage().db->Put(writeOptions, graph->GetStorage().raft_log,
                                                      "log", txSlice, logEntry);
            assert(status.ok());

            // reset buffer
            data.pos(pos);
        }

        std::cout << "executing " << log_idx << std::endl;
        api::RaftLogEntry entry;
        ReadBuffer(entry, data);
        if (entry.has_open()) {
            const api::OpenRequest &request = entry.open();
            ptr<WriteTransaction> txn = OpenWriteTransaction(request, log_idx);
            api::Transaction response;
            response.set_type(api::WRITE);
            response.set_txid(txn->GetTxId());
            return WriteBuffer(response);
        }
        api::TransactionResponse response;
        try {
            if (entry.has_commit()) {
                Commit(entry.commit(), log_idx);
            } else if (entry.has_rollback()) {
                Rollback(entry.rollback(), log_idx);
            } else if (entry.has_addvertex()) {
                AddVertex(entry.addvertex(), log_idx);
            } else if (entry.has_removevertex()) {
                RemoveVertex(entry.removevertex(), log_idx);
            } else if (entry.has_addlabel()) {
                AddLabel(entry.addlabel(), log_idx);
            } else if (entry.has_removelabel()) {
                RemoveLabel(entry.removelabel(), log_idx);
            } else if (entry.has_addedge()) {
                AddEdge(entry.addedge(), log_idx);
            } else if (entry.has_removeedge()) {
                RemoveEdge(entry.removeedge(), log_idx);
            }
            response.set_status(api::OK);
        } catch (TransactionException te) {
            switch (te) {
                case TX_IS_READONLY:
                    response.set_status(api::IS_READONLY);
                    break;
                case TX_NOT_IN_PROGRESS:
                    response.set_status(api::NOT_IN_PROGRESS);
                    break;
                case TX_CONFLICT:
                    response.set_status(api::CONFLICT);
                    break;
                case TX_INVALIDATED:
                    response.set_status(api::INVALIDATED);
                    break;
            }
        }
        return WriteBuffer(response);
    }

    ptr<WriteTransaction> OpenWriteTransaction(const api::OpenRequest &request, ulong raft_log_idx) {
        ptr<WriteTransaction> txn(graph->OpenForWrite(raft_log_idx));
        return txn;
    }

    void Commit(const api::Transaction &request, ulong raft_log_idx) {
        uint64_t txId = request.txid();
        Tx(txId)->Commit(raft_log_idx);
    }

    void Rollback(const api::Transaction &request, ulong raft_log_idx) {
        uint64_t txId = request.txid();
        Tx(txId)->Rollback(raft_log_idx);
    }

    void AddVertex(const api::AddVertexRequest &request, ulong raft_log_idx) {
        Tx(request.txid())->AddVertex(
            {request.vertexid().type(), request.vertexid().id()}, raft_log_idx);
    }

    void RemoveVertex(const api::RemoveVertexRequest &request, ulong raft_log_idx) {
        Tx(request.txid())->RemoveVertex(
            {request.vertexid().type(), request.vertexid().id()}, raft_log_idx);
    }

    void AddLabel(const api::AddLabelRequest &request, ulong raft_log_idx) {
        Tx(request.txid())->AddLabel(
            {request.vertexid().type(), request.vertexid().id()}, request.label(), raft_log_idx);
    }

    void RemoveLabel(const api::RemoveLabelRequest &request, ulong raft_log_idx) {
        Tx(request.txid())->RemoveLabel(
            {request.vertexid().type(), request.vertexid().id()}, request.label(), raft_log_idx);
    }

    void AddEdge(const api::AddEdgeRequest &request, ulong raft_log_idx) {
        Tx(request.txid())->AddEdge(
            request.label(),
            {request.fromvertexid().type(), request.fromvertexid().id()},
            {request.tovertexid().type(), request.tovertexid().id()},
            raft_log_idx
        );
    }

    void RemoveEdge(const api::RemoveEdgeRequest &request, ulong raft_log_idx) {
        Tx(request.txid())->RemoveEdge(
            request.label(),
            {request.fromvertexid().type(), request.fromvertexid().id()},
            {request.tovertexid().type(), request.tovertexid().id()},
            raft_log_idx
        );
    }

    bool apply_snapshot(snapshot &s) {
        return true;
    }

    ptr<snapshot> last_snapshot() {
        return nullptr;
    }

    ulong last_commit_index() {
        rocksdb::ReadOptions readOptions;
        std::string txStr;
        rocksdb::Slice txSlice(rocksdb::EncodeU64Ts(-1, &txStr));
        readOptions.timestamp = &txSlice;

        std::string value;
        auto status = graph->GetStorage().db->Get(readOptions, "state_machine", &value);
        std::cout << "last_commit status " << status.ToString() << std::endl;
        if (status.IsNotFound()) {
            return 0;
        }
        assert(status.ok());
        return std::stoul(value);
    }

    void create_snapshot(snapshot &s,
                         async_result<bool>::handler_type &when_done) {
    }

    int read_logical_snp_obj(snapshot &s, void *&user_snp_ctx, ulong obj_id, ptr<buffer> &data_out,
                             bool &is_last_obj) override {
        if (obj_id == 0) {
            return 0;
        }
        if (user_snp_ctx == nullptr) {
            user_snp_ctx = new SnapshotContext(graph, obj_id, last_commit_index());
        }
        SnapshotContext *ctx = static_cast<SnapshotContext *>(user_snp_ctx);
        if (ctx->Read(data_out)) {
            // continue with a new snapshot if handling of previous one took too long
            // for recent raft log to be applicable.
            if (last_commit_index() - ctx->GetEndLogIdx() > 10000) {
                std::cout << "CONTINUING SNAPSHOT AT " << ctx->GetEndLogIdx() << " TO " << last_commit_index() <<
                        std::endl;
                user_snp_ctx = new SnapshotContext(graph, ctx->GetEndLogIdx(), last_commit_index());
                delete ctx;
            } else {
                is_last_obj = true;
            }
        }
        return 0;
    }

    void save_logical_snp_obj(snapshot &s, ulong &obj_id, buffer &data, bool is_first_obj, bool is_last_obj) override {
        if (obj_id == 0) {
            obj_id = last_commit_index();
            return;
        }
        WriteSnapshotBuffer(data);
        obj_id++;
    }

    void free_user_snp_ctx(void *&user_snp_ctx) override {
        delete static_cast<SnapshotContext *>(user_snp_ctx);
    }

private:
    // TODO: Read from column families when log entries have expired
    class SnapshotContext {
    public:
        SnapshotContext(
            const ptr<Graph> &graph,
            uint64_t start_log_idx,
            uint64_t end_log_idx
        ) : graph(graph),
            end_log_idx(end_log_idx),
            end_log_slice(rocksdb::EncodeU64Ts(end_log_idx, &end_log_str)),
            start_log_slice(rocksdb::EncodeU64Ts(start_log_idx, &start_log_str)) {
            rocksdb::ReadOptions readOptions;
            readOptions.timestamp = &end_log_slice;
            readOptions.iter_start_ts = &start_log_slice;

            const Storage &storage = graph->GetStorage();
            iter.reset(storage.db->NewIterator(readOptions, storage.raft_log));
            iter->SeekToLast();
        }

        bool Read(ptr<buffer> &data_out) {
            data_out = buffer::expand(*data_out, SNAPSHOT_BUFFER_SIZE);
            return ReadFromIterator(iter, data_out);
        }

        uint64_t GetEndLogIdx() const {
            return end_log_idx;
        }

    private:
        ptr<rocksdb::Iterator> iter;

        ptr<Graph> graph;
        uint64_t end_log_idx;
        std::string end_log_str;
        rocksdb::Slice end_log_slice;
        std::string start_log_str;
        rocksdb::Slice start_log_slice;
    };

    static bool ReadFromIterator(const ptr<rocksdb::Iterator> &iter, const ptr<buffer> &data_out) {
        while (iter->Valid()) {
            auto tsSlice = iter->timestamp();
            ulong ts;
            rocksdb::DecodeU64Ts(tsSlice, &ts);
            auto value = iter->value();

            // check for 2 bytes extra for the "is_last" flag
            // 1 to write this entry, 1 to write the last "end" marker
            uint size = 2 + sizeof(ulong) + sizeof(uint32_t) + value.size();
            nuraft::byte isLast = false;
            if (data_out->size() - data_out->pos() < size) {
                isLast = true;
                data_out->put(isLast);
                return false;
            }
            data_out->put(isLast);
            data_out->put(ts);
            data_out->put(value.data(), value.size());
            iter->Prev();
        }
        return true;
    }

    void WriteSnapshotBuffer(buffer &buf) {
        while (buf.pos() < buf.size()) {
            nuraft::byte isLast = buf.get_byte();
            if (isLast == true) {
                return;
            }
            ulong ts = buf.get_ulong();
            commit(ts, buf);
        }
    }

    ptr<WriteTransaction> Tx(uint64_t txId) {
        return cs_new<WriteTransaction>(graph->GetStorage(), graph->txMgr, graph->txMgr->GetWriteTxn(txId));
    }

    ptr<Graph> graph;
};
