#pragma once

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include <csignal>

#include "api.grpc.pb.h"
#include "graph.h"
#include "in_memory_state_mgr.hxx"
#include "libnuraft/nuraft.hxx"

#define SNAPSHOT_BUFFER_SIZE 16384

using namespace nuraft;


class Logger : public logger {
public:
    void put_details(int level,
                     const char *source_file,
                     const char *func_name,
                     size_t line_number,
                     const std::string &msg) {
        // INFO logging
        if (level <= 4) {
            std::cout << level << ": " << source_file << " :" << func_name << " (" << line_number << "): " << msg <<
                    std::endl;
        }
    }
};

struct RaftPeer {
    int id;
    std::string address;
};

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
    StateMachine(ptr<Graph> graph) : graph(graph) {
    }

    ptr<buffer> commit(const ulong log_idx, buffer &data) {
        std::cout << "committing status " << log_idx << std::endl;

        {
            rocksdb::WriteOptions writeOptions;
            std::string txStr;
            rocksdb::Slice txSlice(rocksdb::EncodeU64Ts(log_idx, &txStr));
            auto status = graph->GetStorage().db->Put(writeOptions, "state_machine", txSlice, std::to_string(log_idx));
            assert(status.ok());
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

    ptr<ReadTransaction> OpenReadTransaction(const api::OpenRequest &request) {
        return ptr<ReadTransaction>(graph->OpenForRead(request.txid()));
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
            is_last_obj = true;
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
        if (is_last_obj) {
            graph->txMgr->Initialize(graph->GetStorage().db);
        }
    }

    void free_user_snp_ctx(void *&user_snp_ctx) override {
        delete static_cast<SnapshotContext *>(user_snp_ctx);
    }

private:
    enum ColumnFamilyCell {
        NONE = 0,
        VERTICES = 1,
        EDGES = 2,
        LABELS = 3,
        INDEX = 4,
        DEFAULT = 5,
    };

    class SnapshotContext {
    public:
        SnapshotContext(
            const ptr<Graph> &graph,
            uint64_t startTxId,
            uint64_t txId
        ) : graph(graph),
            tx(rocksdb::EncodeU64Ts(txId, &str_ts)),
            start_tx(rocksdb::EncodeU64Ts(startTxId, &str_start_tx)) {
            rocksdb::ReadOptions readOptions;
            readOptions.timestamp = &tx;
            readOptions.iter_start_ts = &start_tx;

            const Storage &storage = graph->GetStorage();
            vertices.reset(storage.db->NewIterator(readOptions, storage.vertices));
            vertices->SeekToLast();
            edges.reset(storage.db->NewIterator(readOptions, storage.edges));
            edges->SeekToLast();
            labels.reset(storage.db->NewIterator(readOptions, storage.labels));
            labels->SeekToLast();
            index.reset(storage.db->NewIterator(readOptions, storage.index));
            index->SeekToLast();
            _default.reset(storage.db->NewIterator(readOptions, storage._default));
            _default->SeekToLast();
        }

        bool Read(ptr<buffer> &data_out) {
            data_out = buffer::expand(*data_out, SNAPSHOT_BUFFER_SIZE);
            return ReadIterator(vertices, data_out, VERTICES)
                   && ReadIterator(edges, data_out, EDGES)
                   && ReadIterator(labels, data_out, LABELS)
                   && ReadIterator(index, data_out, INDEX)
                   && ReadIterator(_default, data_out, DEFAULT);
        }

    private:
        ptr<rocksdb::Iterator> vertices;
        ptr<rocksdb::Iterator> edges;
        ptr<rocksdb::Iterator> labels;
        ptr<rocksdb::Iterator> index;
        ptr<rocksdb::Iterator> _default;

        ptr<Graph> graph;
        std::string str_ts;
        rocksdb::Slice tx;
        std::string str_start_tx;
        rocksdb::Slice start_tx;
    };

    enum ValueType {
        DELETE = 0,
        PUT = 1,
        MERGE = 2
    };

    static bool ReadIterator(ptr<rocksdb::Iterator> iter, ptr<buffer> &data_out, ColumnFamilyCell cell) {
        while (iter->Valid()) {
            auto key = iter->key();
            auto value = iter->value();

            // UNSAFE VALUE TYPE DETERMINATION
            nuraft::byte valueType = key.data()[key.size() + 8 - 1];
            assert(valueType == DELETE || valueType == PUT || valueType == MERGE);

            // check for 2 byte extra for the "continue" flag
            // 1 to write this entry, 1 to write the last "end" marker
            uint size = 3 + key.size() + value.size() + 2 * sizeof(uint32_t);
            if (data_out->size() - data_out->pos() < size) {
                nuraft::byte done = NONE;
                data_out->put(done);
                return false;
            }
            nuraft::byte done = cell;
            data_out->put(done);
            data_out->put(valueType);
            data_out->put(key.data(), key.size());
            data_out->put(value.data(), value.size());
            iter->Prev();
        }
        return true;
    }

    void WriteSnapshotBuffer(buffer &buffer) {
        auto storage = graph->GetStorage();
        do {
            nuraft::byte byte = buffer.get_byte();
            rocksdb::ColumnFamilyHandle *handle;
            switch (byte) {
                case VERTICES:
                    handle = storage.vertices;
                    break;
                case EDGES:
                    handle = storage.edges;
                    break;
                case LABELS:
                    handle = storage.labels;
                    break;
                case INDEX:
                    handle = storage.index;
                    break;
                case DEFAULT:
                    handle = storage._default;
                    break;
                case NONE:
                default:
                    return;
            }

            ValueType vt = static_cast<ValueType>(buffer.get_byte());

            size_t keySize = buffer.get_int();
            rocksdb::Slice keyWithTx(reinterpret_cast<const char *>(buffer.get_bytes(keySize)), keySize);
            rocksdb::Slice key(keyWithTx.data(), keySize - 8);
            rocksdb::Slice tx(keyWithTx.data() + keySize - 8, 8);

            size_t valueSize = buffer.get_int();
            rocksdb::Slice value(reinterpret_cast<const char *>(buffer.get_bytes(valueSize)), valueSize);

            switch (vt) {
                case MERGE:
                    storage.db->Merge(rocksdb::WriteOptions(), handle, key, tx, value);
                    break;
                case PUT:
                    storage.db->Put(rocksdb::WriteOptions(), handle, key, tx, value);
                    break;
                case DELETE: // TODO: Does this really happen?
                    storage.db->Merge(rocksdb::WriteOptions(), handle, key, tx, value);
                    break;
            }
        } while (true);
    }

    ptr<WriteTransaction> Tx(uint64_t txId) {
        return cs_new<WriteTransaction>(graph->GetStorage(), graph->txMgr, graph->txMgr->GetWriteTxn(txId));
    }

    ptr<Graph> graph;
};

class Service : public api::GritApi::Service {
public:
    Service(int id, int raftPort, const ptr<Graph> &graph, std::vector<RaftPeer> peers) : graph(graph) {
        ptr<logger> my_logger = cs_new<Logger>();
        ptr<state_mgr> my_state_manager = cs_new<inmem_state_mgr>(id, "localhost:" +
                                                                      std::to_string(raftPort));
        auto cluster_config = my_state_manager->load_config();
        for (auto & peer : peers) {
            ptr<srv_config> peer_conf = cs_new<srv_config>(peer.id, peer.address);
            cluster_config->get_servers().push_back(peer_conf);
        }
        asio_service::options asio_opt;
        raft_params params;
        params.custom_commit_quorum_size_ = peers.size() / 2 + 1;
        params.return_method_ = raft_params::blocking;
        params.auto_forwarding_ = true;

        my_state_machine_ = cs_new<StateMachine>(graph);
        launcher_ = cs_new<raft_launcher>();
        server_ = launcher_->init(my_state_machine_, my_state_manager, my_logger, raftPort,
                                  asio_opt, params);

        while (!server_->is_initialized()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    ~Service() override {
        launcher_->shutdown();
    }

    grpc::Status OpenTransaction(
        grpc::ServerContext *context,
        const api::OpenRequest *request,
        api::Transaction *response) override {
        if (request->type() == api::READ) {
            ptr txn(my_state_machine_->OpenReadTransaction(*request));
            response->set_type(api::READ);
            response->set_txid(txn->GetTxId());
            return grpc::Status::OK;
        }

        api::RaftLogEntry entry;
        entry.mutable_open()->CopyFrom(*request);
        ptr<buffer> log_entry = WriteBuffer(entry);
        auto cmd_result = server_->append_entries({log_entry});
        if (cmd_result->get_result_code() == OK) {
            if (cmd_result->has_result()) {
                ReadBuffer(*response, *cmd_result->get());
            }
            return grpc::Status::OK;
        }
        return {grpc::StatusCode::UNAVAILABLE, "Service unavailable"};
    }

    grpc::Status Commit(
        grpc::ServerContext *context,
        const api::Transaction *request,
        api::TransactionResponse *response) override {
        api::RaftLogEntry entry;
        entry.mutable_commit()->CopyFrom(*request);
        return Submit(entry, response);
    }

    grpc::Status Rollback(
        grpc::ServerContext *context,
        const api::Transaction *request,
        api::TransactionResponse *response) override {
        api::RaftLogEntry entry;
        entry.mutable_rollback()->CopyFrom(*request);;
        return Submit(entry, response);
    }

    // write operations
    grpc::Status AddVertex(
        grpc::ServerContext *context,
        const api::AddVertexRequest *request,
        api::TransactionResponse *response) override {
        api::RaftLogEntry entry;
        entry.mutable_addvertex()->CopyFrom(*request);;
        return Submit(entry, response);
    }

    grpc::Status RemoveVertex(
        grpc::ServerContext *context,
        const api::RemoveVertexRequest *request,
        api::TransactionResponse *response) override {
        api::RaftLogEntry entry;
        entry.mutable_removevertex()->CopyFrom(*request);;
        return Submit(entry, response);
    }

    grpc::Status AddLabel(
        grpc::ServerContext *context,
        const api::AddLabelRequest *request,
        api::TransactionResponse *response) override {
        api::RaftLogEntry entry;
        entry.mutable_addlabel()->CopyFrom(*request);;
        return Submit(entry, response);
    }

    grpc::Status RemoveLabel(
        grpc::ServerContext *context,
        const api::RemoveLabelRequest *request,
        api::TransactionResponse *response) override {
        api::RaftLogEntry entry;
        entry.mutable_removelabel()->CopyFrom(*request);;
        return Submit(entry, response);
    }

    grpc::Status AddEdge(
        grpc::ServerContext *context,
        const api::AddEdgeRequest *request,
        api::TransactionResponse *response) override {
        api::RaftLogEntry entry;
        entry.mutable_addedge()->CopyFrom(*request);;
        return Submit(entry, response);
    }

    grpc::Status RemoveEdge(
        grpc::ServerContext *context,
        const api::RemoveEdgeRequest *request,
        api::TransactionResponse *response) override {
        api::RaftLogEntry entry;
        entry.mutable_removeedge()->CopyFrom(*request);;
        return Submit(entry, response);
    }

    // read operations
    grpc::Status GetLabels(
        grpc::ServerContext *context,
        const api::GetLabelsRequest *request,
        grpc::ServerWriter<api::Label> *writer) override {
        ptr<ReadTransaction> txn(graph->OpenForRead(request->txid()));
        VertexId vertexId = {request->vertexid().type(), request->vertexid().id()};
        return WriteItems<std::string, api::Label>(writer, txn->GetLabels(vertexId),
                                                   [](const string &label, api::Label &out) {
                                                       out.set_label(label);
                                                   });
    }

    grpc::Status GetEdges(
        grpc::ServerContext *context,
        const api::GetEdgesRequest *request,
        grpc::ServerWriter<api::Edge> *writer) override {
        ptr<ReadTransaction> txn(graph->OpenForRead(request->txid()));
        VertexId vertexId = {request->vertexid().type(), request->vertexid().id()};
        return WriteItems<Edge, api::Edge>(writer, txn->GetEdges(vertexId), [](const Edge &edge, api::Edge &out) {
            out.set_label(edge.label);
            out.set_direction(edge.direction == IN ? api::IN : api::OUT);
            auto vertex_id = out.mutable_vertexid();
            vertex_id->set_type(edge.vertexId.type);
            vertex_id->set_id(edge.vertexId.id);
            auto other_id = out.mutable_otherid();
            other_id->set_type(edge.otherId.type);
            other_id->set_id(edge.otherId.id);
        });
    }

    grpc::Status GetEdgesByLabel(
        grpc::ServerContext *context,
        const api::GetEdgesByLabelRequest *request,
        grpc::ServerWriter<api::Edge> *writer) override {
        ptr<ReadTransaction> txn(graph->OpenForRead(request->txid()));
        VertexId vertexId = {request->vertexid().type(), request->vertexid().id()};
        return WriteItems<Edge, api::Edge>(
            writer, txn->GetEdges(vertexId, request->label(), request->direction() == api::IN ? IN : OUT), [
            ](const Edge &edge, api::Edge &out) {
                out.set_label(edge.label);
                out.set_direction(edge.direction == IN ? api::IN : api::OUT);
                auto vertex_id = out.mutable_vertexid();
                vertex_id->set_type(edge.vertexId.type);
                vertex_id->set_id(edge.vertexId.id);
                auto other_id = out.mutable_otherid();
                other_id->set_type(edge.otherId.type);
                other_id->set_id(edge.otherId.id);
            });
    }

    grpc::Status GetVerticesByType(
        grpc::ServerContext *context,
        const api::GetVerticesByTypeRequest *request,
        grpc::ServerWriter<api::VertexId> *writer) override {
        ptr<ReadTransaction> txn(graph->OpenForRead(request->txid()));
        return WriteItems<VertexId, api::VertexId>(writer, txn->GetVerticesByType(request->type()),
                                                   [](const VertexId &vertexId, api::VertexId &out) {
                                                       out.set_type(vertexId.type);
                                                       out.set_id(vertexId.id);
                                                   });
    }

    grpc::Status GetVerticesByLabel(
        grpc::ServerContext *context,
        const api::GetVerticesByLabelRequest *request,
        grpc::ServerWriter<api::VertexId> *writer) override {
        ptr<ReadTransaction> txn(graph->OpenForRead(request->txid()));
        return WriteItems<VertexId, api::VertexId>(writer, txn->GetVerticesByLabel(request->label(), request->type()),
                                                   [](const VertexId &vertexId, api::VertexId &out) {
                                                       out.set_type(vertexId.type);
                                                       out.set_id(vertexId.id);
                                                   });
    }

private:
    grpc::Status Submit(const api::RaftLogEntry &entry, api::TransactionResponse *response) {
        auto cmd_result = server_->append_entries({WriteBuffer(entry)});
        if (cmd_result->get_result_code() == OK && cmd_result->has_result()) {
            ReadBuffer(*response, *cmd_result->get());
            return grpc::Status::OK;
        }
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service unavailable");
    }

    template<class D, class A>
    static grpc::Status WriteItems(
        grpc::ServerWriter<A> *writer,
        std::shared_ptr<EntryIterator<D>> items, std::function<void(const D &, A &)> convert) {
        grpc::WriteOptions wo;
        D item;
        bool first = true;
        while (items->Valid()) {
            if (first) {
                first = false;
            } else {
                A out;
                convert(item, out);
                writer->Write(out, wo);
            }
            item = items->Get();
            items->Next();
        }
        if (!first) {
            A out;
            convert(item, out);
            writer->WriteLast(out, wo);
        }
        return grpc::Status::OK;
    }

    ptr<raft_server> server_;
    ptr<raft_launcher> launcher_;
    ptr<StateMachine> my_state_machine_;

    ptr<Graph> graph;
};

std::unique_ptr<grpc::Server> server;

inline void signalHandler(int signum) {
    if (server) {
        server->Shutdown();
        std::cout << "Server shutting down." << std::endl;
    }
}

inline void RunServer(int grpcPort, int raftPort, int raftId, std::vector<RaftPeer> peers,
                      const std::string &storagePath) {
    std::string server_address("0.0.0.0:" + std::to_string(grpcPort));
    api::GritApi::Service *service = new Service(raftId, raftPort, cs_new<Graph>(storagePath), peers);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service);

    grpc::reflection::InitProtoReflectionServerBuilderPlugin();

    server = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    server->Wait();
    delete service;
}
