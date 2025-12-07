#pragma once

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include <csignal>

#include "api.grpc.pb.h"
#include "graph.h"
#include "in_memory_state_mgr.hxx"
#include "libnuraft/nuraft.hxx"

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
    StateMachine(ptr<Graph> graph, rocksdb::DB *db) : graph(graph), db(db) {
    }

    ptr<buffer> commit(const ulong log_idx, buffer &data) {
        std::cout << "committing status " << log_idx << std::endl;

        rocksdb::WriteOptions writeOptions;
        std::string txStr;
        rocksdb::Slice txSlice(rocksdb::EncodeU64Ts(log_idx, &txStr));
        auto status = db->Put(writeOptions, "state_machine", txSlice, std::to_string(log_idx));
        assert(status.ok());

        std::cout << "executing " << log_idx << std::endl;
        api::RaftLogEntry entry;
        ReadBuffer(entry, data);
        if (entry.has_open()) {
            const api::OpenRequest &request = entry.open();
            ptr<ReadTransaction> txn = OpenTransaction(request);
            api::Transaction response;
            if (txn->IsReadOnly()) {
                response.set_type(api::READ);
            } else {
                response.set_type(api::WRITE);
            }
            response.set_txid(txn->GetTxId());
            return WriteBuffer(response);
        }
        api::TransactionResponse response;
        try {
            if (entry.has_commit()) {
                Commit(entry.commit());
            } else if (entry.has_rollback()) {
                Rollback(entry.rollback());
            } else if (entry.has_addvertex()) {
                AddVertex(entry.addvertex());
            } else if (entry.has_removevertex()) {
                RemoveVertex(entry.removevertex());
            } else if (entry.has_addlabel()) {
                AddLabel(entry.addlabel());
            } else if (entry.has_removelabel()) {
                RemoveLabel(entry.removelabel());
            } else if (entry.has_addedge()) {
                AddEdge(entry.addedge());
            } else if (entry.has_removeedge()) {
                RemoveEdge(entry.removeedge());
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

    ptr<ReadTransaction> OpenTransaction(const api::OpenRequest &request) {
        if (request.type() == api::READ) {
            return ptr<ReadTransaction>(graph->OpenForRead(request.txid()));
        } else {
            ptr<WriteTransaction> txn(graph->OpenForWrite());
            openTxns.insert(std::pair(txn->GetTxId(), txn));
            return txn;
        }
    }

    void Commit(const api::Transaction &request) {
        uint64_t txId = request.txid();
        Tx(txId)->Commit();
        openTxns.erase(txId);
    }

    void Rollback(const api::Transaction &request) {
        uint64_t txId = request.txid();
        Tx(txId)->Rollback();
        openTxns.erase(txId);
    }

    void AddVertex(const api::AddVertexRequest &request) {
        Tx(request.txid())->AddVertex(
            {request.vertexid().type(), request.vertexid().id()});
    }

    void RemoveVertex(const api::RemoveVertexRequest &request) {
        Tx(request.txid())->RemoveVertex(
            {request.vertexid().type(), request.vertexid().id()});
    }

    void AddLabel(const api::AddLabelRequest &request) {
        Tx(request.txid())->AddLabel({
                                         request.vertexid().type(), request.vertexid().id()
                                     }, request.label());
    }

    void RemoveLabel(const api::RemoveLabelRequest &request) {
        Tx(request.txid())->RemoveLabel({
                                            request.vertexid().type(), request.vertexid().id()
                                        }, request.label());
    }

    void AddEdge(const api::AddEdgeRequest &request) {
        Tx(request.txid())->AddEdge(
            request.label(),
            {request.fromvertexid().type(), request.fromvertexid().id()},
            {request.tovertexid().type(), request.tovertexid().id()});
    }

    void RemoveEdge(const api::RemoveEdgeRequest &request) {
        Tx(request.txid())->RemoveEdge(
            request.label(),
            {request.fromvertexid().type(), request.fromvertexid().id()},
            {request.tovertexid().type(), request.tovertexid().id()});
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
        auto status = db->Get(readOptions, "state_machine", &value);
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

private:
    ptr<WriteTransaction> Tx(uint64_t txId) {
        if (openTxns.find(txId) != openTxns.end()) {
            return openTxns.find(txId)->second;
        } else {
            throw TX_NOT_IN_PROGRESS;
        }
    }

    ptr<Graph> graph;
    rocksdb::DB *db;
    std::map<uint64_t, ptr<WriteTransaction> > openTxns;
};

class Service : public api::GritApi::Service {
public:
    Service(int id, int raftPort, const ptr<Graph> &graph, std::vector<RaftPeer> peers) : graph(graph) {
        ptr<logger> my_logger = cs_new<Logger>();
        ptr<state_mgr> my_state_manager = cs_new<inmem_state_mgr>(id, "localhost:" +
                                                                      std::to_string(raftPort));
        auto cluster_config = my_state_manager->load_config();
        for (auto peer = peers.begin(); peer != peers.end(); ++peer) {
            ptr<srv_config> peer_conf = cs_new<srv_config>(peer->id, peer->address);
            cluster_config->get_servers().push_back(peer_conf);
        }
        asio_service::options asio_opt;
        raft_params params;
        params.custom_commit_quorum_size_ = peers.size() / 2 + 1;
        params.return_method_ = raft_params::blocking;
        params.auto_forwarding_ = true;

        my_state_machine_ = cs_new<StateMachine>(graph, graph->GetDB());
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
        EntryIterator<D> *iter, std::function<void(const D &, A &)> convert) {
        std::unique_ptr<EntryIterator<D> > items(iter);
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
