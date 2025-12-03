#pragma once

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include <csignal>

#include "api.grpc.pb.h"
#include "graph.h"
#include "in_memory_state_mgr.hxx"
#include "libnuraft/nuraft.hxx"

using namespace nuraft;

template<typename T>
void ReadBuffer(T &target, buffer &buffer) {
    target.ParseFromArray(buffer.data(), buffer.size());
}

template<typename T>
ptr<buffer> WriteBuffer(T &target) {
    size_t size = target.ByteSizeLong();
    ptr<buffer> ret = buffer::alloc(size);
    target.SerializeToArray(ret->data(), size);
    return ret;
}

class StateMachine : public state_machine {
public:
    StateMachine(ptr<Graph> graph) : graph(graph) {
    }

    ptr<buffer> commit(const ulong log_idx, buffer &data) {
        last_committed_idx_ = log_idx;

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
        return last_committed_idx_;
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

    std::atomic<uint64_t> last_committed_idx_;
    ptr<Graph> graph;
    std::map<uint64_t, ptr<WriteTransaction> > openTxns;
};

class Service : public api::GritApi::Service {
public:
    Service(int id, int raftPort, const ptr<Graph> &graph) : graph(graph) {
        ptr<logger> my_logger = nullptr;
        ptr<state_mgr> my_state_manager = cs_new<inmem_state_mgr>(id, "localhost:" +
                                                                      std::to_string(raftPort));
        asio_service::options asio_opt;
        raft_params params;
        params.return_method_ = raft_params::blocking;

        my_state_machine_ = cs_new<StateMachine>(graph);
        launcher_ = cs_new<raft_launcher>();
        server_ = launcher_->init(my_state_machine_, my_state_manager, my_logger, raftPort,
                                  asio_opt, params);

        while (!server_->is_initialized()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    ~Service() {
        launcher_->shutdown();
    }

    grpc::Status OpenTransaction(
        grpc::ServerContext *context,
        const api::OpenRequest *request,
        api::Transaction *response) {
        api::RaftLogEntry entry;
        entry.mutable_open()->CopyFrom(*request);
        ptr<buffer> log_entry = WriteBuffer(entry);

        auto cmd_result = server_->append_entries({log_entry});
        if (cmd_result->has_result()) {
            ReadBuffer(*response, *cmd_result->get());
            return grpc::Status::OK;
        }
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service unavailable");
    }

    grpc::Status Commit(
        grpc::ServerContext *context,
        const api::Transaction *request,
        api::TransactionResponse *response) {
        api::RaftLogEntry entry;
        entry.mutable_commit()->CopyFrom(*request);
        return Submit(entry, response);
    }

    grpc::Status Rollback(
        grpc::ServerContext *context,
        const api::Transaction *request,
        api::TransactionResponse *response) {
        api::RaftLogEntry entry;
        entry.mutable_rollback()->CopyFrom(*request);;
        return Submit(entry, response);
    }

    // write operations
    grpc::Status AddVertex(
        grpc::ServerContext *context,
        const api::AddVertexRequest *request,
        api::TransactionResponse *response) {
        api::RaftLogEntry entry;
        entry.mutable_addvertex()->CopyFrom(*request);;
        return Submit(entry, response);
    }


    grpc::Status RemoveVertex(
        grpc::ServerContext *context,
        const api::RemoveVertexRequest *request,
        api::TransactionResponse *response) {
        api::RaftLogEntry entry;
        entry.mutable_removevertex()->CopyFrom(*request);;
        return Submit(entry, response);
    }

    grpc::Status AddLabel(
        grpc::ServerContext *context,
        const api::AddLabelRequest *request,
        api::TransactionResponse *response) {
        api::RaftLogEntry entry;
        entry.mutable_addlabel()->CopyFrom(*request);;
        return Submit(entry, response);
    }

    grpc::Status RemoveLabel(
        grpc::ServerContext *context,
        const api::RemoveLabelRequest *request,
        api::TransactionResponse *response) {
        api::RaftLogEntry entry;
        entry.mutable_removelabel()->CopyFrom(*request);;
        return Submit(entry, response);
    }

    grpc::Status AddEdge(
        grpc::ServerContext *context,
        const api::AddEdgeRequest *request,
        api::TransactionResponse *response) {
        api::RaftLogEntry entry;
        entry.mutable_addedge()->CopyFrom(*request);;
        return Submit(entry, response);
    }


    grpc::Status RemoveEdge(
        grpc::ServerContext *context,
        const api::RemoveEdgeRequest *request,
        api::TransactionResponse *response) {
        api::RaftLogEntry entry;
        entry.mutable_removeedge()->CopyFrom(*request);;
        return Submit(entry, response);
    }

    // read operations
    grpc::Status GetVerticesByType(
        grpc::ServerContext *context,
        const api::GetVerticesByTypeRequest *request,
        grpc::ServerWriter<api::VertexId> *writer) {
        uint64_t txId = request->txid();
        ptr<ReadTransaction> txn(graph->OpenForRead(txId));
        std::unique_ptr<VertexIterator> vertices(txn->GetVerticesByType(request->type()));
        grpc::WriteOptions wo;
        api::VertexId out;
        VertexId vertexId;
        bool first = true;
        while (vertices->Valid()) {
            if (first) {
                first = false;
            } else {
                out.set_type(vertexId.type);
                out.set_id(vertexId.id);
                writer->Write(out, wo);
            }
            vertexId = vertices->Get();
            vertices->Next();
        }
        if (!first) {
            out.set_type(vertexId.type);
            out.set_id(vertexId.id);
            writer->WriteLast(out, wo);
        }
        return grpc::Status::OK;
    }

private:
    grpc::Status Submit(const api::RaftLogEntry &entry, api::TransactionResponse *response) {
        auto cmd_result = server_->append_entries({WriteBuffer(entry)});
        if (cmd_result->has_result()) {
            ReadBuffer(*response, *cmd_result->get());
            return grpc::Status::OK;
        }
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service unavailable");
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

inline void RunServer(int grpcPort, int raftPort, int raftId) {
    std::string server_address("0.0.0.0:" + std::to_string(grpcPort));
    api::GritApi::Service *service = new Service(raftId, raftPort, cs_new<Graph>("/tmp/graph"));

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
