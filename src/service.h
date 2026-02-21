#pragma once

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include <csignal>

#include "api.grpc.pb.h"
#include "graph.h"
#include "in_memory_state_mgr.hxx"
#include "libnuraft/nuraft.hxx"
#include "state_machine.h"

using namespace nuraft;

class Logger : public logger {
   public:
    void put_details(
        int level,
        const char* source_file,
        const char* func_name,
        size_t line_number,
        const std::string& msg) {
        // INFO logging
        if (level <= 4) {
            std::cout << level << ": " << source_file << " :" << func_name << " ("
                      << line_number << "): " << msg << std::endl;
        }
    }
};

struct RaftPeer {
    int id;
    std::string address;
};

class Service : public api::GritApi::Service {
   public:
    Service(int id, int raftPort, const ptr<Graph>& graph, std::vector<RaftPeer> peers)
        : graph(graph) {
        ptr<logger> my_logger = cs_new<Logger>();
        ptr<state_mgr> my_state_manager =
            cs_new<inmem_state_mgr>(id, "localhost:" + std::to_string(raftPort));
        auto cluster_config = my_state_manager->load_config();
        for (auto& peer : peers) {
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
        server_ = launcher_->init(
            my_state_machine_, my_state_manager, my_logger, raftPort, asio_opt, params);

        while (!server_->is_initialized()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    ~Service() override { launcher_->shutdown(); }

    grpc::Status OpenTransaction(
        grpc::ServerContext* context,
        const api::OpenRequest* request,
        api::Transaction* response) override {
        // There is no need to go through raft for opening a read transaction
        // No mutations are needed.
        if (request->type() == api::READ) {
            ptr<ReadTransaction> txn(graph->OpenForRead(request->txid()));
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
        grpc::ServerContext* context,
        const api::Transaction* request,
        api::TransactionResponse* response) override {
        api::RaftLogEntry entry;
        entry.mutable_commit()->CopyFrom(*request);
        return Submit(entry, response);
    }

    grpc::Status Rollback(
        grpc::ServerContext* context,
        const api::Transaction* request,
        api::TransactionResponse* response) override {
        api::RaftLogEntry entry;
        entry.mutable_rollback()->CopyFrom(*request);
        ;
        return Submit(entry, response);
    }

    // write operations
    grpc::Status AddVertex(
        grpc::ServerContext* context,
        const api::AddVertexRequest* request,
        api::TransactionResponse* response) override {
        api::RaftLogEntry entry;
        entry.mutable_addvertex()->CopyFrom(*request);
        ;
        return Submit(entry, response);
    }

    grpc::Status RemoveVertex(
        grpc::ServerContext* context,
        const api::RemoveVertexRequest* request,
        api::TransactionResponse* response) override {
        api::RaftLogEntry entry;
        entry.mutable_removevertex()->CopyFrom(*request);
        ;
        return Submit(entry, response);
    }

    grpc::Status AddLabel(
        grpc::ServerContext* context,
        const api::AddLabelRequest* request,
        api::TransactionResponse* response) override {
        api::RaftLogEntry entry;
        entry.mutable_addlabel()->CopyFrom(*request);
        ;
        return Submit(entry, response);
    }

    grpc::Status RemoveLabel(
        grpc::ServerContext* context,
        const api::RemoveLabelRequest* request,
        api::TransactionResponse* response) override {
        api::RaftLogEntry entry;
        entry.mutable_removelabel()->CopyFrom(*request);
        ;
        return Submit(entry, response);
    }

    grpc::Status AddEdge(
        grpc::ServerContext* context,
        const api::AddEdgeRequest* request,
        api::TransactionResponse* response) override {
        api::RaftLogEntry entry;
        entry.mutable_addedge()->CopyFrom(*request);
        ;
        return Submit(entry, response);
    }

    grpc::Status RemoveEdge(
        grpc::ServerContext* context,
        const api::RemoveEdgeRequest* request,
        api::TransactionResponse* response) override {
        api::RaftLogEntry entry;
        entry.mutable_removeedge()->CopyFrom(*request);
        ;
        return Submit(entry, response);
    }

    // read operations
    grpc::Status GetLabels(
        grpc::ServerContext* context,
        const api::GetLabelsRequest* request,
        grpc::ServerWriter<api::Label>* writer) override {
        ptr<ReadTransaction> txn(graph->OpenForRead(request->txid()));
        VertexId vertexId = {request->vertexid().type(), request->vertexid().id()};
        return WriteItems<std::string, api::Label>(
            writer, txn->GetLabels(vertexId), [](const std::string& label, api::Label& out) {
                out.set_label(label);
            });
    }

    grpc::Status GetEdges(
        grpc::ServerContext* context,
        const api::GetEdgesRequest* request,
        grpc::ServerWriter<api::Edge>* writer) override {
        ptr<ReadTransaction> txn(graph->OpenForRead(request->txid()));
        return WriteMultiIterator<api::VertexId, Edge, api::Edge>(
            writer,
            request->vertices(),
            [txn,
             request](const api::VertexId& vertexId) -> std::shared_ptr<EntryIterator<Edge>> {
                VertexId id = {vertexId.type(), vertexId.id()};
                return txn->GetEdges(id);
            },
            convertEdge);
    }

    grpc::Status GetEdgesByLabel(
        grpc::ServerContext* context,
        const api::GetEdgesByLabelRequest* request,
        grpc::ServerWriter<api::Edge>* writer) override {
        ptr<ReadTransaction> txn(graph->OpenForRead(request->txid()));
        return WriteMultiIterator<api::VertexId, Edge, api::Edge>(
            writer,
            request->vertices(),
            [txn,
             request](const api::VertexId& vertexId) -> std::shared_ptr<EntryIterator<Edge>> {
                VertexId id = {vertexId.type(), vertexId.id()};
                return txn->GetEdges(
                    id, request->label(), request->direction() == api::IN ? IN : OUT);
            },
            convertEdge);
    }

    grpc::Status GetVerticesByType(
        grpc::ServerContext* context,
        const api::GetVerticesByTypeRequest* request,
        grpc::ServerWriter<api::VertexId>* writer) override {
        ptr<ReadTransaction> txn(graph->OpenForRead(request->txid()));
        return WriteItems<VertexId, api::VertexId>(
            writer, txn->GetVerticesByType(request->type()), convertVertex);
    }

    grpc::Status GetVerticesByLabel(
        grpc::ServerContext* context,
        const api::GetVerticesByLabelRequest* request,
        grpc::ServerWriter<api::VertexId>* writer) override {
        ptr<ReadTransaction> txn(graph->OpenForRead(request->txid()));
        return WriteMultiIterator<std::string, VertexId, api::VertexId>(
            writer,
            request->labels(),
            [txn,
             request](const std::string& label) -> std::shared_ptr<EntryIterator<VertexId>> {
                return txn->GetVerticesByLabel(label, request->type());
            },
            convertVertex);
    }

   private:
    static void convertVertex(const VertexId& vertexId, api::VertexId& out) {
        out.set_type(vertexId.type);
        out.set_id(vertexId.id);
    }

    static void convertEdge(const Edge& edge, api::Edge& out) {
        out.set_label(edge.label);
        out.set_direction(edge.direction == IN ? api::IN : api::OUT);
        auto vertex_id = out.mutable_vertexid();
        vertex_id->set_type(edge.vertexId.type);
        vertex_id->set_id(edge.vertexId.id);
        auto other_id = out.mutable_otherid();
        other_id->set_type(edge.otherId.type);
        other_id->set_id(edge.otherId.id);
    }

    grpc::Status Submit(const api::RaftLogEntry& entry, api::TransactionResponse* response) {
        auto cmd_result = server_->append_entries({WriteBuffer(entry)});
        if (cmd_result->get_result_code() == OK && cmd_result->has_result()) {
            ReadBuffer(*response, *cmd_result->get());
            return grpc::Status::OK;
        }
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service unavailable");
    }

    template <class D, class A>
    static grpc::Status WriteItems(
        grpc::ServerWriter<A>* writer,
        std::shared_ptr<EntryIterator<D>> items,
        std::function<void(const D&, A&)> convert) {
        grpc::WriteOptions wo;
        A out;
        bool first = WriteIterator(writer, items, convert, out);
        if (!first) {
            writer->WriteLast(out, wo);
        }
        return grpc::Status::OK;
    }

    template <class K, class D, class A>
    static grpc::Status WriteMultiIterator(
        grpc::ServerWriter<A>* writer,
        const google::protobuf::RepeatedPtrField<K>& field,
        std::function<std::shared_ptr<EntryIterator<D>>(const K&)> to_iter,
        std::function<void(const D&, A&)> convert) {
        A out;
        bool first = false;
        for (auto it = field.begin(); it != field.end(); it++) {
            if (!first) {
                writer->Write(out);
            }
            first = WriteIterator<D, A>(writer, to_iter(*it), convert, out);
        }
        if (!first) {
            writer->WriteLast(out, grpc::WriteOptions());
        }
        return grpc::Status::OK;
    }

    template <class D, class A>
    static bool WriteIterator(
        grpc::ServerWriter<A>* writer,
        std::shared_ptr<EntryIterator<D>> items,
        std::function<void(const D&, A&)> convert,
        A& out) {
        D item;
        bool first = true;
        while (items->Valid()) {
            if (first) {
                first = false;
            } else {
                writer->Write(out);
            }
            item = items->Get();
            convert(item, out);
            items->Next();
        }
        return first;
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

inline void RunServer(
    int grpcPort,
    int raftPort,
    int raftId,
    std::vector<RaftPeer> peers,
    const std::string& storagePath) {
    Storage storage(storagePath);
    std::string server_address("0.0.0.0:" + std::to_string(grpcPort));
    api::GritApi::Service* service =
        new Service(raftId, raftPort, cs_new<Graph>(storage), peers);

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
