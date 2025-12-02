#pragma once

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include <csignal>

#include "api.grpc.pb.h"
#include "graph.h"

class Service : public api::GritApi::Service {
   public:
    Service(Graph* graph) : api::GritApi::Service(), graph(graph) {}

    ::grpc::Status OpenTransaction(
        ::grpc::ServerContext* context,
        const ::api::OpenRequest* request,
        ::api::Transaction* response) {
        if (request->type() == api::READ) {
            std::unique_ptr<ReadTransaction> txn(graph->OpenForRead(request->txid()));
            response->set_txid(txn->GetTxId());
        } else {
            WriteTransaction* txn = graph->OpenForWrite();
            open.insert(std::pair(txn->GetTxId(), std::unique_ptr<WriteTransaction>(txn)));
            response->set_txid(txn->GetTxId());
        }
        return grpc::Status::OK;
    }

    ::grpc::Status Commit(
        ::grpc::ServerContext* context,
        const ::api::Transaction* request,
        ::google::protobuf::Empty* response) {
        uint64_t txId = request->txid();
        if (open.find(txId) != open.end()) {
            open.find(txId)->second->Commit();
            open.erase(txId);
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Transaction not found");
        }
    }
    ::grpc::Status Rollback(
        ::grpc::ServerContext* context,
        const ::api::Transaction* request,
        ::google::protobuf::Empty* response) {
        uint64_t txId = request->txid();
        if (open.find(txId) != open.end()) {
            open.find(txId)->second->Rollback();
            open.erase(txId);
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Transaction not found");
        }
    }

    // write operations
    ::grpc::Status AddVertex(
        ::grpc::ServerContext* context,
        const ::api::AddVertexRequest* request,
        ::google::protobuf::Empty* response) {
        uint64_t txId = request->txid();
        if (open.find(txId) != open.end()) {
            open.find(txId)->second->AddVertex(
                {request->vertexid().type(), request->vertexid().id()});
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Transaction not found");
        }
    }

    ::grpc::Status RemoveVertex(
        ::grpc::ServerContext* context,
        const ::api::RemoveVertexRequest* request,
        ::google::protobuf::Empty* response) {
        uint64_t txId = request->txid();
        if (open.find(txId) != open.end()) {
            open.find(txId)->second->RemoveVertex(
                {request->vertexid().type(), request->vertexid().id()});
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Transaction not found");
        }
    }

    ::grpc::Status AddLabel(
        ::grpc::ServerContext* context,
        const ::api::AddLabelRequest* request,
        ::google::protobuf::Empty* response) {
        uint64_t txId = request->txid();
        if (open.find(txId) != open.end()) {
            open.find(txId)->second->AddLabel(
                {request->vertexid().type(), request->vertexid().id()}, request->label());
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Transaction not found");
        }
    }
    ::grpc::Status RemoveLabel(
        ::grpc::ServerContext* context,
        const ::api::RemoveLabelRequest* request,
        ::google::protobuf::Empty* response) {
        uint64_t txId = request->txid();
        if (open.find(txId) != open.end()) {
            open.find(txId)->second->RemoveLabel(
                {request->vertexid().type(), request->vertexid().id()}, request->label());
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Transaction not found");
        }
    }

    ::grpc::Status AddEdge(
        ::grpc::ServerContext* context,
        const ::api::AddEdgeRequest* request,
        ::google::protobuf::Empty* response) {
        uint64_t txId = request->txid();
        if (open.find(txId) != open.end()) {
            open.find(txId)->second->AddEdge(
                request->label(),
                {request->fromvertexid().type(), request->fromvertexid().id()},
                {request->tovertexid().type(), request->tovertexid().id()});
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Transaction not found");
        }
    }

    ::grpc::Status RemoveEdge(
        ::grpc::ServerContext* context,
        const ::api::RemoveEdgeRequest* request,
        ::google::protobuf::Empty* response) {
        uint64_t txId = request->txid();
        if (open.find(txId) != open.end()) {
            open.find(txId)->second->RemoveEdge(
                request->label(),
                {request->fromvertexid().type(), request->fromvertexid().id()},
                {request->tovertexid().type(), request->tovertexid().id()});
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Transaction not found");
        }
    }

    // read operations
    ::grpc::Status GetVerticesByType(
        ::grpc::ServerContext* context,
        const ::api::GetVerticesByTypeRequest* request,
        ::grpc::ServerWriter<::api::VertexId>* writer) {
        uint64_t txId = request->txid();
        std::unique_ptr<VertexIterator> vertices;
        if (open.find(txId) != open.end()) {
            ReadTransaction* txn = open.find(txId)->second.get();
            vertices.reset(txn->GetVerticesByType(request->type()));
        } else {
            std::unique_ptr<ReadTransaction> txn(graph->OpenForRead(txId));
            vertices.reset(txn->GetVerticesByType(request->type()));
        }
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
    std::unique_ptr<Graph> graph;
    std::map<uint64_t, std::unique_ptr<WriteTransaction>> open;
};

std::unique_ptr<grpc::Server> server;

void signalHandler(int signum) {
    if (server) {
        server->Shutdown();
        std::cout << "Server shutting down." << std::endl;
    }
}

void RunServer(int grpcPort, int raftPort, int raftId) {
    std::string server_address("0.0.0.0:" + std::to_string(grpcPort));
    api::GritApi::Service* service = new Service(new Graph("/tmp/graph"));

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
