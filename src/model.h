#pragma once

#include <string>

struct VertexId {
    std::string type;
    long id;
};

constexpr bool operator<(const VertexId& a, const VertexId& b) {
    int cmp = a.type.compare(b.type);
    return cmp == 0 ? a.id < b.id : cmp < 0;
}

enum Direction { IN, OUT };

struct Edge {
    VertexId vertexId;
    VertexId otherId;
    std::string label;
    Direction direction;
};
