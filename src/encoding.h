#pragma once

#include <format>
#include <iostream>
#include <rocksdb/slice.h>

#include "buffer.hxx"
#include "model.h"

/**
 * Field order also determines key sorting. Keys are sorted bytewise, with reverse time order (newest first).
 * For querying, iterators over prefixes are used.
 */
class encoding {
public:
    encoding() {
        buffer = nuraft::buffer::alloc(256);
    }

    encoding(const std::string &value) {
        buffer = nuraft::buffer::alloc(value.size() + sizeof(uint32_t));
        buffer->put_raw(reinterpret_cast<const nuraft::byte *>(value.data()), value.size());
        buffer->pos(sizeof(uint32_t));
    }

    encoding(const rocksdb::Slice &value) {
        buffer = nuraft::buffer::alloc(value.size() + sizeof(uint32_t));
        buffer->put(value.data(), value.size());
        buffer->pos(sizeof(uint32_t));
    }

    encoding get_ulong(ulong &out) {
        out = buffer->get_ulong();
        return *this;
    }

    encoding put_ulong(ulong in) {
        grow(sizeof(ulong));
        buffer->put(in);
        return *this;
    }

    encoding get_string(std::string &out) {
        int size = buffer->get_int();
        out.assign(reinterpret_cast<const char *>(buffer->get_raw(size)), size);
        return *this;
    }

    encoding put_string(const std::string &str) {
        grow(str.size() + sizeof(int32_t));
        buffer->put(str.data(), str.size());
        return *this;
    }

    encoding get_vertex(VertexId &vid) {
        get_string(vid.type);
        vid.id = buffer->get_ulong();
        return *this;
    }

    encoding put_vertex(const VertexId &vertex_id) {
        put_string(vertex_id.type);
        grow(sizeof(ulong));
        buffer->put(vertex_id.id);
        return *this;
    }

    encoding get_direction(Direction &direction) {
        direction = buffer->get_byte() == '\0' ? IN : OUT;
        return *this;
    }

    encoding &put_direction(const Direction &direction) {
        grow(1);
        nuraft::byte val = direction == IN ? '\0' : '\1';
        buffer->put(val);
        return *this;
    }

    encoding &get_edge(Edge &edge) {
        get_vertex(edge.vertexId);
        get_string(edge.label);
        get_direction(edge.direction);
        get_vertex(edge.otherId);
        return *this;
    }

    encoding &put_edge(const Edge &edge) {
        put_vertex(edge.vertexId);
        put_string(edge.label);
        put_direction(edge.direction);
        put_vertex(edge.otherId);
        return *this;
    }

    encoding &get_merge(MergeValue &out) {
        nuraft::byte byte = buffer->get_byte();
        out.action = byte == '\0' ? PUT : DELETE;
        out.txId = buffer->get_ulong();
        return *this;
    }

    encoding &put_merge(const MergeValue &value) {
        grow(sizeof(ulong) + 1);
        nuraft::byte b = value.action == PUT ? '\0' : '\1';
        buffer->put(b);
        buffer->put(value.txId);
        return *this;
    }

    const rocksdb::Slice ToSlice() const {
        return rocksdb::Slice(reinterpret_cast<const char *>(buffer->data_begin()), buffer->pos());
    }

    const std::string ToString() const {
        return std::string(reinterpret_cast<const char *>(buffer->data_begin()), buffer->pos());
    }

    const std::string ToHex(bool read = false) const {
        int max = read ? buffer->size() : buffer->pos();
        int start = read ? sizeof(uint32_t) : 0;
        std::stringstream ss;
        ss << "0x";
        for (int i = start; i < max; i++) {
            ss << std::format("{:02x}", buffer->data_begin()[i]);
        }
        return ss.str();
    }

private:
    void grow(int size) {
        if (buffer->pos() + size > buffer->size()) {
            uint newsize = size > buffer->size() ? buffer->size() + size : 2 * buffer->size();
            buffer = nuraft::buffer::expand(*buffer, newsize);
        }
    }

    nuraft::ptr<nuraft::buffer> buffer;
};
