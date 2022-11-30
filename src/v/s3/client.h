/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "http/client.h"
#include "model/fundamental.h"
#include "outcome.h"
#include "s3/configuration.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>

namespace s3 {

struct object_tag {
    ss::sstring key;
    ss::sstring value;
};

class client {
public:
    virtual ~client() = default;

    /// Stop the client
    virtual ss::future<> stop() = 0;

    /// Shutdown the underlying connection
    virtual void shutdown() = 0;

    /// Download object from cloud storage.
    ///
    /// \param name is a bucket name
    /// \param key is an object key
    /// \param timeout is a timeout of the operation
    /// \param expect_no_such_key log missing key events as warnings if false
    /// \return future that becomes ready after request was sent
    virtual ss::future<http::client::response_stream_ref> get_object(
      bucket_name const& name,

      object_key const& key,
      const ss::lowres_clock::duration& timeout,
      bool expect_no_such_key = false)
      = 0;

    struct head_object_result {
        uint64_t object_size;
        ss::sstring etag;
    };

    /// Get metadata for object from cloud storage.
    ///
    /// \param name is a bucket name
    /// \param key is an object key
    /// \param timeout is a timeout of the operation
    /// \return future that becomes ready when the request is completed
    virtual ss::future<head_object_result> head_object(
      bucket_name const& name,
      object_key const& key,
      const ss::lowres_clock::duration& timeout)
      = 0;

    /// Upload object to cloud storage
    ///
    /// \param name is a bucket name
    /// \param key is an id of the object
    /// \param payload_size is a size of the object in bytes
    /// \param body is an input_stream that can be used to read body
    /// \param tags is a list of kv tags to attach to the object
    /// \param timeout is a timeout of the operation
    /// \return future that becomes ready when the upload is completed
    virtual ss::future<> put_object(
      bucket_name const& name,
      object_key const& key,
      size_t payload_size,
      ss::input_stream<char>&& body,
      const std::vector<object_tag>& tags,
      const ss::lowres_clock::duration& timeout)
      = 0;

    struct list_bucket_item {
        ss::sstring key;
        std::chrono::system_clock::time_point last_modified;
        size_t size_bytes;
        ss::sstring etag;
    };
    struct list_bucket_result {
        bool is_truncated;
        ss::sstring prefix;
        std::vector<list_bucket_item> contents;
    };

    /// List the objects in a bucket
    ///
    /// \param name is a bucket name
    /// \param prefix optional prefix of objects to list
    /// \param start_after optional object key to start listing after
    /// \param max_keys optional upper bound on the number of returned keys
    /// \param body is an input_stream that can be used to read body
    /// \return future that becomes ready when the request is completed
    virtual ss::future<list_bucket_result> list_objects_v2(
      const bucket_name& name,
      std::optional<object_key> prefix = std::nullopt,
      std::optional<object_key> start_after = std::nullopt,
      std::optional<size_t> max_keys = std::nullopt,
      const ss::lowres_clock::duration& timeout = http::default_connect_timeout)
      = 0;

    /// Delete object from cloud storage
    ///
    /// \param name is a bucket name
    /// \param key is an object key
    /// \param timeout is a timeout of the operation
    /// \return future that becomes ready when the request is completed
    virtual ss::future<> delete_object(
      const bucket_name& bucket,
      const object_key& key,
      const ss::lowres_clock::duration& timeout)
      = 0;
};

} // namespace s3
