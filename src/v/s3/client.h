/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_roles/apply_credentials.h"
#include "http/client.h"
#include "model/fundamental.h"
#include "outcome.h"
#include "s3/client_probe.h"
#include "s3/configuration.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>

namespace s3 {

struct object_tag {
    ss::sstring key;
    ss::sstring value;
};

/// Request formatter for AWS S3
class request_creator {
public:
    /// C-tor
    /// \param conf is a configuration container
    explicit request_creator(
      const configuration& conf,
      ss::lw_shared_ptr<const cloud_roles::apply_credentials>
        apply_credentials);

    /// \brief Create unsigned 'PutObject' request header
    /// The payload is unsigned which means that we don't need to calculate
    /// hash from it (which don't want to do for large files).
    ///
    /// \param name is a bucket that should be used to store new object
    /// \param key is an object name
    /// \param payload_size_bytes is a size of the object in bytes
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_unsigned_put_object_request(
      bucket_name const& name,
      object_key const& key,
      size_t payload_size_bytes,
      const std::vector<object_tag>& tags);

    /// \brief Create a 'GetObject' request header
    ///
    /// \param name is a bucket that has the object
    /// \param key is an object name
    /// \return initialized and signed http header or error
    result<http::client::request_header>
    make_get_object_request(bucket_name const& name, object_key const& key);

    /// \brief Create a 'HeadObject' request header
    ///
    /// \param name is a bucket that has the object
    /// \param key is an object name
    /// \return initialized and signed http header or error
    result<http::client::request_header>
    make_head_object_request(bucket_name const& name, object_key const& key);

    /// \brief Create a 'DeleteObject' request header
    ///
    /// \param name is a bucket that has the object
    /// \param key is an object name
    /// \return initialized and signed http header or error
    result<http::client::request_header>
    make_delete_object_request(bucket_name const& name, object_key const& key);

    /// \brief Initialize http header for 'ListObjectsV2' request
    ///
    /// \param name of the bucket
    /// \param region to connect
    /// \param max_keys is a max number of returned objects
    /// \param offset is an offset of the first returned object
    /// \return initialized and signed http header or error
    result<http::client::request_header> make_list_objects_v2_request(
      const bucket_name& name,
      std::optional<object_key> prefix,
      std::optional<object_key> start_after,
      std::optional<size_t> max_keys);

private:
    access_point_uri _ap;
    /// Applies credentials to http requests by adding headers and signing
    /// request payload. Shared pointer so that the credentials can be rotated
    /// through the client pool.
    ss::lw_shared_ptr<const cloud_roles::apply_credentials> _apply_credentials;
};

/// S3 REST-API client
class client {
public:
    client(
      const configuration& conf,
      ss::lw_shared_ptr<const cloud_roles::apply_credentials>
        apply_credentials);
    client(
      const configuration& conf,
      const ss::abort_source& as,
      ss::lw_shared_ptr<const cloud_roles::apply_credentials>
        apply_credentials);

    /// Stop the client
    ss::future<> stop();
    /// Shutdown the underlying connection
    void shutdown();

    /// Download object from S3 bucket
    ///
    /// \param name is a bucket name
    /// \param key is an object key
    /// \param timeout is a timeout of the operation
    /// \param expect_no_such_key log 404 as warning if set to false
    /// \return future that gets ready after request was sent
    ss::future<http::client::response_stream_ref> get_object(
      bucket_name const& name,
      object_key const& key,
      const ss::lowres_clock::duration& timeout,
      bool expect_no_such_key = false);

    struct head_object_result {
        uint64_t object_size;
        ss::sstring etag;
    };

    /// HeadObject request.
    /// \param name is a bucket name
    /// \param key is an id of the object
    /// \return future that becomes ready when the request is completed
    ss::future<head_object_result> head_object(
      bucket_name const& name,
      object_key const& key,
      const ss::lowres_clock::duration& timeout);

    /// Put object to S3 bucket.
    /// \param name is a bucket name
    /// \param key is an id of the object
    /// \param payload_size is a size of the object in bytes
    /// \param body is an input_stream that can be used to read body
    /// \return future that becomes ready when the upload is completed
    ss::future<> put_object(
      bucket_name const& name,
      object_key const& key,
      size_t payload_size,
      ss::input_stream<char>&& body,
      const std::vector<object_tag>& tags,
      const ss::lowres_clock::duration& timeout);

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
    ss::future<list_bucket_result> list_objects_v2(
      const bucket_name& name,
      std::optional<object_key> prefix = std::nullopt,
      std::optional<object_key> start_after = std::nullopt,
      std::optional<size_t> max_keys = std::nullopt,
      const ss::lowres_clock::duration& timeout
      = http::default_connect_timeout);

    ss::future<> delete_object(
      const bucket_name& bucket,
      const object_key& key,
      const ss::lowres_clock::duration& timeout);

private:
    request_creator _requestor;
    http::client _client;
    ss::shared_ptr<client_probe> _probe;
};

} // namespace s3
