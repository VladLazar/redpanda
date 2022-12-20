/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/abs_error.h"

#include <boost/lexical_cast.hpp>

#include <map>

namespace cloud_storage_clients {

// NOLINTNEXTLINE
static const std::map<ss::sstring, abs_error_code> known_abs_error_codes = {
  {"BlobNotFound", abs_error_code::blob_not_found},
  {"ServerBusy", abs_error_code::server_busy},
  {"InternalError", abs_error_code::internal_error},
  {"SystemInUse", abs_error_code::system_in_use},
  {"BlobNotFound", abs_error_code::blob_not_found},
  {"AccountBeingCreated", abs_error_code::account_being_created},
  {"ResourceAlreadyExists", abs_error_code::resource_already_exists},
  {"BlobAlreadyExists", abs_error_code::blob_already_exists},
  {"InvalidBlobOrBlock", abs_error_code::invalid_blob},
  {"PendingCopyOperation", abs_error_code::pending_copy_operation},
  {"SnapshotPresent", abs_error_code::snapshot_present},
  {"BlobBeingRehydrated", abs_error_code::blob_being_rehydrated},
  {"ContainerDisabled", abs_error_code::container_being_disabled},
  {"ContainerBeingDeleted", abs_error_code::container_being_deleted},
  {"ContainerNotFound", abs_error_code::container_not_found}};

std::istream& operator>>(std::istream& i, abs_error_code& code) {
    ss::sstring c;
    i >> c;
    auto it = known_abs_error_codes.find(c);
    if (it != known_abs_error_codes.end()) {
        code = it->second;
    } else {
        code = abs_error_code::_unknown;
    }
    return i;
}

abs_rest_error_response::abs_rest_error_response(
  ss::sstring code, ss::sstring message, boost::beast::http::status http_code)
  : _code(boost::lexical_cast<abs_error_code>(code))
  , _code_str(std::move(code))
  , _message(std::move(message))
  , _http_code(http_code) {}

const char* abs_rest_error_response::what() const noexcept {
    return _message.c_str();
}

abs_error_code abs_rest_error_response::code() const noexcept { return _code; }

std::string_view abs_rest_error_response::code_string() const noexcept {
    return _code_str;
}

std::string_view abs_rest_error_response::message() const noexcept {
    return _message;
}

boost::beast::http::status abs_rest_error_response::http_code() const noexcept {
    return _http_code;
}

} // namespace cloud_storage_clients
