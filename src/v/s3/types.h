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

#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

namespace s3 {

using object_key = named_type<std::filesystem::path, struct s3_object_key>;
using endpoint_url = named_type<ss::sstring, struct s3_endpoint_url>;
using ca_trust_file
  = named_type<std::filesystem::path, struct s3_ca_trust_file>;
using access_point_uri = named_type<ss::sstring, struct s3_access_point_uri>;

// TODO(vlad): Fix this comment
/// List of default overrides that can be used to workaround issues
/// that can arise when we want to deal with different S3 API implementations
/// and different OS issues (like different truststore locations on different
/// Linux distributions).
struct default_overrides {
    std::optional<endpoint_url> endpoint = std::nullopt;
    std::optional<uint16_t> port = std::nullopt;
    std::optional<ca_trust_file> trust_file = std::nullopt;
    std::optional<ss::lowres_clock::duration> max_idle_time = std::nullopt;
    bool disable_tls = false;
};

} // namespace s3
