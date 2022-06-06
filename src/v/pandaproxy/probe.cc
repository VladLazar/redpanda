// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/probe.h"

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace pandaproxy {

probe::probe(ss::httpd::path_description& path_desc)
  : _request_hist()
  , _metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    std::vector<sm::label_instance> labels{
      sm::label("operation")(path_desc.operations.nickname)};

    auto aggregate_labels = std::vector<std::string>{
      sm::shard_label.name(), "operation"};
    auto internal_aggregate_labels
      = config::shard_local_cfg().aggregate_metrics()
          ? aggregate_labels
          : std::vector<std::string>{};

    _metrics.add_group(
      "pandaproxy",
      {sm::make_histogram(
        "request_latency",
        sm::description("Request latency"),
        labels,
        [this] { return _request_hist.seastar_histogram_logform(); },
        internal_aggregate_labels)});
}

} // namespace pandaproxy