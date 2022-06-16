/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"
#include "utils/hdr_hist.h"

#include <seastar/core/metrics.hh>

namespace kafka {
class latency_probe {
public:
    void setup_metrics() {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }
        std::vector<sm::label_instance> labels{
          sm::label("latency_metric")("microseconds")};
        auto aggregate_labels
          = config::shard_local_cfg().aggregate_metrics()
              ? std::vector<std::string>{sm::shard_label.name()}
              : std::vector<std::string>{};
        _metrics.add_group(
          prometheus_sanitize::metrics_name("kafka:latency"),
          {sm::make_histogram(
             "fetch_latency_us",
             sm::description("Fetch Latency"),
             labels,
             [this] { return _fetch_latency.seastar_histogram_logform(); },
             aggregate_labels),
           sm::make_histogram(
             "produce_latency_us",
             sm::description("Produce Latency"),
             labels,
             [this] { return _produce_latency.seastar_histogram_logform(); },
             aggregate_labels)});

        _metrics.add_group(
          prometheus_sanitize::metrics_name("kafka"),
          {sm::make_histogram(
            "request_latency_seconds",
            sm::description("Internal latency of client request"),
            {sm::label("request")("produce")},
            [this] { return _produce_latency.seastar_histogram_logform(18, 1000, 2.0, 1000000); },
            {sm::shard_label.name()}),
           sm::make_histogram(
            "request_latency_seconds",
            sm::description("Internal latency of client request"),
            {sm::label("request")("consume")},
            [this] { return _produce_latency.seastar_histogram_logform(18, 1000, 2.0, 1000000); },
            {sm::shard_label.name()}),
          },
          sm::impl::default_handle() + 1);
    }

    std::unique_ptr<hdr_hist::measurement> auto_produce_measurement() {
        return _produce_latency.auto_measure();
    }
    std::unique_ptr<hdr_hist::measurement> auto_fetch_measurement() {
        return _fetch_latency.auto_measure();
    }

private:
    hdr_hist _produce_latency;
    hdr_hist _fetch_latency;
    ss::metrics::metric_groups _metrics;
};

} // namespace kafka
