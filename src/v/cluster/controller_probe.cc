/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/controller.h"
#include "cluster/controller_probe.h"
#include "cluster/health_monitor_backend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/members_table.h"
#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"

#include <absl/container/flat_hash_set.h>

#include <seastar/core/metrics.hh>

namespace cluster {

controller_probe::controller_probe(controller& c) noexcept : _controller(c) {}

void controller_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
        prometheus_sanitize::metrics_name("cluster"),
        {
            sm::make_gauge(
                "brokers",
                [this] {
                    const auto& members_table = _controller.get_members_table().local();
                    return members_table.all_broker_ids().size();
                },
                sm::description("Number of brokers in the cluster"),
                {},
                {sm::shard_label.name()}),
            sm::make_gauge(
                "topics",
                [this] {
                    auto& health_monitor = _controller._hm_backend.local();
                    const auto& report = 
                        health_monitor.get_current_cluster_health_snapshot({});

                    absl::flat_hash_set<model::topic_namespace> unique_topics;
                    for (const auto& node_report: report.node_reports) {
                        for (const auto& topic_status: node_report.topics) {
                            unique_topics.emplace(topic_status.tp_ns);
                        }
                    }

                    return unique_topics.size();
                },
                sm::description("Number of topics in the cluster"),
                {},
                {sm::shard_label.name()}),
            sm::make_gauge(
                "partitions",
                [this] { 
                    const auto& topic_table = _controller.get_topics_state().local();
                    const auto& metadata = topic_table.all_topics_metadata();

                    return std::reduce(metadata.begin(), metadata.end(), 0,
            [](auto acc, const auto& topic_metadata) {
                auto count = topic_metadata.second.get_configuration().partition_count;
                return acc + count;
            });
                },
                sm::description("Number of partitions in the cluster"),
                {},
                {sm::shard_label.name()}),
        },
      sm::impl::default_handle() + 1);
}

}
