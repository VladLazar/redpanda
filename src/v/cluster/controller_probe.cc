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

#include "cluster/controller_probe.h"

#include "cluster/controller.h"
#include "cluster/health_monitor_backend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/members_table.h"
#include "prometheus/prometheus_sanitize.h"

#include <absl/container/flat_hash_set.h>
#include <seastar/core/metrics.hh>

namespace cluster {

controller_probe::controller_probe(controller& c) noexcept : _controller(c) {
    _controller._raft_manager.local()
        .register_leadership_notification([this](
            raft::group_id group,
            model::term_id /*term*/,
            std::optional<model::node_id> leader_id) {

        // We are only interested in notifications regarding the controller group.
        if (_controller._raft0->group() != group) {
            return;
        }

        if (!leader_id.has_value() || leader_id.value() != _controller.self()) {
            _metrics.reset();
        } else {
            setup_metrics();
        }
    });
}

void controller_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics = std::make_unique<ss::metrics::metric_groups>();
    _metrics->add_group(
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
                    const auto& leaders_table = _controller._partition_leaders.local();
                    auto leaders = leaders_table.get_leaders();

                    return leaders.size();
                },
                sm::description("Number of partitions in the cluster"),
                {},
                {sm::shard_label.name()}),
            sm::make_gauge(
                "unavailable_partitions",
                [this] {
                    const auto& leaders_table = _controller._partition_leaders.local();
                    auto leaders = leaders_table.get_leaders();
                    
                    return std::count_if(leaders.begin(), leaders.end(),
                        [](const auto& leader_info) {
                            return !leader_info.current_leader.has_value();
                    });
                },
                sm::description(
                    "Number of partitions that lack quorum among replicants"),
                {},
                {sm::shard_label.name()}),
        },
      sm::impl::default_handle() + 1);
}

}
