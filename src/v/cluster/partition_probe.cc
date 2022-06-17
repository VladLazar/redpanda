// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition_probe.h"

#include "cluster/partition.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cluster {

replicated_partition_probe::replicated_partition_probe(
  const partition& p) noexcept
  : _partition(p) {}

void replicated_partition_probe::setup_metrics(const model::ntp& ntp) {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    auto request_label = sm::label("request");
    auto ns_label = sm::label("namespace");
    auto topic_label = sm::label("topic");
    auto partition_label = sm::label("partition");
    auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                              ? std::vector<std::string>{sm::shard_label.name()}
                              : std::vector<std::string>{};

    const std::vector<sm::label_instance> labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };

    _metrics.add_group(
      prometheus_sanitize::metrics_name("kafka"),
      {
        // Partition Level Metrics
        sm::make_gauge(
          "max_offset",
          [this] { 
            auto log_offset = _partition.committed_offset();
            auto translator = _partition.get_offset_translator_state();

            return translator->from_log_offset(log_offset);
          },
          sm::description("Partition max offset"),
          labels,
          aggregate_labels),
        sm::make_gauge(
          "under_replicated_replicas",
          [this] {
              auto metrics = _partition._raft->get_follower_metrics();
              return std::count_if(
                metrics.cbegin(),
                metrics.cend(),
                [](const raft::follower_metrics& fm) {
                    return fm.under_replicated;
                });
          },
          sm::description("Number of under replicated replicas"),
          labels,
          aggregate_labels),
        // Topic Level Metrics
        sm::make_total_bytes(
          "request_bytes_total",
          [this] { return _bytes_produced; },
          sm::description("Total number of bytes produced per topic"),
          {request_label("produce"),
           ns_label(ntp.ns()),
           topic_label(ntp.tp.topic()),
           partition_label(ntp.tp.partition())},
          sm::impl::shard(),
          {sm::shard_label.name(), partition_label.name()}),
        sm::make_total_bytes(
          "request_bytes_total",
          [this] { return _bytes_fetched; },
          sm::description("Total number of bytes consumed per topic"),
          {request_label("consume"),
           ns_label(ntp.ns()),
           topic_label(ntp.tp.topic()),
           partition_label(ntp.tp.partition())},
          sm::impl::shard(),
          {sm::shard_label.name(), partition_label.name()}),
        sm::make_gauge(
          "replicas",
          [this] {
              auto metrics = _partition._raft->get_follower_metrics();
              return metrics.size();
          },
          sm::description("Number of replicas"),
          {ns_label(ntp.ns()), topic_label(ntp.tp.topic()), partition_label(ntp.tp.partition())},
          {sm::shard_label.name(), partition_label.name()}),
        //sm::make_gauge(
        //  "partitions",
        //  [this] {
        //      auto metrics = _partition._raft->get_follower_metrics();
        //      return metrics.size();
        //  },
        //  sm::description("Number of partitions"),
        //  {ns_label(ntp.ns()), topic_label(ntp.tp.topic())},
        //  {sm::shard_label.name(), partition_label.name()}),
        // Broker Level Metrics
        sm::make_total_bytes(
          "request_bytes_per_broker",
          [this] { return _bytes_produced; },
          sm::description("Total number of bytes produced"),
          {request_label("produce"),
           ns_label(ntp.ns()),
           topic_label(ntp.tp.topic())},
          sm::impl::shard(),
          {sm::shard_label.name(), partition_label.name(), topic_label.name(), ns_label.name()}),
        sm::make_total_bytes(
          "request_bytes_total_per_broker",
          [this] { return _bytes_fetched; },
          sm::description("Total number of bytes consumed"),
          {request_label("consume"),
           ns_label(ntp.ns()),
           topic_label(ntp.tp.topic())},
          sm::impl::shard(),
          {sm::shard_label.name(), partition_label.name(), topic_label.name(), ns_label.name()}),
      },
      sm::impl::default_handle() + 1);

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cluster:partition"),
      {
        sm::make_gauge(
          "leader",
          [this] { return _partition.is_elected_leader() ? 1 : 0; },
          sm::description(
            "Flag indicating if this partition instance is a leader"),
          labels,
          aggregate_labels),
        sm::make_gauge(
          "last_stable_offset",
          [this] { return _partition.last_stable_offset(); },
          sm::description("Last stable offset"),
          labels,
          aggregate_labels),
        sm::make_gauge(
          "committed_offset",
          [this] { return _partition.committed_offset(); },
          sm::description("Partition commited offset. i.e. safely persisted on "
                          "majority of replicas"),
          labels,
          aggregate_labels),
        sm::make_gauge(
          "end_offset",
          [this] { return _partition.dirty_offset(); },
          sm::description(
            "Last offset stored by current partition on this node"),
          labels,
          aggregate_labels),
        sm::make_gauge(
          "high_watermark",
          [this] { return _partition.high_watermark(); },
          sm::description(
            "Partion high watermark i.e. highest consumable offset"),
          labels,
          aggregate_labels),
        sm::make_gauge(
          "leader_id",
          [this] {
              return _partition._raft->get_leader_id().value_or(
                model::node_id(-1));
          },
          sm::description("Id of current partition leader"),
          labels,
          aggregate_labels),
        sm::make_gauge(
          "under_replicated_replicas",
          [this] {
              auto metrics = _partition._raft->get_follower_metrics();
              return std::count_if(
                metrics.cbegin(),
                metrics.cend(),
                [](const raft::follower_metrics& fm) {
                    return fm.under_replicated;
                });
          },
          sm::description("Number of under replicated replicas"),
          labels,
          aggregate_labels),
        sm::make_counter(
          "records_produced",
          [this] { return _records_produced; },
          sm::description("Total number of records produced"),
          labels,
          aggregate_labels),
        sm::make_counter(
          "records_fetched",
          [this] { return _records_fetched; },
          sm::description("Total number of records fetched"),
          labels,
          aggregate_labels),
        sm::make_total_bytes(
          "bytes_produced_total",
          [this] { return _bytes_produced; },
          sm::description("Total number of bytes produced"),
          labels,
          sm::impl::shard(),
          aggregate_labels),
        sm::make_total_bytes(
          "bytes_fetched_total",
          [this] { return _bytes_fetched; },
          sm::description("Total number of bytes fetched"),
          labels,
          sm::impl::shard(),
          aggregate_labels),
      });
}
partition_probe make_materialized_partition_probe() {
    // TODO: implement partition probe for materialized partitions
    class impl : public partition_probe::impl {
        void setup_metrics(const model::ntp&) final {}
        void add_records_fetched(uint64_t) final {}
        void add_records_produced(uint64_t) final {}
        void add_bytes_fetched(uint64_t) final {}
        void add_bytes_produced(uint64_t) final {}
    };
    return partition_probe(std::make_unique<impl>());
}
} // namespace cluster
