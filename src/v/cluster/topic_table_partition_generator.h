/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/topic_table.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/generator.hh>

namespace cluster {
struct partition_replicas {
    model::ntp partition;
    std::vector<model::broker_shard> replicas;
};

class topic_table_partition_generator_exception : public std::runtime_error {
public:
    explicit topic_table_partition_generator_exception(const std::string& m)
      : std::runtime_error(m) {}
};

class topic_table_partition_generator {
public:
    using generator_type_t = std::vector<partition_replicas>;

    explicit topic_table_partition_generator(
      ss::sharded<topic_table>& topic_table, size_t batch_size = 256)
      : _topic_table(topic_table)
      , _stable_revision_id(_topic_table.local().last_applied_revision())
      , _batch_size(batch_size) {
        if (_topic_table.local()._topics.empty()) {
            _topic_iterator = _topic_table.local()._topics.end();
            _exhausted = true;
        } else {
            _topic_iterator = _topic_table.local()._topics.begin();
            _partition_iterator = current_assignment_set().begin();
        }
    }

    ss::future<std::optional<generator_type_t>> next_batch() {
        if (_exhausted) {
            co_return std::nullopt;
        }

        const auto current_revision_id
          = _topic_table.local().last_applied_revision();
        if (current_revision_id != _stable_revision_id) {
            throw topic_table_partition_generator_exception(fmt::format(
              "Last applied revision id moved from {} to {} whilst "
              "the generator was active",
              _stable_revision_id,
              current_revision_id));
        }

        generator_type_t batch;
        batch.reserve(_batch_size);

        while (!_exhausted && batch.size() < _batch_size) {
            model::topic_namespace tn = _topic_iterator.value()->first;
            model::partition_id pid = _partition_iterator.value()->id;
            std::vector<model::broker_shard> replicas
              = _partition_iterator.value()->replicas;
            partition_replicas entry{
              .partition = model::ntp{tn.ns, tn.tp, pid},
              .replicas = std::move(replicas)};

            batch.push_back(std::move(entry));

            next();
        }

        co_return batch;
    }

private:
    void next() {
        if (++(*_partition_iterator) == current_assignment_set().end()) {
            if (++(*_topic_iterator) == _topic_table.local()._topics.end()) {
                _exhausted = true;
                return;
            }

            _partition_iterator = current_assignment_set().begin();
        }
    }

    const assignments_set& current_assignment_set() const {
        return (*_topic_iterator)->second.get_assignments();
    }

    ss::sharded<topic_table>& _topic_table;
    model::revision_id _stable_revision_id;
    size_t _batch_size;
    bool _exhausted{false};

    std::optional<topic_table::underlying_t::const_iterator> _topic_iterator;
    std::optional<assignments_set::const_iterator> _partition_iterator;
};
} // namespace cluster
