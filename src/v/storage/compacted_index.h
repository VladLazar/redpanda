/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/bytes.h"
#include "model/fundamental.h"

#include <cstddef>
#include <cstdint>
#include <ostream>

namespace storage {
// simple types shared among readers and writers

/**
 * Type representing a record key prefixed with batch_type
 */
struct compaction_key : bytes {
    explicit compaction_key(bytes b)
      : bytes(std::move(b)) {}
};

struct compacted_index {
    static constexpr const size_t max_entry_size = size_t(
      std::numeric_limits<uint16_t>::max());

    enum class entry_type : uint8_t {
        none, // error detection
        key,  // most common - just keys
        /// \brief because of raft truncations, we write a truncation, for
        /// the recovery thread to compact up to key-point on the index.
        truncation,
    };
    // bitflags for index
    enum class footer_flags : uint32_t {
        none = 0,
        /// needed for truncation events in the same raft-term
        truncation = 1U,
        /// needed to determine if we should self compact first
        self_compaction = 1U << 1U,
    };
    struct footer {
        uint32_t size{0};
        uint32_t keys{0};
        footer_flags flags{0};
        uint32_t crc{0}; // crc32
        // version *must* be the last value
        int8_t version{0};

        friend std::ostream&
        operator<<(std::ostream& o, const compacted_index::footer& f) {
            return o << "{size:" << f.size << ", keys:" << f.keys
                     << ", flags:" << (uint32_t)f.flags << ", crc:" << f.crc
                     << ", version: " << (int)f.version << "}";
        }
    };
    enum class recovery_state {
        /**
         * Index may be missing when either was deleted or not stored when
         * redpanda crashed
         */
        index_missing,
        /**
         * Index may needs a rebuild when it is corrupted
         */
        index_needs_rebuild,
        /**
         * Segment is already compacted
         */
        already_compacted,
        /**
         * Compaction index is recovered, ready to compaction
         */
        index_recovered
    };
    static constexpr size_t footer_size = sizeof(footer::size)
                                          + sizeof(footer::keys)
                                          + sizeof(footer::flags)
                                          + sizeof(footer::crc)
                                          + sizeof(footer::version);
    // for the readers and friends
    struct entry {
        entry(entry_type t, bytes k, model::offset o, int32_t d) noexcept
          : type(t)
          , key(std::move(k))
          , offset(o)
          , delta(d) {}

        entry_type type;
        bytes key;
        model::offset offset;
        int32_t delta;
    };
};

std::ostream& operator<<(std::ostream&, compacted_index::recovery_state);

[[gnu::always_inline]] inline compacted_index::footer_flags
operator|(compacted_index::footer_flags a, compacted_index::footer_flags b) {
    return compacted_index::footer_flags(
      std::underlying_type_t<compacted_index::footer_flags>(a)
      | std::underlying_type_t<compacted_index::footer_flags>(b));
}

[[gnu::always_inline]] inline void
operator|=(compacted_index::footer_flags& a, compacted_index::footer_flags b) {
    a = (a | b);
}

[[gnu::always_inline]] inline compacted_index::footer_flags
operator~(compacted_index::footer_flags a) {
    return compacted_index::footer_flags(
      ~std::underlying_type_t<compacted_index::footer_flags>(a));
}

[[gnu::always_inline]] inline compacted_index::footer_flags
operator&(compacted_index::footer_flags a, compacted_index::footer_flags b) {
    return compacted_index::footer_flags(
      std::underlying_type_t<compacted_index::footer_flags>(a)
      & std::underlying_type_t<compacted_index::footer_flags>(b));
}

[[gnu::always_inline]] inline void
operator&=(compacted_index::footer_flags& a, compacted_index::footer_flags b) {
    a = (a & b);
}

} // namespace storage
