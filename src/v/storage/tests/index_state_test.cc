#define BOOST_TEST_MODULE storage
#include "bytes/bytes.h"
#include "random/generators.h"
#include "serde/serde.h"
#include "storage/index_state.h"
#include "storage/index_state_serde_compat.h"

#include <boost/test/unit_test.hpp>

static storage::index_state make_random_index_state(
  storage::offset_delta_time apply_offset = storage::offset_delta_time::yes) {
    storage::index_state st;
    st.bitflags = random_generators::get_int<uint32_t>();
    st.base_offset = model::offset(random_generators::get_int<int64_t>());
    st.max_offset = model::offset(random_generators::get_int<int64_t>());
    st.base_timestamp = model::timestamp(random_generators::get_int<int64_t>());
    st.max_timestamp = model::timestamp(random_generators::get_int<int64_t>());
    st.batch_timestamps_are_monotonic = apply_offset
                                        == storage::offset_delta_time::yes;

    const auto n = random_generators::get_int(1, 10000);
    for (auto i = 0; i < n; ++i) {
        st.add_entry(
          random_generators::get_int<uint32_t>(),
          storage::offset_time_index{
            model::timestamp{random_generators::get_int<int64_t>()},
            apply_offset},
          random_generators::get_int<uint64_t>());
    }

    if (apply_offset == storage::offset_delta_time::no) {
        fragmented_vector<uint32_t> time_index;
        for (auto i = 0; i < n; ++i) {
            time_index.push_back(random_generators::get_int<uint32_t>());
        }

        std::swap(st.relative_time_index, time_index);
    }

    return st;
}

static bool compare_before_and_after_time_ofsetting(
  const storage::index_state& before, const storage::index_state& after) {
    bool equal = before.bitflags == after.bitflags
                 && before.base_offset == after.base_offset
                 && before.max_offset == after.max_offset
                 && before.base_timestamp == after.base_timestamp
                 && before.max_timestamp == after.max_timestamp
                 && before.relative_offset_index == after.relative_offset_index
                 && before.position_index == after.position_index;

    if (!equal) {
        return equal;
    }

    equal = equal
            && before.relative_time_index.size()
                 == after.relative_time_index.size();

    // Before ofsetting: [0, ..., 2 ^ 31 - 1, ..., 2 ^ 32 - 1]
    //                    |             |              |
    //                    |             |______________|
    //                    |             |
    // After ofsetting:  [2^31, ..., 2 ^ 32 - 1]
    uint32_t offset = storage::offset_time_index::offset;
    uint32_t max_delta = storage::offset_time_index::delta_time_max;
    for (size_t i = 0; i < before.relative_time_index.size(); ++i) {
        uint32_t delta_before = before.relative_time_index[i];
        uint32_t delta_after = after.relative_time_index[i];

        if (delta_before >= max_delta) {
            equal = equal
                    && delta_after == std::numeric_limits<uint32_t>::max();
        } else {
            equal = equal && delta_after == delta_before + offset;
        }
    }

    return equal;
}

static void set_version(iobuf& buf, int8_t version) {
    auto tmp = iobuf_to_bytes(buf);
    buf.clear();
    buf.append((const char*)&version, sizeof(version));
    buf.append(bytes_to_iobuf(tmp.substr(1)));
}

// encode/decode using new serde framework
BOOST_AUTO_TEST_CASE(serde_basic) {
    for (int i = 0; i < 100; ++i) {
        auto input = make_random_index_state();
        const auto input_copy = input.copy();
        BOOST_REQUIRE_EQUAL(input, input_copy);

        // objects are equal
        const auto buf = serde::to_iobuf(std::move(input));
        auto output = serde::from_iobuf<storage::index_state>(buf.copy());
        BOOST_REQUIRE_EQUAL(output, input_copy);

        // round trip back to equal iobufs
        const auto buf2 = serde::to_iobuf(std::move(output));
        BOOST_REQUIRE_EQUAL(buf, buf2);
    }
}

BOOST_AUTO_TEST_CASE(serde_before_and_after_time_ofsetting) {
    for (int i = 0; i < 100; ++i) {
        // Create index without time ofsetting
        auto input = make_random_index_state(storage::offset_delta_time::no);
        const auto input_copy = input.copy();
        auto buf = serde::to_iobuf(std::move(input));
        set_version(buf, 4);

        // Read the index and apply time ofsetting
        auto output = serde::from_iobuf<storage::index_state>(buf.copy());
        auto output_copy = output.copy();

        BOOST_REQUIRE(
          compare_before_and_after_time_ofsetting(input_copy, output));

        // Re-encode with version 5 and verify
        const auto buf2 = serde::to_iobuf(std::move(output));
        auto output2 = serde::from_iobuf<storage::index_state>(buf2.copy());
        BOOST_REQUIRE_EQUAL(output_copy, output2);
    }
}

// accept decoding supported old version
BOOST_AUTO_TEST_CASE(serde_supported_deprecated) {
    for (int i = 0; i < 100; ++i) {
        auto input = make_random_index_state(storage::offset_delta_time::no);
        const auto output = serde::from_iobuf<storage::index_state>(
          storage::serde_compat::index_state_serde::encode(input));

        BOOST_REQUIRE(compare_before_and_after_time_ofsetting(input, output));
    }
}

// reject decoding unsupported old versions
BOOST_AUTO_TEST_CASE(serde_unsupported_deprecated) {
    auto test = [](int version) {
        auto input = make_random_index_state(storage::offset_delta_time::no);
        auto buf = storage::serde_compat::index_state_serde::encode(input);
        set_version(buf, version);

        BOOST_REQUIRE_EXCEPTION(
          const auto output = serde::from_iobuf<storage::index_state>(
            buf.copy()),
          serde::serde_exception,
          [version](const serde::serde_exception& e) {
              auto s = fmt::format("Unsupported version: {}", version);
              return std::string_view(e.what()).find(s)
                     != std::string_view::npos;
          });
    };
    test(0);
    test(1);
    test(2);
}

// decoding should fail if all the data isn't available
BOOST_AUTO_TEST_CASE(serde_clipped) {
    auto input = make_random_index_state(storage::offset_delta_time::no);
    auto buf = serde::to_iobuf(std::move(input));

    // trim off some data from the end
    BOOST_REQUIRE_GT(buf.size_bytes(), 10);
    buf.trim_back(buf.size_bytes() - 10);

    BOOST_REQUIRE_EXCEPTION(
      serde::from_iobuf<storage::index_state>(buf.copy()),
      serde::serde_exception,
      [](const serde::serde_exception& e) {
          return std::string_view(e.what()).find("bytes_left")
                 != std::string_view::npos;
      });
}

// decoding deprecated format should fail if not all data is available
BOOST_AUTO_TEST_CASE(serde_deprecated_clipped) {
    auto input = make_random_index_state(storage::offset_delta_time::no);
    auto buf = storage::serde_compat::index_state_serde::encode(input);

    // trim off some data from the end
    BOOST_REQUIRE_GT(buf.size_bytes(), 10);
    buf.trim_back(buf.size_bytes() - 10);

    BOOST_REQUIRE_EXCEPTION(
      serde::from_iobuf<storage::index_state>(buf.copy()),
      serde::serde_exception,
      [](const serde::serde_exception& e) {
          return std::string_view(e.what()).find(
                   "Index size does not match header size")
                 != std::string_view::npos;
      });
}

BOOST_AUTO_TEST_CASE(serde_crc) {
    auto input = make_random_index_state();
    auto good_buf = serde::to_iobuf(std::move(input));

    auto bad_bytes = iobuf_to_bytes(good_buf);
    auto& bad_byte = bad_bytes[bad_bytes.size() / 2];
    bad_byte += 1;
    auto bad_buf = bytes_to_iobuf(bad_bytes);

    BOOST_REQUIRE_EXCEPTION(
      serde::from_iobuf<storage::index_state>(bad_buf.copy()),
      serde::serde_exception,
      [](const serde::serde_exception& e) {
          return std::string_view(e.what()).find("Mismatched checksum")
                 != std::string_view::npos;
      });
}

BOOST_AUTO_TEST_CASE(serde_deprecated_crc) {
    auto input = make_random_index_state(storage::offset_delta_time::no);
    auto good_buf = storage::serde_compat::index_state_serde::encode(input);

    auto bad_bytes = iobuf_to_bytes(good_buf);
    auto& bad_byte = bad_bytes[bad_bytes.size() / 2];
    bad_byte += 1;
    auto bad_buf = bytes_to_iobuf(bad_bytes);

    BOOST_REQUIRE_EXCEPTION(
      serde::from_iobuf<storage::index_state>(bad_buf.copy()),
      std::exception,
      [](const std::exception& e) {
          auto msg = std::string_view(e.what());
          auto is_crc = msg.find("Invalid checksum for index")
                        != std::string_view::npos;
          auto is_out_of_bounds = msg.find("Invalid consume_to")
                                  != std::string_view::npos;
          return is_crc || is_out_of_bounds;
      });
}
