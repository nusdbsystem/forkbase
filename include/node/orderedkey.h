// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_ORDEREDKEY_H_
#define USTORE_NODE_ORDEREDKEY_H_

#include <cstddef>

#include "spec/slice.h"
#include "types/type.h"
#include "utils/logging.h"

namespace ustore {
// TODO(pingcheng): Later make OrdreedKey contain a slice as member
//   and use slice for comparison if not by value
class OrderedKey {
  /* OrderedKey can either be a hash value (byte array) or an uint64_t integer

   Encoding Scheme by OrderedKey (variable size)
   |-----hash value/uint64 |
   0  -- variable size
 */
 public:
  inline static OrderedKey FromSlice(const Slice& key) {
    return OrderedKey(false,
                      reinterpret_cast<const byte_t*>(key.data()),
                      key.len());
  }
  // Set an integer value for key
  // own set to false
  OrderedKey() {}
  explicit OrderedKey(uint64_t value);
  // Set the hash data for key
  OrderedKey(bool by_value, const byte_t* data, size_t num_bytes);
  OrderedKey(const OrderedKey& key) = default;
  ~OrderedKey() {}

  OrderedKey& operator=(const OrderedKey& key) = default;

  inline const byte_t* data() const { return data_; }
  inline size_t numBytes() const {
    return by_value_ ? sizeof(uint64_t) : num_bytes_;
  }
  // encode OrderedKey into buffer
  // given buffer capacity > numBytes
  size_t Encode(byte_t* buffer) const;
  inline bool byValue() const { return by_value_; }

  bool operator>(const OrderedKey& otherKey) const;
  bool operator<(const OrderedKey& otherKey) const;
  bool operator==(const OrderedKey& otherKey) const;
  inline bool operator<=(const OrderedKey& otherKey) const {
    return *this < otherKey || *this == otherKey;
  }
  inline bool operator>=(const OrderedKey& otherKey) const {
    return *this > otherKey || *this == otherKey;
  }

  inline Slice ToSlice() const {
    CHECK(!by_value_);
    return Slice(reinterpret_cast<const char*>(data_),
                 num_bytes_);
  }

 private:
  size_t num_bytes_;  // number of bytes of data
  // Parse the data as a number to compare
  // Otherwise as hash value
  bool by_value_;
  uint64_t value_;
  const byte_t* data_;  // data bytes
};

}  // namespace ustore

#endif  // USTORE_NODE_ORDEREDKEY_H_
