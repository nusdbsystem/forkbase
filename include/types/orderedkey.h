// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_ORDEREDKEY_H_
#define USTORE_TYPES_ORDEREDKEY_H_

#include <cstddef>
#include <cstdint>

#include "types/type.h"

namespace ustore {

class OrderedKey {
 /* OrderedKey can either be a hash value (byte array) or an uint64_t integer

  Encoding Scheme by OrderedKey (variable size)
  |--by_value--|-----hash value/uint64 |
  0 -----------1  -- variable size
*/
 public:
  // Set the hash data for key
  OrderedKey(const byte* data, size_t num_bytes, bool own);

  // Set an integer value for key
  // own set to false
  explicit OrderedKey(uint64_t value);

  // Delete the data if own is true
  ~OrderedKey();

  friend bool operator>(const OrderedKey& lhs, const OrderedKey& rhs);

  friend bool operator<(const OrderedKey& lhs, const OrderedKey& rhs);

  friend bool operator==(const OrderedKey& lhs, const OrderedKey& rhs);

  friend inline bool operator<=(const OrderedKey& lhs, const OrderedKey& rhs) {
    return lhs < rhs || lhs == rhs;
  }

  friend inline bool operator>=(const OrderedKey& lhs, const OrderedKey& rhs) {
    return lhs > rhs || lhs == rhs;
  }

 private:
  const bool own;  // whether ordered key is repsonsible to clear the data

  const size_t num_bytes;  // number of bytes of data

  // Parse the data as a number to compare
  // Otherwise as hash value
  const bool by_value;
  const uint64_t value_;
  const byte* data;  // data bytes
};

}  // namespace ustore

#endif  // USTORE_TYPES_ORDEREDKEY_H_
