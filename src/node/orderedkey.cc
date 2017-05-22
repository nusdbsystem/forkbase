// Copyright (c) 2017 The Ustore Authors.

#include "node/orderedkey.h"

#include <cstring>  // for memcpy

namespace ustore {
OrderedKey::OrderedKey(uint64_t value) noexcept
    : by_value_(true), value_(value), slice_() {}

// NOTE: num_bytes_ count the first byte of by_value field
//       data_ points to the first byte of key value, skipping by_value field.
OrderedKey::OrderedKey(bool by_value, const byte_t* data,
                       size_t num_bytes) noexcept
    : by_value_(by_value), slice_(data, num_bytes) {
  // parse by_value from paremeter data
  if (by_value_) {
    // parse value from data_ (class members)
    this->value_ = *(reinterpret_cast<const uint64_t*>(slice_.data()));
  }
}

size_t OrderedKey::Encode(byte_t* buffer) const {
  if (by_value_) {
    std::memcpy(buffer, &value_, sizeof(value_));
  } else {
    std::memcpy(buffer, slice_.data(), slice_.len());
  }
  return numBytes();
}

bool OrderedKey::operator>(const OrderedKey& otherKey) const {
  CHECK_EQ(by_value_, otherKey.by_value_);
  return by_value_ ? value_ > otherKey.value_ : slice_ > otherKey.slice_;
}

bool OrderedKey::operator<(const OrderedKey& otherKey) const {
  CHECK_EQ(by_value_, otherKey.by_value_);
  return by_value_ ? value_ < otherKey.value_ : slice_ < otherKey.slice_;
}

bool OrderedKey::operator==(const OrderedKey& otherKey) const {
  CHECK_EQ(by_value_, otherKey.by_value_);
  return by_value_ ? value_ == otherKey.value_ : slice_ == otherKey.slice_;
}
}  // namespace ustore
