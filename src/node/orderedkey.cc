// Copyright (c) 2017 The Ustore Authors.

#include "node/orderedkey.h"

#include <cstring>  // for memcpy
#include "utils/logging.h"

namespace ustore {

OrderedKey::OrderedKey(uint64_t value)
    : num_bytes_(0), by_value_(true), value_(value), data_(nullptr) {
  // do nothing
}

// NOTE: num_bytes_ count the first byte of by_value field
//       data_ points to the first byte of key value, skipping by_value field.
OrderedKey::OrderedKey(bool by_value, const byte_t* data, size_t num_bytes)
    // skip by_value field (1 Byte)
    : num_bytes_(num_bytes), by_value_(by_value), data_(data) {
  // parse by_value from paremeter data
  if (by_value_) {
    // parse value from data_ (class members)
    this->value_ = *(reinterpret_cast<const uint64_t*>(data_));
  }
}

size_t OrderedKey::encode(byte_t* buffer) const {
  if (by_value_) {
    std::memcpy(buffer, &value_, sizeof(value_));
  } else {
    std::memcpy(buffer, data_, num_bytes_);
  }
  return numBytes();
}

bool OrderedKey::operator>(const OrderedKey& otherKey) const {
  CHECK_EQ(by_value_, otherKey.by_value_);
  if (by_value_) return value_ > otherKey.value_;
  // compare the first min_len bytes of two ordered keys data
  size_t min_len = std::min(num_bytes_, otherKey.num_bytes_);
  int rc = std::memcmp(data_, otherKey.data_, min_len);
  if (rc) return rc > 0;
  // two key data share the same preceding min_len bytes
  // compare their byte length
  return num_bytes_ > otherKey.num_bytes_;
}

bool OrderedKey::operator<(const OrderedKey& otherKey) const {
  CHECK_EQ(by_value_, otherKey.by_value_);
  if (by_value_) return value_ < otherKey.value_;
  // compare the first min_len bytes of two ordered keys data
  size_t min_len = std::min(num_bytes_, otherKey.num_bytes_);
  int rc = std::memcmp(data_, otherKey.data_, min_len);
  if (rc) return rc < 0;
  // two key data share the same preceding min_len bytes
  // compare their byte length
  return num_bytes_ < otherKey.num_bytes_;
}

bool OrderedKey::operator==(const OrderedKey& otherKey) const {
  CHECK_EQ(by_value_, otherKey.by_value_);
  if (by_value_) return value_ == otherKey.value_;
  return num_bytes_ == otherKey.num_bytes_ &&
    !std::memcmp(data_, otherKey.data_, num_bytes_);
}
}  // namespace ustore
