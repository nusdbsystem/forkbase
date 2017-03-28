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
// TODO(pingcheng): make the additional bool explicit to user
OrderedKey::OrderedKey(const byte_t* data, size_t num_bytes)
    // skip by_value field (1 Byte)
    : num_bytes_(num_bytes), data_(data + sizeof(bool)) {
  // parse by_value from paremeter data
  by_value_ = *(reinterpret_cast<const bool*>(data));
  if (by_value_) {
    // parse value from data_ (class members)
    this->value_ = *(reinterpret_cast<const uint64_t*>(data_));
  }
}

size_t OrderedKey::encode(byte_t* buffer) const {
  *buffer = static_cast<byte_t>(by_value_);
  if (by_value_) {
    std::memcpy(buffer + sizeof(bool), &value_, sizeof(value_));
  } else {
    std::memcpy(buffer + sizeof(bool), data_, num_bytes_ - sizeof(bool));
  }
  return numBytes();
}

bool OrderedKey::operator>(const OrderedKey& otherKey) const {
  CHECK_EQ(by_value_, otherKey.by_value_);
  if (by_value_) return value_ > otherKey.value_;
  // compare the first min_len bytes of two ordered keys data
  // min_len = min(this->num_bytes, otheryKey.num_bytes) - 1;
  size_t min_len = std::min(num_bytes_, otherKey.num_bytes_);
  int rc = std::memcmp(data_, otherKey.data_, min_len - 1);
  if (rc) return rc > 0;
  // two key data share the same preceding min_len bytes
  // compare their byte length
  return num_bytes_ > otherKey.num_bytes_;
}

bool OrderedKey::operator<(const OrderedKey& otherKey) const {
  CHECK_EQ(by_value_, otherKey.by_value_);
  if (by_value_) return value_ < otherKey.value_;
  // compare the first min_len bytes of two ordered keys data
  // min_len = min(this->num_bytes, otheryKey.num_bytes) - 1;
  size_t min_len = std::min(num_bytes_, otherKey.num_bytes_);
  int rc = std::memcmp(data_, otherKey.data_, min_len - 1);
  if (rc) return rc < 0;
  // two key data share the same preceding min_len bytes
  // compare their byte length
  return num_bytes_ < otherKey.num_bytes_;
}

bool OrderedKey::operator==(const OrderedKey& otherKey) const {
  CHECK_EQ(by_value_, otherKey.by_value_);
  if (by_value_) return value_ == otherKey.value_;
  return num_bytes_ == otherKey.num_bytes_ &&
    !std::memcmp(data_, otherKey.data_, num_bytes_ - 1);
}
}  // namespace ustore
