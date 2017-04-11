// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HASH_HASH_H_
#define USTORE_HASH_HASH_H_

#include <cstring>
#include <iostream>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include "hash/murmurhash_ustore.hpp"
#include "types/type.h"
#include "utils/logging.h"

namespace ustore {

class Hash {
 public:
  static constexpr size_t kByteLength = 20;
  static constexpr size_t kBase32Length = 32;
  // the hash used to represent null value
  static const Hash kNull;

  // TODO(wangsh): consider if need make default hash value as kNull
  // TODO(wangsh): but will make uninitialized hash and kNull indistinguishable
  inline Hash() {}
  // use existing hash
  inline Hash(const Hash& hash) noexcept : value_(hash.value_) {}
  // use existing byte array
  explicit inline Hash(const byte_t* hash) noexcept : value_(hash) {}
  inline ~Hash() {}

  // movable
  inline Hash(Hash&& hash) noexcept : own_(std::move(hash.own_)) {
    std::swap(value_, hash.value_);
  }

  // copy and move assignment
  inline Hash& operator=(Hash hash) noexcept {
      own_.swap(hash.own_);
      std::swap(value_, hash.value_);
      return *this;
  }

  friend inline bool operator<(const Hash& lhs, const Hash& rhs) noexcept {
      CHECK(lhs.value_ != nullptr && rhs.value_ != nullptr);
      return std::memcmp(lhs.value_, rhs.value_, Hash::kByteLength) < 0;
  }

  friend inline bool operator>(const Hash& lhs, const Hash& rhs) noexcept {
      CHECK(lhs.value_ != nullptr && rhs.value_ != nullptr);
      return std::memcmp(lhs.value_, rhs.value_, Hash::kByteLength) > 0;
  }

  friend inline bool operator==(const Hash& lhs, const Hash& rhs) noexcept {
      CHECK(lhs.value_ != nullptr && rhs.value_ != nullptr);
      return std::memcmp(lhs.value_, rhs.value_, Hash::kByteLength) == 0;
  }

  friend inline bool operator<=(const Hash& lhs, const Hash& rhs) noexcept {
      return !operator>(lhs, rhs);
  }

  friend inline bool operator>=(const Hash& lhs, const Hash& rhs) noexcept {
      return !operator<(lhs, rhs);
  }

  friend inline bool operator!=(const Hash& lhs, const Hash& rhs) noexcept {
      return !operator==(lhs, rhs);
  }

  // check if the hash is empty
  inline bool empty() const { return value_ == nullptr; }
  // expose byte array to others
  inline const byte_t* value() const { return value_; }
  // copy content from another
  void CopyFrom(const Hash& hash);
  // decode hash from base32 format
  // if do so, must allocate own value
  void FromBase32(const std::string& base32);
  // compute hash from data
  // if do so, must allocate own value
  void Compute(const byte_t* data, size_t len);
  // encode to base32 format
  std::string ToBase32() const;
  // get a copy that contains own bytes
  Hash Clone() const;

  friend std::ostream& operator<<(std::ostream &, const Hash &);

 private:
  static const byte_t kEmptyBytes[kByteLength];

  // allocate space if previously not have
  void Alloc();

  // hash own the value if is calculated by itself
  std::unique_ptr<byte_t[]> own_;
  // otherwise the pointer should be read-only
  // big-endian
  const byte_t* value_ = nullptr;
};

}  // namespace ustore

namespace std {
template<>
struct hash<ustore::Hash> {
  inline size_t operator()(const ::ustore::Hash & obj) const {
    const auto & hval = obj.value();
    size_t ret;
    std::copy(hval, hval + sizeof(ret), reinterpret_cast<::ustore::byte_t*>(&ret));
    return ret;
  }
};
} // namespace std

#endif  // USTORE_HASH_HASH_H_
