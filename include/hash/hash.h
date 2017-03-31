// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HASH_HASH_H_
#define USTORE_HASH_HASH_H_

#include <memory>
#include <string>
#include "types/type.h"

namespace ustore {

class Hash {
 private:
  // allocate space if previously not have
  void Alloc();

  bool own_ = false;
  // big-endian
  byte_t* value_ = nullptr;

 public:

  static constexpr size_t kByteLength = 20;
  static constexpr size_t kBase32Length = 32;
  // the hash used to represent null value
  static const Hash kNull;

  // TODO(wangsh): consider if need make default hash value as kNull
  // TODO(wangsh): but will make uninitialized hash and kNull indistinguishable
  Hash() {}
  // use existing hash
  Hash(const Hash& hash) : value_(hash.value_) {}
  // use existing byte array
  explicit Hash(const byte_t* hash) : value_(hash) {}
  ~Hash() {}

  // movable
  Hash(Hash&& hash);
  Hash& operator=(Hash&& hash);


  inline Hash& operator=(Hash hash);
  inline friend bool operator<(const Hash& lhs, const Hash& rhs);
  inline friend bool operator<=(const Hash& lhs, const Hash& rhs);
  inline friend bool operator>(const Hash& lhs, const Hash& rhs);
  inline friend bool operator>=(const Hash& lhs, const Hash& rhs);
  inline friend bool operator==(const Hash& lhs, const Hash& rhs);
  inline friend bool operator!=(const Hash& lhs, const Hash& rhs);

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
#endif  // USTORE_HASH_HASH_H_
