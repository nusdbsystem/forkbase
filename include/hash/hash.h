// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HASH_HASH_H_
#define USTORE_HASH_HASH_H_

#include <string>
#include "types/type.h"

namespace ustore {

// TODO(wangsh): move class-related const into classes
constexpr size_t HASH_BYTE_LEN = 20;
constexpr size_t HASH_STRING_LEN = 32;

class Hash {
 public:
  // the hash used to represent null value
  static const Hash NULL_HASH;
  // create empty hash
  Hash() {}
  // use existing hash
  Hash(const Hash& hash);
  // use existing byte array
  explicit Hash(const byte_t* hash);
  ~Hash();

  Hash& operator=(const Hash& hash);
  bool operator<(const Hash& hash) const;
  bool operator<=(const Hash& hash) const;
  bool operator>(const Hash& hash) const;
  bool operator>=(const Hash& hash) const;
  bool operator==(const Hash& hash) const;
  bool operator!=(const Hash& hash) const;

  // check if the hash is empty
  inline bool empty() const { return value_ == nullptr; }
  // expose byte array to others
  inline const byte_t* value() const { return value_; }
  // copy content from another
  void CopyFrom(const Hash& hash);
  // decode hash from base32 format
  // if do so, must allocate own value
  void FromString(const std::string& base32);
  // compute hash from data
  // if do so, must allocate own value
  void Compute(const byte_t* data, size_t len);
  // encode to base32 format
  std::string ToString() const;

 private:
  // allocate space if previously not have
  void Alloc();

  bool own_ = false;
  // big-endian
  byte_t* value_ = nullptr;

  static const byte_t EMPTY_HASH_DATA[HASH_BYTE_LEN];
};

}  // namespace ustore
#endif  // USTORE_HASH_HASH_H_
