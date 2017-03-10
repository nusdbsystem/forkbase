// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HASH_HASH_H_
#define USTORE_HASH_HASH_H_

#include <string>
#include "types/type.h"

namespace ustore {

const size_t HASH_BYTE_LEN = 20;
const size_t HASH_STRING_LEN = 32;

class Hash {
 public:
  // create empty hash
  Hash() {};
  // use existing byte array
  explicit Hash(const byte_t* hash);
  ~Hash();

  void operator=(const Hash& hash);
  bool operator<(const Hash& hash) const;
  bool operator<=(const Hash& hash) const;
  bool operator>(const Hash& hash) const;
  bool operator>=(const Hash& hash) const;
  bool operator==(const Hash& hash) const;
  bool operator!=(const Hash& hash) const;

  // check if the hash is empty
  inline bool empty() { return value_ == nullptr; }
  // expose byte array to others
  inline const byte_t* value() const { return value_; }
  // decode hash from base32 format
  // if do so, must allocate own value
  void FromString(const std::string& base32);
  // compute hash from data
  // if do so, must allocate own value
  virtual void Compute(const byte_t* data, size_t len) = 0;
  // encode to base32 format
  std::string ToString();

 protected:
  bool own_ = false;
  // big-endian
  byte_t* value_ = nullptr;
};

}  // namespace ustore
#endif  // USTORE_HASH_HASH_H_
