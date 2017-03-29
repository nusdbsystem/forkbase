// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HASH_HASH_H_
#define USTORE_HASH_HASH_H_

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

 static const size_t HASH_BYTE_LEN = 20;
 static const size_t HASH_STRING_LEN = 32;
  // create empty hash
  Hash() {}
  // use existing hash
  Hash(const Hash& hash) noexcept : own_(false), value_(hash.value_) {}
  // move ctor
  Hash(Hash&& hash) noexcept : Hash(hash)  {
      std::swap(own_, hash.own_);
  }
  // use existing byte array
  explicit Hash(byte_t* hash) noexcept : own_(false), value_(hash) {}

  ~Hash() {
      if (own_) delete[] value_;
  }

  inline Hash& operator=(Hash hash);
  inline friend bool operator<(const Hash& lhs, const Hash& rhs);
  inline friend bool operator<=(const Hash& lhs, const Hash& rhs);
  inline friend bool operator>(const Hash& lhs, const Hash& rhs);
  inline friend bool operator>=(const Hash& lhs, const Hash& rhs);
  inline friend bool operator==(const Hash& lhs, const Hash& rhs);
  inline friend bool operator!=(const Hash& lhs, const Hash& rhs);

  // check if the hash is empty
  inline bool empty() { return value_ == nullptr; }
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

};

}  // namespace ustore
#endif  // USTORE_HASH_HASH_H_
