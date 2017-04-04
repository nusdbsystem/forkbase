// Copyright (c) 2017 The Ustore Authors.

#include "hash/hash.h"

#include <cstring>
#include <iostream>
#include <map>
#include <utility>
#include "utils/logging.h"

#ifdef USE_SHA256
#include "hash/sha2.h"
#endif  // USE_SHA256

namespace ustore {

const byte_t Hash::kEmptyBytes[] = {};
const Hash Hash::kNull(kEmptyBytes);

constexpr char base32alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
const std::map<char, byte_t> base32dict = {{'A', 0},
                                           {'B', 1},
                                           {'C', 2},
                                           {'D', 3},
                                           {'E', 4},
                                           {'F', 5},
                                           {'G', 6},
                                           {'H', 7},
                                           {'I', 8},
                                           {'J', 9},
                                           {'K', 10},
                                           {'L', 11},
                                           {'M', 12},
                                           {'N', 13},
                                           {'O', 14},
                                           {'P', 15},
                                           {'Q', 16},
                                           {'R', 17},
                                           {'S', 18},
                                           {'T', 19},
                                           {'U', 20},
                                           {'V', 21},
                                           {'W', 22},
                                           {'X', 23},
                                           {'Y', 24},
                                           {'Z', 25},
                                           {'2', 26},
                                           {'3', 27},
                                           {'4', 28},
                                           {'5', 29},
                                           {'6', 30},
                                           {'7', 31}};

//Hash& Hash::operator=(Hash&& hash) {
//  own_ = std::move(hash.own_);
//  value_ = hash.value_;
//  hash.value_ = nullptr;
//  return *this;
//}
//
//Hash& Hash::operator=(const Hash& hash) {
//  if (this == &hash) return *this;
//  own_.reset();
//  value_ = hash.value_;
//  return *this;
//}

void Hash::CopyFrom(const Hash& hash) {
  Alloc();
  std::memcpy(own_.get(), hash.value_, kByteLength);
}

// caution: this base32 implementation can only used in UStore case,
// it does not process the padding, since UStore's hash value have 20 bytes
// which is a multiplier of 5 bits, so no need of padding.
void Hash::FromBase32(const std::string& base32) {
  CHECK_EQ(kBase32Length, base32.length())
      << "length of input string is not 32 bytes";
  Alloc();
  uint64_t tmp;
  size_t dest = 0;
  for (size_t i = 0; i < kBase32Length; i += 8) {
    tmp = 0;
    for (size_t j = 0; j < 8; ++j) {
      tmp <<= 5;
      tmp += base32dict.at(base32[i + j]);
    }
    for (size_t j = 0; j < 5; ++j) {
      own_[dest + 4 - j] = byte_t(tmp & ((1 << 8) - 1));
      tmp >>= 8;
    }
    dest += 5;
  }
}

std::string Hash::ToBase32() const {
  std::string ret;
  uint64_t tmp;
  for (size_t i = 0; i < kByteLength; i += 5) {
    tmp = 0;
    for (size_t j = 0; j < 5; ++j) tmp = (tmp << 8) + uint64_t(value_[i + j]);
    for (size_t j = 0; j < 8; ++j) {
      ret += base32alphabet[size_t(uint8_t(tmp >> 5 * (7 - j)))];
      tmp &= (uint64_t(1) << 5 * (7 - j)) - 1;
    }
  }
  return ret;
}

Hash Hash::Clone() const {
  Hash hash;
  hash.Alloc();
  std::memcpy(hash.own_.get(), value_, kByteLength);
  return hash;
}

void Hash::Alloc() {
  if (!own_) {
    own_.reset(new byte_t[kByteLength]);
    value_ = own_.get();
  }
}

#ifdef USE_SHA256
void Hash::Compute(const byte_t* data, size_t len) {
  Alloc();
  byte_t fullhash[kBase32Length];
  picosha2::hash256(data, data + len, fullhash, fullhash + kBase32Length);
  std::copy(fullhash, fullhash + kByteLength, own_.get());
}
#endif  // USE_SHA256

}  // namespace ustore
