// Copyright (c) 2017 The Ustore Authors.


#include <cstring>
#include <iostream>
#include <map>
#include "hash/hash.h"
#include "utils/logging.h"

#ifdef USE_SHA256 
#include "hash/sha2.h"
#endif  // USE_SHA256

namespace ustore {

const char base32alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
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

Hash::Hash(const byte_t* hash) { value_ = const_cast<byte_t*>(hash); }
Hash::Hash(const Hash& hash) { value_ = hash.value_; }
Hash::~Hash() {
  if (own_) delete[] value_;
}
void Hash::operator=(const Hash& hash) {
  if (own_) delete[] value_;
  own_ = false;
  value_ = hash.value_;
}

bool Hash::operator<(const Hash& hash) const {
  return std::memcmp(value_, hash.value(), HASH_BYTE_LEN) < 0;
}

bool Hash::operator==(const Hash& hash) const {
  return std::memcmp(value_, hash.value(), HASH_BYTE_LEN) == 0;
}

bool Hash::operator>(const Hash& hash) const {
  return std::memcmp(value_, hash.value(), HASH_BYTE_LEN) > 0;
}

bool Hash::operator<=(const Hash& hash) const { return !operator>(hash); }
bool Hash::operator>=(const Hash& hash) const { return !operator<(hash); }
bool Hash::operator!=(const Hash& hash) const { return !operator==(hash); }

// caution: this base32 implementation can only used in UStore case,
// it does not process the padding, since UStore's hash value have 20 bytes
// which is a multiplier of 5 bits, so no need of padding.
void Hash::FromString(const std::string& base32) {
  CHECK_EQ(HASH_STRING_LEN, base32.length())
      << "length of input string is not 32 bytes";
  if (own_ == false) {
    own_ = true;
    value_ = new byte_t[HASH_BYTE_LEN];
  }
  uint64_t tmp;
  size_t dest = 0;
  for (size_t i = 0; i < HASH_STRING_LEN; i += 8) {
    tmp = 0;
    for (size_t j = 0; j < 8; ++j) {
      tmp <<= 5;
      tmp += base32dict.at(base32[i + j]);
    }
    for (size_t j = 0; j < 5; ++j) {
      value_[dest + 4 - j] = byte_t(tmp & ((1 << 8) - 1));
      tmp >>= 8;
    }
    dest += 5;
  }
}

std::string Hash::ToString() {
  std::string ret;
  uint64_t tmp;
  for (size_t i = 0; i < HASH_BYTE_LEN; i += 5) {
    tmp = 0;
    for (size_t j = 0; j < 5; ++j) tmp = (tmp << 8) + uint64_t(value_[i + j]);
    for (size_t j = 0; j < 8; ++j) {
      ret += base32alphabet[size_t(uint8_t(tmp >> 5 * (7 - j)))];
      tmp &= (uint64_t(1) << 5 * (7 - j)) - 1;
    }
  }
  return ret;
}

#ifdef USE_SHA256
void Hash::Compute(const byte_t* data, size_t len) {
  if (own_ == false) {
    own_ = true;
    value_ = new byte_t[HASH_BYTE_LEN];
  }
  byte_t fullhash[32];
  picosha2::hash256(data, data + len, fullhash, fullhash + 32);
  std::copy(fullhash, fullhash + HASH_BYTE_LEN, value_);
}
#endif  // USE_SHA256

}  // namespace ustore
