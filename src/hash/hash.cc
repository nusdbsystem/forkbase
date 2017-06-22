// Copyright (c) 2017 The Ustore Authors.

#include "hash/hash.h"

#include <cstring>
#include <iostream>
#include <map>
#include <utility>
#include "utils/logging.h"

#ifdef USE_CRYPTOPP
#include "cryptopp/cryptlib.h"
#ifdef USE_SHA256
#include "cryptopp/sha.h"
#elif USE_BLAKE2b
#include "cryptopp/blake2.h"
#endif
#else
#include "hash/sha2.h"
#endif

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

// caution: this base32 implementation can only used in UStore case,
// it does not process the padding, since UStore's hash value have 20 bytes
// which is a multiplier of 5 bits, so no need of padding.
Hash Hash::FromBase32(const std::string& base32) {
  CHECK_EQ(kBase32Length, base32.length())
      << "length of input string is not 32 bytes";
  Hash h;
  h.Alloc();
  uint64_t tmp;
  size_t dest = 0;
  for (size_t i = 0; i < kBase32Length; i += 8) {
    tmp = 0;
    for (size_t j = 0; j < 8; ++j) {
      tmp <<= 5;
      tmp += base32dict.at(base32[i + j]);
    }
    for (size_t j = 0; j < 5; ++j) {
      h.own_[dest + 4 - j] = byte_t(tmp & ((1 << 8) - 1));
      tmp >>= 8;
    }
    dest += 5;
  }
  return h;
}

std::string Hash::ToBase32() const {
  if (empty()) {
    LOG(WARNING) << "Convert from an empty hash";
    return std::string();
  }
  std::string ret;
  uint64_t tmp = 0;
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
  if (!empty()) {
    hash.Alloc();
    std::memcpy(hash.own_.get(), value_, kByteLength);
  }
  return hash;
}

void Hash::Alloc() {
  if (!own_) {
    own_.reset(new byte_t[kByteLength]);
    value_ = own_.get();
  }
}

#ifdef USE_CRYPTOPP
#ifdef USE_SHA256
Hash Hash::ComputeFrom(const byte_t* data, size_t len) {
  byte_t fullhash[CryptoPP::SHA256::DIGESTSIZE];
  Hash h;
  h.Alloc();
  CryptoPP::SHA256 hash_gen;
  hash_gen.CalculateDigest(fullhash, data, len);
  std::copy(fullhash, fullhash + kByteLength, h.own_.get());
  return h;
}
#elif USE_BLAKE2b
Hash Hash::ComputeFrom(const byte_t* data, size_t len) {
  byte_t fullhash[CryptoPP::BLAKE2b::DIGESTSIZE];
  Hash h;
  h.Alloc();
  CryptoPP::BLAKE2b hash_gen;
  hash_gen.CalculateDigest(fullhash, data, len);
  std::copy(fullhash, fullhash + kByteLength, h.own_.get());
  return h;
}
#endif  // USE_SHA256
#else
Hash Hash::ComputeFrom(const byte_t* data, size_t len) {
  byte_t fullhash[Hash::kBase32Length];
  Hash h;
  h.Alloc();
  picosha2::hash256(data, data + len, fullhash, fullhash + kBase32Length);
  std::copy(fullhash, fullhash + kByteLength, h.own_.get());
  return h;
}
#endif  // USE_CRYPTOPP

Hash Hash::ComputeFrom(const std::string& data) {
  return Hash::ComputeFrom(reinterpret_cast<const byte_t*>(data.c_str()),
                           data.size());
}

}  // namespace ustore
