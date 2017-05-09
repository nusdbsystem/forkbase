// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#include <cstring>
#include <string>
#include <chrono>
#include <thread>
#include "gtest/gtest.h"
#include "store/lst_store.h"
#include "utils/type_traits.h"

#define MAKE_TYPE_ITERATOR(type) \
  using type##Iterator = typename ustore::lst_store::LSTStore::type_iterator \
        <ustore::ChunkType, ustore::ChunkType::k##type>;

#define MAKE_UNSAFE_TYPE_ITERATOR(type) \
  using type##Iterator = typename ustore::lst_store::LSTStore::unsafe_type_iterator \
        <ustore::ChunkType, ustore::ChunkType::k##type>;

const int NUMBER = 98342;
const int LEN = 100;
ustore::byte_t raw_data[LEN];
ustore::byte_t hash[NUMBER][ustore::Hash::kByteLength];

using LSTStore = ustore::lst_store::LSTStore;
using Chunk = ustore::Chunk;

LSTStore* lstStore = LSTStore::Instance();

template <typename Iterator>
Iterator FindChunk(const Chunk& chunk) {
  auto it = lstStore->begin<Iterator>();
  for (; it != lstStore->end<Iterator>(); ++it) {
    const ustore::Chunk& ichunk = *it;
    if (ichunk.hash() == chunk.hash()) break;
  }
  return it;
}

template <typename Iterator>
Iterator begin() {
  std::memset(raw_data, 0, LEN);
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
  chunk.forceHash();
  return FindChunk<typename Iterator::BaseIterator>(chunk);
}

template <typename Iterator>
Iterator end() {
  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  *val = NUMBER - 1;
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
  chunk.forceHash();
  return ++FindChunk<typename Iterator::BaseIterator>(chunk);
}

TEST(LSTStore, Put) {
  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  for (int i = 0; i < NUMBER; ++i, (*val)++) {
    ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    lstStore->Put(chunk.hash(), chunk);
  }
  lstStore->Sync();
}

TEST(LSTStore, Get) {
  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  for (int i = 0; i < NUMBER; ++i, (*val)++) {
    ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    std::copy(chunk.hash().value(), chunk.hash().value()
              + ustore::Hash::kByteLength, hash[i]);
  }

  auto tp = std::chrono::steady_clock::now();
  for (int i = 0; i < NUMBER; ++i) {
    // load from stroage
    const ustore::Chunk c =
      lstStore->Get(ustore::Hash(reinterpret_cast<ustore::byte_t*>(hash[i])));
      EXPECT_EQ(c.type(), ustore::ChunkType::kBlob);
      EXPECT_EQ(c.numBytes(), LEN + ::ustore::Chunk::kMetaLength);
      EXPECT_EQ(c.capacity(), LEN);
      //EXPECT_EQ(c->hash(), chunk.forceHash());
  }
  //DLOG(INFO) << std::chrono::duration_cast<std::chrono::microseconds>(
  //    std::chrono::steady_clock::now() - tp).count();
}

TEST(LSTStore, Iterator) {
  using Iterator = typename LSTStore::iterator;
  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  int cnt = 0;
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  auto first = begin<Iterator>();
  auto last = end<Iterator>();
  for (auto it = first; it != last; ++it) {
    (*val) = cnt++;
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    const ustore::Chunk& ichunk = *it;
    EXPECT_EQ(ichunk.type(), chunk.type());
    EXPECT_EQ(ichunk.hash(), chunk.forceHash());
    EXPECT_EQ(ichunk.numBytes(), chunk.numBytes());
    EXPECT_EQ(ichunk.capacity(), chunk.capacity());
  }
  EXPECT_EQ(cnt, NUMBER);

  EXPECT_EQ(std::distance(first, last), NUMBER);
}

TEST(LSTStore, UnsafeIterator) {
  using Iterator = typename LSTStore::unsafe_iterator;
  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  int cnt = 0;
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  auto first = begin<Iterator>();
  auto last = end<Iterator>();
  for (auto it = first; it != last; ++it) {
    (*val) = cnt++;
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    const ustore::Chunk& ichunk = *it;
    EXPECT_EQ(ichunk.type(), chunk.type());
    EXPECT_EQ(ichunk.hash(), chunk.forceHash());
    EXPECT_EQ(ichunk.numBytes(), chunk.numBytes());
    EXPECT_EQ(ichunk.capacity(), chunk.capacity());
  }
  EXPECT_EQ(cnt, NUMBER);

  EXPECT_EQ(std::distance(first, last), NUMBER);
}

TEST(LSTStore, TypeIterator) {

  MAKE_TYPE_ITERATOR(Cell);
  MAKE_TYPE_ITERATOR(Meta);
  MAKE_TYPE_ITERATOR(Blob);
  MAKE_TYPE_ITERATOR(String);

  EXPECT_EQ(std::distance(begin<MetaIterator>(), end<MetaIterator>()), 0);
  EXPECT_EQ(std::distance(begin<CellIterator>(), end<CellIterator>()), 0);
  EXPECT_EQ(std::distance(begin<StringIterator>(), end<StringIterator>()), 0);
  EXPECT_EQ(std::distance(begin<BlobIterator>(), end<BlobIterator>()), NUMBER);

  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  int cnt = 0;
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  auto first = begin<BlobIterator>();
  auto last = end<BlobIterator>();
  for (auto it = first; it != last; ++it) {
    (*val) = cnt++;
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    const ustore::Chunk& ichunk = *it;
    EXPECT_EQ(ichunk.type(), chunk.type());
    EXPECT_EQ(ichunk.hash(), chunk.forceHash());
    EXPECT_EQ(ichunk.numBytes(), chunk.numBytes());
    EXPECT_EQ(ichunk.capacity(), chunk.capacity());
  }
  EXPECT_EQ(cnt, NUMBER);
}

TEST(LSTStore, UnsafeTypeIterator) {
  MAKE_UNSAFE_TYPE_ITERATOR(Cell);
  MAKE_UNSAFE_TYPE_ITERATOR(Meta);
  MAKE_UNSAFE_TYPE_ITERATOR(Blob);
  MAKE_UNSAFE_TYPE_ITERATOR(String);

  EXPECT_EQ(std::distance(begin<MetaIterator>(), end<MetaIterator>()), 0);
  EXPECT_EQ(std::distance(begin<CellIterator>(), end<CellIterator>()), 0);
  EXPECT_EQ(std::distance(begin<StringIterator>(), end<StringIterator>()), 0);
  EXPECT_EQ(std::distance(begin<BlobIterator>(), end<BlobIterator>()), NUMBER);

  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  int cnt = 0;
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  auto first = begin<BlobIterator>();
  auto last = end<BlobIterator>();
  for (auto it = first; it != last; ++it) {
    (*val) = cnt++;
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    const ustore::Chunk& ichunk = *it;
    EXPECT_EQ(ichunk.type(), chunk.type());
    EXPECT_EQ(ichunk.hash(), chunk.forceHash());
    EXPECT_EQ(ichunk.numBytes(), chunk.numBytes());
    EXPECT_EQ(ichunk.capacity(), chunk.capacity());
  }
  EXPECT_EQ(cnt, NUMBER);
}
