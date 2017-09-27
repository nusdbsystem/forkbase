// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#include <cstring>
#include <string>
#include <chrono>
#include <thread>
#include "gtest/gtest.h"
#include "store/lst_store.h"
#include "store/iterator.h"
#include "utils/enum.h"
#include "utils/type_traits.h"

#define MAKE_TYPE_ITERATOR(type) \
  using type##Iterator = typename ustore::lst_store::LSTStore::type_iterator \
        <ustore::ChunkType, ustore::ChunkType::k##type>;

#define MAKE_UNSAFE_TYPE_ITERATOR(type) \
  using type##Iterator = typename ustore::lst_store::LSTStore::unsafe_type_iterator \
        <ustore::ChunkType, ustore::ChunkType::k##type>;

// const int NUMBER = 98342;
const int NUMBER = 150000;
const int LEN = 100;
ustore::byte_t raw_data[LEN];
ustore::byte_t hash[NUMBER][ustore::Hash::kByteLength];

using LSTStore = ustore::lst_store::LSTStore;
using Chunk = ustore::Chunk;
using StoreIterator = ustore::StoreIterator;

template <typename Iterator>
StoreIterator FindChunk(const Chunk& chunk) {
  LSTStore* lstStore = LSTStore::Instance();
  auto it = lstStore->begin<Iterator>();
  for (; it != lstStore->end<Iterator>(); ++it) {
    const ustore::Chunk& ichunk = *it;
    if (ichunk.hash() == chunk.hash()) return (it);
  }
  return it;
}

template <typename Iterator>
StoreIterator begin() {
  std::memset(raw_data, 0, LEN);
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
  chunk.forceHash();
  return FindChunk<Iterator>(chunk);
}

template <typename Iterator>
StoreIterator end() {
  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  *val = NUMBER - 1;
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
  chunk.forceHash();
  auto it = FindChunk<Iterator>(chunk);
  if (it != LSTStore::Instance()->end<Iterator>() && chunk.hash() == (*it).hash()) ++it;
  return it;
}

TEST(LSTStore, Put) {
  LSTStore* lstStore = LSTStore::Instance();
  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  for (int i = 0; i < NUMBER; ++i, (*val)++) {
    ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    lstStore->Put(chunk.hash(), chunk);
  }
  lstStore->Sync();
  std::cout << lstStore->GetInfo();
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

  LSTStore* lstStore = LSTStore::Instance();
  for (int i = 0; i < NUMBER; ++i) {
    // load from stroage
    ustore::Chunk c =
      lstStore->Get(ustore::Hash(hash[i]));
      EXPECT_EQ(c.type(), ustore::ChunkType::kBlob);
      EXPECT_EQ(c.numBytes(), LEN + ::ustore::Chunk::kMetaLength);
      EXPECT_EQ(c.capacity(), size_t(LEN));
  }
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
  MAKE_TYPE_ITERATOR(List);

  EXPECT_EQ(std::distance(begin<MetaIterator>(), end<MetaIterator>()), 0);
  EXPECT_EQ(std::distance(begin<CellIterator>(), end<CellIterator>()), 0);
  EXPECT_EQ(std::distance(begin<ListIterator>(), end<ListIterator>()), 0);
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
  MAKE_UNSAFE_TYPE_ITERATOR(List);

  EXPECT_EQ(std::distance(begin<MetaIterator>(), end<MetaIterator>()), 0);
  EXPECT_EQ(std::distance(begin<CellIterator>(), end<CellIterator>()), 0);
  EXPECT_EQ(std::distance(begin<ListIterator>(), end<ListIterator>()), 0);
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
