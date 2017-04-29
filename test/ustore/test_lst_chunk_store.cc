// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#include <cstring>
#include <string>
#include <chrono>
#include <thread>
#include "gtest/gtest.h"
#include "store/lst_store.h"

#define MAKE_TYPE_ITERATOR(type) \
  using type##Iterator = typename ustore::lst_store::LSTStore::type_iterator \
        <ustore::ChunkType, ustore::ChunkType::k##type>;

#define MAKE_UNSAFE_TYPE_ITERATOR(type) \
  using type##Iterator = typename ustore::lst_store::LSTStore::unsafe_type_iterator \
        <ustore::ChunkType, ustore::ChunkType::k##type>;

template<typename Iterator>
int count(Iterator begin) {
  Iterator end = ustore::lst_store::LSTStore::Instance()->end<Iterator>();
  int cnt = 0;
  while (begin != end) {
    ++begin; ++cnt;
  }
  return cnt;
}

const int NUMBER = 98324;
const int LEN = 100;
ustore::byte_t raw_data[LEN];
ustore::byte_t hash[NUMBER][ustore::Hash::kByteLength];

TEST(LSTStore, Put) {
  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  ustore::lst_store::LSTStore* cs = ustore::lst_store::LSTStore::Instance();
  for (int i = 0; i < NUMBER; ++i, (*val)++) {
    ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    cs->Put(chunk.hash(), chunk);
  }
  cs->Sync();
}

TEST(LSTStore, Get) {
  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  ustore::lst_store::LSTStore* cs = ustore::lst_store::LSTStore::Instance();
  for (int i = 0; i < NUMBER; ++i, (*val)++) {
    ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    std::copy(chunk.hash().value(), chunk.hash().value()
              + ustore::Hash::kByteLength, hash[i]);
  }

  auto tp = std::chrono::steady_clock::now();
  for (int i = 0; i < NUMBER; ++i) {
    // load from stroage
    const ustore::Chunk* c =
      cs->Get(ustore::Hash(reinterpret_cast<ustore::byte_t*>(hash[i])));
      EXPECT_EQ(c->type(), ustore::ChunkType::kBlob);
      EXPECT_EQ(c->numBytes(), LEN + ::ustore::Chunk::kMetaLength);
      EXPECT_EQ(c->capacity(), LEN);
    // EXPECT_EQ(c->hash(), chunk.hash());
    delete c;
  }
  //DLOG(INFO) << std::chrono::duration_cast<std::chrono::microseconds>(
  //    std::chrono::steady_clock::now() - tp).count();
}

TEST(LSTStore, Iterator) {
  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  ustore::lst_store::LSTStore* cs = ustore::lst_store::LSTStore::Instance();
  int cnt = 0;
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  for (auto it = cs->begin(); it != cs->end(); ++it) {
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    cnt++;
    const ustore::Chunk& ichunk = *it;
    EXPECT_EQ(ichunk.type(), chunk.type());
    EXPECT_EQ(ichunk.hash(), chunk.forceHash());
    EXPECT_EQ(ichunk.numBytes(), chunk.numBytes());
    EXPECT_EQ(ichunk.capacity(), chunk.capacity());
    (*val)++;
  }
  EXPECT_EQ(cnt, NUMBER);

  EXPECT_EQ(cnt, NUMBER);
  EXPECT_EQ(std::distance(cs->begin(), cs->end()), NUMBER);

  cnt = 0;
  for (const auto& chunk : (*cs)) {
    cnt++;
  }
  EXPECT_EQ(cnt, NUMBER);
}

TEST(LSTStore, UnsafeIterator) {
  using Iterator = typename ustore::lst_store::LSTStore::unsafe_iterator;

  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  ustore::lst_store::LSTStore* cs = ustore::lst_store::LSTStore::Instance();
  int cnt = 0;
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  for (auto it = cs->begin<Iterator>(); it != cs->end<Iterator>(); ++it) {
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    cnt++;
    const ustore::Chunk& ichunk = *it;
    EXPECT_EQ(ichunk.type(), chunk.type());
    EXPECT_EQ(ichunk.hash(), chunk.forceHash());
    EXPECT_EQ(ichunk.numBytes(), chunk.numBytes());
    EXPECT_EQ(ichunk.capacity(), chunk.capacity());
    (*val)++;
  }
  EXPECT_EQ(cnt, NUMBER);
}

TEST(LSTStore, TypeIterator) {

  MAKE_TYPE_ITERATOR(Cell);
  MAKE_TYPE_ITERATOR(Meta);
  MAKE_TYPE_ITERATOR(Blob);
  MAKE_TYPE_ITERATOR(String);

  ustore::lst_store::LSTStore* cs = ustore::lst_store::LSTStore::Instance();
  EXPECT_EQ(count(cs->begin<CellIterator>()), 0);
  EXPECT_EQ(count(cs->begin<MetaIterator>()), 0);
  EXPECT_EQ(count(cs->begin<StringIterator>()), 0);
  EXPECT_EQ(count(cs->begin<BlobIterator>()), NUMBER);

  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  int cnt = 0;
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  for (auto it = cs->begin<BlobIterator>(); it != cs->end<BlobIterator>(); ++it) {
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    cnt++;
    const ustore::Chunk& ichunk = *it;
    EXPECT_EQ(ichunk.type(), chunk.type());
    EXPECT_EQ(ichunk.hash(), chunk.forceHash());
    EXPECT_EQ(ichunk.numBytes(), chunk.numBytes());
    EXPECT_EQ(ichunk.capacity(), chunk.capacity());
    (*val)++;
  }
  EXPECT_EQ(cnt, NUMBER);
}

TEST(LSTStore, UnsafeTypeIterator) {
  MAKE_UNSAFE_TYPE_ITERATOR(Cell);
  MAKE_UNSAFE_TYPE_ITERATOR(Meta);
  MAKE_UNSAFE_TYPE_ITERATOR(Blob);
  MAKE_UNSAFE_TYPE_ITERATOR(String);

  ustore::lst_store::LSTStore* cs = ustore::lst_store::LSTStore::Instance();
  EXPECT_EQ(count(cs->begin<CellIterator>()), 0);
  EXPECT_EQ(count(cs->begin<MetaIterator>()), 0);
  EXPECT_EQ(count(cs->begin<StringIterator>()), 0);
  EXPECT_EQ(count(cs->begin<BlobIterator>()), NUMBER);

  std::memset(raw_data, 0, LEN);
  uint64_t* val = reinterpret_cast<uint64_t*>(raw_data);
  int cnt = 0;
  ustore::Chunk chunk(ustore::ChunkType::kBlob, sizeof(raw_data));
  for (auto it = cs->begin<BlobIterator>(); it != cs->end<BlobIterator>(); ++it) {
    std::copy(raw_data, raw_data + sizeof(raw_data), chunk.m_data());
    cnt++;
    const ustore::Chunk& ichunk = *it;
    EXPECT_EQ(ichunk.type(), chunk.type());
    EXPECT_EQ(ichunk.hash(), chunk.forceHash());
    EXPECT_EQ(ichunk.numBytes(), chunk.numBytes());
    EXPECT_EQ(ichunk.capacity(), chunk.capacity());
    (*val)++;
  }
  EXPECT_EQ(cnt, NUMBER);
}
