// Copyright (c) 2017 The Ustore Authors.

#include "types/server/factory.h"

namespace ustore {

ChunkableTypeFactory::ChunkableTypeFactory(const Partitioner* ptt)
    : writer_(new LocalChunkWriter()) {}

SBlob ChunkableTypeFactory::LoadBlob(const Hash& root_hash) {
  return SBlob(root_hash, std::make_shared<LocalChunkLoader>(), writer_.get());
}

SBlob ChunkableTypeFactory::CreateBlob(const Slice& slice) {
  return SBlob(slice, std::make_shared<LocalChunkLoader>(), writer_.get());
}

SList ChunkableTypeFactory::LoadList(const Hash& root_hash) {
  return SList(root_hash, std::make_shared<LocalChunkLoader>(), writer_.get());
}

SList ChunkableTypeFactory::CreateList(const std::vector<Slice>& elements) {
  return SList(elements, std::make_shared<LocalChunkLoader>(), writer_.get());
}

SMap ChunkableTypeFactory::LoadMap(const Hash& root_hash) {
  return SMap(root_hash, std::make_shared<LocalChunkLoader>(), writer_.get());
}

SMap ChunkableTypeFactory::CreateMap(const std::vector<Slice>& keys,
                                     const std::vector<Slice>& vals) {
  return SMap(keys, vals, std::make_shared<LocalChunkLoader>(), writer_.get());
}

}  // namespace ustore
