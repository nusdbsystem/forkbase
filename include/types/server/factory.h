// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_FACTORY_H_
#define USTORE_TYPES_SERVER_FACTORY_H_

#include <memory>
#include <vector>

#include "cluster/partitioner.h"
#include "types/server/sblob.h"
#include "types/server/slist.h"
#include "types/server/smap.h"
#include "utils/noncopyable.h"

namespace ustore {

class ChunkableTypeFactory : private Noncopyable {
 public:
  // local chunkable types
  ChunkableTypeFactory() : ChunkableTypeFactory(nullptr) {}
  // distributed chunkable types
  explicit ChunkableTypeFactory(const Partitioner* ptt);
  ~ChunkableTypeFactory() = default;

  // Load exsiting SBlob
  SBlob LoadBlob(const Hash& root_hash);
  // Create new SBlob
  SBlob CreateBlob(const Slice& slice);

  // Load existing SList
  SList LoadList(const Hash& root_hash);
  // create new SList
  SList CreateList(const std::vector<Slice>& elements);

  // Load existing SMap
  SMap LoadMap(const Hash& root_hash);
  // Create new SMap
  // kv_items must be sorted in strict ascending order based on key
  SMap CreateMap(const std::vector<Slice>& keys,
                 const std::vector<Slice>& vals);

 private:
  const Partitioner* ptt_;
  std::unique_ptr<ChunkWriter> writer_;
};

}  // namespace ustore
#endif  // USTORE_TYPES_SERVER_FACTORY_H_
