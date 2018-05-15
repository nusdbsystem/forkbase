// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VHANDLER_H_
#define USTORE_TYPES_CLIENT_VHANDLER_H_

#include <memory>
#include <utility>
#include "spec/db.h"
#include "types/client/vblob.h"
#include "types/client/vlist.h"
#include "types/client/vmap.h"
#include "types/client/vset.h"
#include "types/client/vstring.h"
#include "utils/utils.h"

namespace ustore {

/*
 * Main entry for initializing objects in all types.
 */
class VHandler {
 public:
  VHandler() = default;
  explicit VHandler(DB* db) : db_(db) {}
  VHandler(DB* db, std::shared_ptr<ChunkLoader> loader)
    : db_(db), loader_(loader) {}
  virtual ~VHandler() = default;

  inline bool empty() const { return !db_; }
  virtual UType type() const = 0;
  virtual Hash dataHash() const = 0;
  virtual Slice partitionKey() const = 0;

  // primitive types are not supported by default
  virtual VString String() const;

  // chunkable types
  VBlob Blob() const;
  VList List() const;
  VMap Map() const;
  VSet Set() const;

  friend std::ostream& operator<<(std::ostream& os, const VHandler& obj);

 private:
  DB* db_ = nullptr;
  std::shared_ptr<ChunkLoader> loader_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_CLIENT_VHANDLER_H_
