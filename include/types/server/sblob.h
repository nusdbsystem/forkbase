// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_SBLOB_H_
#define USTORE_TYPES_SERVER_SBLOB_H_

#include <memory>

#include "chunk/chunk_loader.h"
#include "chunk/chunk_writer.h"
#include "types/ublob.h"

namespace ustore {

class SBlob : public UBlob {
  friend class ChunkableTypeFactory;
 public:
  SBlob(SBlob&&) = default;
  SBlob& operator=(SBlob&&) = default;
  ~SBlob() = default;

  Hash Splice(size_t pos, size_t num_delete,
              const byte_t* data, size_t num_insert) const override;

 protected:
  // Load exsiting SBlob
  SBlob(const Hash& root_hash,
        std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer) noexcept;
  // Create new SBlob
  SBlob(const Slice& slice,
        std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer) noexcept;

 private:
  ChunkWriter* chunk_writer_;
};

}  // namespace ustore
#endif  // USTORE_TYPES_SERVER_SBLOB_H_
