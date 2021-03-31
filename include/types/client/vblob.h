// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VBLOB_H_
#define USTORE_TYPES_CLIENT_VBLOB_H_

#include <memory>
#include "types/client/vobject.h"
#include "types/ublob.h"

namespace ustore {

class VBlob : public UBlob, public VObject {
  friend class VHandler;

 public:
  VBlob() noexcept : VBlob(Slice()) {}
  VBlob(VBlob&&) = default;
  VBlob& operator=(VBlob&&) = default;
  // Create new VBlob
  explicit VBlob(const Slice& slice) noexcept;
  explicit VBlob(std::istream* is) noexcept;
  ~VBlob() = default;

  Hash Splice(size_t pos, size_t num_delete, const byte_t* data,
              size_t num_insert) const override;

  Hash Splice(size_t pos, size_t num_delete, std::istream* is,
              size_t num_insert) const;

 protected:
  // Load exsiting VBlob
  VBlob(std::shared_ptr<ChunkLoader> loader, const Hash& root_hash) noexcept;
};

}  // namespace ustore

#endif  // USTORE_TYPES_CLIENT_VBLOB_H_
