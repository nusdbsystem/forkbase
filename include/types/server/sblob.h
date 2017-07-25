// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_SBLOB_H_
#define USTORE_TYPES_SERVER_SBLOB_H_

#include <memory>
#include "types/ublob.h"

namespace ustore {

class SBlob : public UBlob {
 public:
  SBlob() = default;
  SBlob(SBlob&&) = default;
  SBlob& operator=(SBlob&&) = default;
  // Load exsiting SBlob
  explicit SBlob(const Hash& root_hash) noexcept;
  // Create new SBlob
  explicit SBlob(const Slice& slice) noexcept;
  ~SBlob() = default;

  Hash Splice(size_t pos, size_t num_delete,
              const byte_t* data, size_t num_insert) const override;
};

}  // namespace ustore
#endif  // USTORE_TYPES_SERVER_SBLOB_H_
