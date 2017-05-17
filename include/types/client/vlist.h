// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VLIST_H_
#define USTORE_TYPES_CLIENT_VLIST_H_

#include <memory>
#include <vector>
#include "types/client/buffered.h"
#include "types/ulist.h"

namespace ustore {

class VList : public UList, public VValue {
 public:
  ~SList() = default;

  // entry vector can be empty
  Hash Splice(uint64_t start_idx, uint64_t num_to_delete,
              const std::vector<Slice>& entries) const override;

 protected:
  // Load an existing VList
  VList(std::shared_ptr<ChunkLoader>, const Hash& root_hash) noexcept;
  // Create a new VList
  VList(std::shared_ptr<ChunkLoader>,
        const std::vector<Slice>& elements) noexcept;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_CLIENT_VLIST_H_
