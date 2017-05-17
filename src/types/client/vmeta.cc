// Copyright (c) 2017 The Ustore Authors.

#include "types/client/vmeta.h"

namespace ustore {

VBlob VMeta::Blob() const {
  if (!cell_.empty() && cell_.type() == UType::kBlob) {
    return VBlob(std::make_shared<ClientChunkLoader>(db_, cell_.key()),
                 cell_.dataHash());
  }
  return VBlob();
}

}  // namespace ustore

