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

VString VMeta::String() const {
  if (!cell_.empty() && cell_.type() == UType::kString) {
    return VString(std::make_shared<ClientChunkLoader>(db_, cell_.key()),
                   cell_.dataHash());
  }
  return VString();
}

VList VMeta::List() const {
  if (!cell_.empty() && cell_.type() == UType::kList) {
    return VList(std::make_shared<ClientChunkLoader>(db_, cell_.key()),
                   cell_.dataHash());
  }
  return VList();
}

VMap VMeta::Map() const {
  if (!cell_.empty() && cell_.type() == UType::kMap) {
    return VMap(std::make_shared<ClientChunkLoader>(db_, cell_.key()),
                   cell_.dataHash());
  }
  return VMap();
}

}  // namespace ustore

