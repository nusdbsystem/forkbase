// Copyright (c) 2017 The Ustore Authors.

#include "types/client/vmeta.h"
#include "utils/utils.h"

namespace ustore {

VBlob VMeta::Blob() const {
  if (!cell_.empty() && cell_.type() == UType::kBlob) {
    return VBlob(std::make_shared<ClientChunkLoader>(db_, cell_.key()),
                 cell_.dataHash());
  }
  LOG(WARNING) << "Get empty VBlob, actual type: " << cell_.type();
  return VBlob();
}

VString VMeta::String() const {
  if (!cell_.empty() && cell_.type() == UType::kString) {
    return VString(cell_);
  }
  LOG(WARNING) << "Get empty VString, actual type: " << cell_.type();
  return VString();
}

VList VMeta::List() const {
  if (!cell_.empty() && cell_.type() == UType::kList) {
    return VList(std::make_shared<ClientChunkLoader>(db_, cell_.key()),
                 cell_.dataHash());
  }
  LOG(WARNING) << "Get empty VList, actual type: " << cell_.type();
  return VList();
}

VMap VMeta::Map() const {
  if (!cell_.empty() && cell_.type() == UType::kMap) {
    return VMap(std::make_shared<ClientChunkLoader>(db_, cell_.key()),
                cell_.dataHash());
  }
  LOG(WARNING) << "Get empty VMap, actual type: " << cell_.type();
  return VMap();
}

VSet VMeta::Set() const {
  if (!cell_.empty() && cell_.type() == UType::kSet) {
    return VSet(std::make_shared<ClientChunkLoader>(db_, cell_.key()),
                cell_.dataHash());
  }
  LOG(WARNING) << "Get empty VSet, actual type: " << cell_.type();
  return VSet();
}

std::ostream& operator<<(std::ostream& os, const VMeta& obj) {
  switch (obj.cell_.type()) {
    case UType::kBlob:
      os << obj.Blob();
      break;
    case UType::kString:
      os << obj.String();
      break;
    case UType::kList:
      os << obj.List();
      break;
    case UType::kMap:
      os << obj.Map();
      break;
    case UType::kSet:
      os << obj.Set();
      break;
    default:
      os << "<unknown>";
      break;
  }
  return os;
}

}  // namespace ustore
