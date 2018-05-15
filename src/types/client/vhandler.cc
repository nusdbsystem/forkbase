// Copyright (c) 2017 The Ustore Authors.

#include "types/client/vhandler.h"

namespace ustore {

VBlob VHandler::Blob() const {
  if (!empty() && type() == UType::kBlob) {
    if (loader_) return VBlob(loader_, dataHash());
    return VBlob(std::make_shared<ClientChunkLoader>(db_, partitionKey()),
                 dataHash());
  }
  LOG(WARNING) << "Get empty VBlob, actual type: " << type();
  return VBlob();
}

VString VHandler::String() const {
  LOG(WARNING) << "Get empty VString, type not supported";
  return VString();
}

VList VHandler::List() const {
  if (!empty() && type() == UType::kList) {
    if (loader_) return VList(loader_, dataHash());
    return VList(std::make_shared<ClientChunkLoader>(db_, partitionKey()),
                 dataHash());
  }
  LOG(WARNING) << "Get empty VList, actual type: " << type();
  return VList();
}

VMap VHandler::Map() const {
  if (!empty() && type() == UType::kMap) {
    if (loader_) return VMap(loader_, dataHash());
    return VMap(std::make_shared<ClientChunkLoader>(db_, partitionKey()),
                dataHash());
  }
  LOG(WARNING) << "Get empty VMap, actual type: " << type();
  return VMap();
}

VSet VHandler::Set() const {
  if (!empty() && type() == UType::kSet) {
    if (loader_) return VSet(loader_, dataHash());
    return VSet(std::make_shared<ClientChunkLoader>(db_, partitionKey()),
                dataHash());
  }
  LOG(WARNING) << "Get empty VSet, actual type: " << type();
  return VSet();
}

std::ostream& operator<<(std::ostream& os, const VHandler& obj) {
  switch (obj.type()) {
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
