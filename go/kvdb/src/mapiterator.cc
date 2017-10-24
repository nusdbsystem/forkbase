// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#include "db.h"
#include "utils/logging.h"

namespace ustore_kvdb {

MapIterator::MapIterator(ustore::ObjectDB* odb, const std::string& key, const std::string& version)
{
  odb_ = odb;
  iterator_ = new ustore::UMap::Iterator(odb_->Get(ustore::Slice(key),
            ustore::Hash(reinterpret_cast<const unsigned char*>(version.data()))).value.Map().Scan());
  key_ = key;
  version_ = version;
}

bool MapIterator::Valid() { return !iterator_->end(); }

void MapIterator::SeekToFirst() {
  iterator_ = new ustore::UMap::Iterator(odb_->Get(ustore::Slice(key_),
            ustore::Hash(reinterpret_cast<const unsigned char*>(version_.data()))).value.Map().Scan());
}

bool MapIterator::Next() {
  return iterator_->next();
}

std::string MapIterator::key() const {
  return iterator_->key().ToString(); 
}

std::string MapIterator::value() const {
  return iterator_->value().ToString();
}

}  // namespace ustore_kvdb
