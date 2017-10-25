// Copyright (c) 2017 The Ustore Authors.
#include "db.h"
#include "types/client/vmap.h"

namespace ustore_kvdb {

const ustore::Slice KVDB::DEFAULT_BRANCH = ustore::Slice("default", 7);
const size_t kMaxSize = 1<<12;
Status KVDB::Get(const std::string& key, std::string* value) {
  //auto ret = odb_.Get(ustore::Slice(key), DEFAULT_BRANCH);
  auto ret = odb_.Get(ustore::Slice(cfname_), ustore::Slice(key)); 
  if (ret.stat == ustore::ErrorCode::kUCellNotExists ||
      ret.stat == ustore::ErrorCode::kKeyNotExists ||
      ret.stat == ustore::ErrorCode::kBranchNotExists) {
    return Status::NotFound(key);
  }
  CHECK(ustore::ErrorCode::kOK == ret.stat);
//#ifdef GO_USE_BLOB
  if (ret.value.type() == ustore::UType::kBlob) { 
    auto blob = ret.value.Blob();
    blob.Read(0, blob.size(), value);
  } else {
//#else
    auto str = ret.value.String();
    value->clear();
    value->reserve(str.len());
    value->append(reinterpret_cast<const char*>(str.data()), str.len());
  }
//#endif  // GO_USE_BLOB
  return Status::OK();
}

size_t KVDB::GetSize() {
  auto info = odb_.GetStorageInfo().value;
  return info[0].chunkBytes;
}

std::pair<Status, std::string> KVDB::Get(const std::string& key) {
  std::string str;
  auto ret = Get(key, &str);

  return std::make_pair(ret, str);
}

Status KVDB::Put(const std::string& key, const std::string& value) {
  if (value.length() > kMaxSize) { 
//#ifdef GO_USE_BLOB
    ustore::VBlob val{ustore::Slice{value}};
    auto ret = odb_.Put(ustore::Slice(cfname_), val, ustore::Slice{key});
    CHECK(ustore::ErrorCode::kOK == ret.stat);
  } else {
//#else
    ustore::VString val{ustore::Slice{value}};
    auto ret = odb_.Put(ustore::Slice(cfname_), val, ustore::Slice{key});
    CHECK(ustore::ErrorCode::kOK == ret.stat);
  }
//#endif  // GO_USE_BLOB
  //auto ret = odb_.Put(ustore::Slice{key}, val, DEFAULT_BRANCH);
    return Status::OK();
}

Status KVDB::Delete(const std::string& key) {
  //odb_.Delete(ustore::Slice(key), DEFAULT_BRANCH);
  odb_.Delete(ustore::Slice(cfname_), ustore::Slice(key));
  return Status::OK();
}

bool KVDB::Exist(const std::string& key) {
  ustore::Slice lookup_key(key);
  //return wk_.Exists(lookup_key) && wk_.Exists(lookup_key, DEFAULT_BRANCH);
  return wk_.Exists(ustore::Slice(cfname_), lookup_key);
}

Status KVDB::Write(WriteBatch* updates) {
  if (updates->types_.size() != updates->keys_.size())
    return Status::Corruption("WriteBatch corrupted");
  size_t j = 0;
  for (size_t i = 0; i < updates->types_.size(); ++i) {
    if (updates->types_[i] == WriteBatch::kInsertion) {
      auto ret = Put(updates->keys_[i], updates->vals_[j]);
      j++;
      if (!ret.ok()) return ret;
    } else {
      auto ret = Delete(updates->keys_[i]);
      if (!ret.ok()) return ret;
    }
  }
  return Status::OK();
}

Iterator* KVDB::NewIterator() { return new Iterator(this, &wk_); }

MapIterator* KVDB::NewMapIterator(const std::string& key, const std::string& version) {
  return new MapIterator(&odb_, key, version);
}

// Initialize empty map
Status KVDB::InitMap(const std::string& mapkey) {
  mapkey_ = mapkey;
  std::vector<ustore::Slice> sk, sv;
  ustore::VMap vmap(sk, sv);
  auto ret = odb_.Put(ustore::Slice(mapkey_), vmap, DEFAULT_BRANCH);
  CHECK(ustore::ErrorCode::kOK == ret.stat);
  allMapKeys_[mapkey_] = true;
  return Status::OK();
}

// Initialize empty map
Status KVDB::StartMapBatch(const std::string& mapkey) {
  current_key_= mapkey;
  if (allMapKeys_.find(current_key_) == allMapKeys_.end()) { // not exists
    std::vector<ustore::Slice> sk, sv;
    ustore::VMap vmap(sk, sv);
    auto ret = odb_.Put(ustore::Slice(current_key_), vmap, DEFAULT_BRANCH);
    CHECK(ustore::ErrorCode::kOK == ret.stat);
    allMapKeys_[current_key_] = true;
  }
  return Status::OK();
}

std::string HashToString(ustore::Hash hash) {
  return std::string(reinterpret_cast<const char*>(hash.value()), ustore::Hash::kByteLength);
}

std::pair<Status, std::string> KVDB::PutMap(const std::string& key, const std::string& value) {
  skeys_.push_back(key);
  svalues_.push_back(value);
  return std::make_pair(Status::OK(), "");
  /*
  auto v = odb_.Get(ustore::Slice(mapkey_), DEFAULT_BRANCH).value.Map();
  v.Set(ustore::Slice(key), ustore::Slice(value));
  auto ret = odb_.Put(ustore::Slice(mapkey_), v, DEFAULT_BRANCH);
  CHECK(ustore::ErrorCode::kOK == ret.stat);
  return std::make_pair(Status::OK(), HashToString(ret.value));
  */
}

std::pair<Status, std::string> KVDB::SyncMap() {
  auto v = odb_.Get(ustore::Slice(current_key_), DEFAULT_BRANCH).value.Map();
  //v.Set(ustore::Slice(key), ustore::Slice(value));
  std::vector<ustore::Slice> keys, values;
  for (auto& k : skeys_) keys.push_back(ustore::Slice(k));
  for (auto& v : svalues_) values.push_back(ustore::Slice(v));

  v.Set(keys, values);
  auto ret = odb_.Put(ustore::Slice(current_key_), v, DEFAULT_BRANCH);
  CHECK(ustore::ErrorCode::kOK == ret.stat);
  skeys_.clear();
  svalues_.clear();
  
  // put the list, to upate the top-level map
  std::string version = HashToString(ret.value);
  mapKeys_.push_back(current_key_);
  mapValues_.push_back(version);
  return std::make_pair(Status::OK(), version);
}

std::pair<Status, std::string> KVDB::WriteMap() {
  auto v = odb_.Get(ustore::Slice(mapkey_), DEFAULT_BRANCH).value.Map();
  //v.Set(ustore::Slice(key), ustore::Slice(value));
  std::vector<ustore::Slice> keys, values;
  for (auto& k : mapKeys_) keys.push_back(ustore::Slice(k));
  for (auto& v : mapValues_) values.push_back(ustore::Slice(v));

  v.Set(keys, values);
  auto ret = odb_.Put(ustore::Slice(mapkey_), v, DEFAULT_BRANCH);
  CHECK(ustore::ErrorCode::kOK == ret.stat);
  mapKeys_.clear();
  mapValues_.clear();
  
  mapKeys_.clear();
  mapValues_.clear();
  return std::make_pair(Status::OK(), HashToString(ret.value));
}

std::pair<Status, std::string> KVDB::PutBlob(const std::string& key, const std::string& value) {
  ustore::VBlob val{ustore::Slice{value}};
  auto ret = odb_.Put(ustore::Slice(key), val, DEFAULT_BRANCH);

  CHECK(ustore::ErrorCode::kOK == ret.stat);
  return std::make_pair(Status::OK(), HashToString(ret.value));
}

std::pair<Status, std::string> KVDB::GetMap(const std::string& mapkey, const std::string& key) {
  auto v = odb_.Get(ustore::Slice(mapkey), DEFAULT_BRANCH).value.Map();
  ustore::Slice ret = v.Get(ustore::Slice(key));
  if (ret.empty())
    return std::make_pair(Status::NotFound("", ""), "");
  else 
    return std::make_pair(Status::OK(), ret.ToString());
}

std::pair<Status, std::string> KVDB::GetMap(const std::string& mapkey, 
                              const std::string& key, const std::string& version) {
  auto v = odb_.Get(ustore::Slice(mapkey), ustore::Hash(
                    reinterpret_cast<const unsigned char*>(version.data()))).value.Map();
  ustore::Slice ret = v.Get(ustore::Slice(key));
  if (ret.empty())
    return std::make_pair(Status::NotFound("", ""), "");
  else 
    return std::make_pair(Status::OK(), ret.ToString());
}

std::pair<Status, MapIterator*> KVDB::GetMapIterator(const std::string& mapkey, const std::string& version) {
  return std::make_pair(Status::OK(), NewMapIterator(mapkey, version));
}

std::pair<Status, std::string> KVDB::GetPreviousVersion(const std::string& key, const std::string& version) {
  auto v = odb_.Get(ustore::Slice(key), ustore::Hash(
                    reinterpret_cast<const unsigned char*>(version.data())));
  CHECK(ustore::ErrorCode::kOK == v.stat);
  if (v.value.cell().preHash() == ustore::Hash::kNull) {
    return std::make_pair(Status::NotFound(""), "");
  }
  return std::make_pair(Status::OK(), HashToString(v.value.cell().preHash()));
}
std::pair<Status, std::string> KVDB::GetBlob(const std::string& key) {
  auto ret = odb_.Get(ustore::Slice(key), DEFAULT_BRANCH); 
  CHECK(ret.stat == ustore::ErrorCode::kOK);
  if (ret.stat != ustore::ErrorCode::kOK) {
    return std::make_pair(Status::NotFound(key), "");
  }

  std::string value;
  auto blob = ret.value.Blob();
  blob.Read(0, blob.size(), &value);
  return std::make_pair(Status::OK(), value);
}

std::pair<Status, std::string> KVDB::GetBlob(const std::string& key, const std::string& version) {
  auto ret = odb_.Get(ustore::Slice(key), ustore::Hash(
                      reinterpret_cast<const unsigned char*>(version.data()))); 
  if (ret.stat != ustore::ErrorCode::kOK) {
    return std::make_pair(Status::NotFound(key), "");
  }

  std::string value;
  auto blob = ret.value.Blob();
  blob.Read(0, blob.size(), &value);
  return std::make_pair(Status::OK(), value);

}

}  // namespace ustore_kvdb
