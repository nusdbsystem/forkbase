// Copyright (c) 2017 The Ustore Authors.

#include "worker/worker.h"

#include <boost/filesystem.hpp>
#include <memory>
#include "types/server/sblob.h"
#include "types/server/slist.h"
#include "types/server/smap.h"
#include "types/server/sset.h"
#include "types/server/sstring.h"
#include "utils/env.h"
#include "utils/logging.h"
#include "utils/utils.h"

namespace ustore {

namespace fs = boost::filesystem;

Worker::Worker(const WorkerID& id, const Partitioner* ptt, bool pst)
  : StoreInitializer(id, pst), id_(id), factory_(ptt) {
#if defined(USE_SIMPLE_HEAD_VERSION)
  if (persist()) {
    // load branch versions
    const std::string log_path(dataPath() + ".head");
    if (fs::exists(fs::path(log_path))) head_ver_.Load(log_path);

    // load latest versions
    auto store = store::GetChunkStore();
    for (auto it = store->begin(); it != store->end(); ++it) {
      Chunk chunk = *it;
      if (chunk.type() == ChunkType::kCell)
        UpdateLatestVersion(UCell(std::move(chunk)));
    }
  }
#else
  if (persist()) {
    head_ver_.Load(dataPath() + ".head");
  } else {
    const std::string tmp_db("/tmp/ustore.head-" + std::to_string(id_));
    RocksHeadVersion::DestroyDB(tmp_db);
    head_ver_.Load(tmp_db);
  }
#endif
}

Worker::~Worker() {
#if defined(USE_SIMPLE_HEAD_VERSION)
  if (persist()) {
    std::string log_path(dataPath() + ".head");
    head_ver_.Dump(log_path);
  }
#else
  if (persist()) {
    head_ver_.Dump(dataPath() + ".head.db");
    head_ver_.CloseDB();
  } else {
    head_ver_.DestroyDB();
  }
#endif
}

ErrorCode Worker::Get(const Slice& key, const Slice& branch, UCell* ucell)
const {
  Hash ver;
  if (!head_ver_.GetBranch(key, branch, &ver)) {
    if (Exists(key)) {
      LOG(WARNING) << "Branch \"" << branch << "\" for Key \"" << key
                   << "\" does not exist!";
      return ErrorCode::kBranchNotExists;
    } {
      LOG(WARNING) << "Key \"" << key << "\" does not exist!";
      return ErrorCode::kKeyNotExists;
    }
  }
  return Get(key, ver, ucell);
}

ErrorCode Worker::Get(const Slice& key, const Hash& ver, UCell* ucell) const {
  DCHECK_NE(ver, Hash::kNull);
  *ucell = UCell::Load(ver);
  if (ucell->empty()) {
    LOG(WARNING) << "Data version \"" << ver << "\" does not exists!";
    return ErrorCode::kUCellNotExists;
  }
  if (ucell->key() != key) {
    LOG(ERROR) << "Inconsistent data key: [Expected] " << key
               << ", [Actual] " << ucell->key();
    return ErrorCode::kInconsistentKey;
  }
  return ErrorCode::kOK;
}

ErrorCode Worker::Put(const Slice& key, const Value& val, const Slice& branch,
                      const Hash& prev_ver, Hash* ver) {
  const auto ec = Put(key, val, prev_ver, ver);
  if (ec == ErrorCode::kOK && !branch.empty()) {
    head_ver_.PutBranch(key, branch, *ver);
  }
  return ec;
}

ErrorCode Worker::Put(const Slice& key, const Value& val, const Hash& prev_ver,
                      Hash* ver) {
  static Hash empty_hash;
  return (prev_ver == Hash::kNull || Exists(prev_ver))
         ? WriteUCell(key, val, prev_ver, empty_hash, ver)
         : ErrorCode::kReferringVersionNotExist;
}

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Slice& tgt_branch, const Slice& ref_branch,
                        Hash* ver) {
  Hash ref_ver;
  if (!head_ver_.GetBranch(key, ref_branch, &ref_ver)) {
    LOG(ERROR) << "Branch \"" << ref_branch << "\" for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  return Merge(key, val, tgt_branch, ref_ver, ver);
}

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Slice& tgt_branch, const Hash& ref_ver,
                        Hash* ver) {
  Hash tgt_ver;
  if (!head_ver_.GetBranch(key, tgt_branch, &tgt_ver)) {
    LOG(ERROR) << "Branch \"" << tgt_branch << "\" for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  auto ec = Merge(key, val, tgt_ver, ref_ver, ver);
  if (ec == ErrorCode::kOK) head_ver_.PutBranch(key, tgt_branch, *ver);
  return ec;
}

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Hash& ref_ver1, const Hash& ref_ver2, Hash* ver) {
  return (Exists(ref_ver1) && Exists(ref_ver2))
         ? WriteUCell(key, val, ref_ver1, ref_ver2, ver)
         : ErrorCode::kReferringVersionNotExist;
}

ErrorCode Worker::WriteUCell(const Slice& key, const Value& val,
                            const Hash& prev_ver1, const Hash& prev_ver2, 
                            Hash* ver) {
  // for primitive types
  if (val.type == UType::kString) {
    USTORE_GUARD(CheckString(val));
    return CreateUCell(key, val.type, val.vals.front(), val.ctx, prev_ver1,
                       prev_ver2, ver);
  }
  // for chunkable types
  static Slice unused;
  Hash root;
  USTORE_GUARD(PutUnkeyed(unused, val, &root));
  // create UCell
  return CreateUCell(key, val.type, root, val.ctx, prev_ver1, prev_ver2, ver);
}

ErrorCode Worker::CheckString(const Value& val) const {
  DCHECK(val.type == UType::kString);
  if (val.base.empty()) {  // new insertion
    if (val.vals.size() != 1) return ErrorCode::kInvalidValue;
  } else if (!val.dels && val.vals.empty()) {  // origin
    LOG(WARNING) << "String type does not support loading original value";
    return ErrorCode::kInvalidValue;
  } else {  // update
    LOG(WARNING) << val.dels << " " << val.vals.size();
    return ErrorCode::kInvalidValue;
  }
  return ErrorCode::kOK;
}

ErrorCode Worker::WriteBlob(const Value& val, Hash* ver) {
  DCHECK(val.type == UType::kBlob);
  if (val.base.empty()) {  // new insertion
    if (val.vals.size() != 1) return ErrorCode::kInvalidValue;
    auto sblob = factory_.Create<SBlob>(val.vals.front());
    if (sblob.empty()) {
      LOG(ERROR) << "Failed to create SBlob";
      return ErrorCode::kFailedCreateSBlob;
    }
    *ver = sblob.hash().Clone();
  } else if (!val.dels && val.vals.empty()) {  // origin
    *ver = val.base;  // no need to clone base hash
  } else {  // update
    if (val.vals.size() > 1) return ErrorCode::kInvalidValue;
    auto sblob = factory_.Load<SBlob>(val.base);
    auto slice = val.vals.empty() ? Slice() : val.vals.front();
    auto data_hash = sblob.Splice(val.pos, val.dels, slice.data(), slice.len());
    if (data_hash == Hash::kNull) return ErrorCode::kFailedModifySBlob;
    *ver = data_hash.Clone();
  }
  return ErrorCode::kOK;
}

ErrorCode Worker::WriteList(const Value& val, Hash* ver) {
  DCHECK(val.type == UType::kList);
  if (val.base.empty()) {  // new insertion
    auto list = factory_.Create<SList>(val.vals);
    if (list.empty()) {
      LOG(ERROR) << "Failed to create SList";
      return ErrorCode::kFailedCreateSList;
    }
    *ver = list.hash().Clone();
  } else if (!val.dels && val.vals.empty()) {  // origin
    *ver = val.base;  // no need to clone base hash
  } else {  // update
    auto list = factory_.Load<SList>(val.base);
    auto data_hash = list.Splice(val.pos, val.dels, val.vals);
    if (data_hash == Hash::kNull) return ErrorCode::kFailedModifySList;
    *ver = data_hash.Clone();
  }
  return ErrorCode::kOK;
}

ErrorCode Worker::WriteMap(const Value& val, Hash* ver) {
  DCHECK(val.type == UType::kMap);
  if (val.base.empty()) {  // new insertion
    if (val.keys.size() != val.vals.size()) return ErrorCode::kInvalidValue;
    auto map = factory_.Create<SMap>(val.keys, val.vals);
    if (map.empty()) {
      LOG(ERROR) << "Failed to create SMap";
      return ErrorCode::kFailedCreateSMap;
    }
    *ver = map.hash().Clone();
  } else if (!val.dels && val.vals.empty()) {  // origin
    *ver = val.base;  // no need to clone base hash
  } else if (val.keys.size() > 1) {  // update multiple entries
    if (val.keys.size() != val.vals.size() || val.dels)
      return ErrorCode::kInvalidValue;
    auto map = factory_.Load<SMap>(val.base);
    auto data_hash = map.Set(val.keys, val.vals);
    if (data_hash == Hash::kNull) return ErrorCode::kFailedModifySMap;
    *ver = data_hash.Clone();
  } else {  // update single entry
    if (val.keys.size() != 1 || val.keys.size() < val.vals.size())
      return ErrorCode::kInvalidValue;
    auto mkey = val.keys.front();
    auto map = factory_.Load<SMap>(val.base);
    Hash data_hash;
    if (val.dels) {
      data_hash = map.Remove(mkey);
    } else {
      DCHECK_EQ(val.vals.size(), size_t(1));
      data_hash = map.Set(mkey, val.vals.front());
    }
    if (data_hash == Hash::kNull) return ErrorCode::kFailedModifySMap;
    *ver = data_hash.Clone();
  }
  return ErrorCode::kOK;
}

ErrorCode Worker::WriteSet(const Value& val, Hash* ver) {
  DCHECK(val.type == UType::kSet);
  if (val.base.empty()) {  // new insertion
    auto set = factory_.Create<SSet>(val.keys);
    if (set.empty()) {
      LOG(ERROR) << "Failed to create SSet";
      return ErrorCode::kFailedCreateSSet;
    }
    *ver = set.hash().Clone();
  } else if (!val.dels && val.keys.empty()) {  // origin
    *ver = val.base;  // no need to clone base hash
    // TODO(pingcheng): Support updating multiple entries
  } else {  // update single entry
    if (val.keys.size() != 1)
      return ErrorCode::kInvalidValue;
    auto mkey = val.keys.front();
    auto set = factory_.Load<SSet>(val.base);
    Hash data_hash;
    if (val.dels) {
      data_hash = set.Remove(mkey);
    } else {
      DCHECK_EQ(val.keys.size(), 1);
      data_hash = set.Set(mkey);
    }
    if (data_hash == Hash::kNull) return ErrorCode::kFailedModifySSet;
    *ver = data_hash.Clone();
  }
  return ErrorCode::kOK;
}

ErrorCode Worker::CreateUCell(const Slice& key, const UType& utype,
                              const Slice& utype_data, const Slice& ctx,
                              const Hash& prev_ver1, const Hash& prev_ver2,
                              Hash* ver) {
  auto ucell(UCell::Create(utype, key, utype_data, ctx, prev_ver1, prev_ver2));
  if (ucell.empty()) {
    LOG(ERROR) << "Failed to create UCell (Primitive) for Key \"" << key << "\"";
    return ErrorCode::kFailedCreateUCell;
  }
  *ver = ucell.hash().Clone();
  UpdateLatestVersion(ucell);
  return ErrorCode::kOK;
}

ErrorCode Worker::CreateUCell(const Slice& key, const UType& utype,
                              const Hash& utype_hash, const Slice& ctx,
                              const Hash& prev_ver1, const Hash& prev_ver2,
                              Hash* ver) {
  auto ucell(UCell::Create(utype, key, utype_hash, ctx, prev_ver1, prev_ver2));
  if (ucell.empty()) {
    LOG(ERROR) << "Failed to create UCell(Chunkable) for Key \"" << key << "\"";
    return ErrorCode::kFailedCreateUCell;
  }
  *ver = ucell.hash().Clone();
  UpdateLatestVersion(ucell);
  return ErrorCode::kOK;
}

ErrorCode Worker::Branch(const Slice& key, const Slice& old_branch,
                         const Slice& new_branch) {
  Hash ver;
  if (!head_ver_.GetBranch(key, old_branch, &ver)) {
    LOG(ERROR) << "Branch \"" << old_branch << "\" for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  return Branch(key, ver, new_branch);
}

ErrorCode Worker::Branch(const Slice& key, const Hash& ver,
                         const Slice& new_branch) {
  if (head_ver_.Exists(key, new_branch)) {
    LOG(ERROR) << "Branch \"" << new_branch << "\" for Key \"" << key
               << "\" already exists!";
    return ErrorCode::kBranchExists;
  }
  if (Exists(ver)) {
    head_ver_.PutBranch(key, new_branch, ver);
    return ErrorCode::kOK;
  } else {
    LOG(ERROR) << "Version \"" << ver << "\" for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kReferringVersionNotExist;
  }
}

ErrorCode Worker::Rename(const Slice& key, const Slice& old_branch,
                         const Slice& new_branch) {
  if (!head_ver_.Exists(key, old_branch)) {
    LOG(ERROR) << "Branch \"" << old_branch << "\" for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  if (head_ver_.Exists(key, new_branch)) {
    LOG(ERROR) << "Branch \"" << new_branch << "\" for Key \"" << key
               << "\" already exists!";
    return ErrorCode::kBranchExists;
  }
  head_ver_.RenameBranch(key, old_branch, new_branch);
  return ErrorCode::kOK;
}

ErrorCode Worker::Delete(const Slice& key, const Slice& branch) {
  if (!head_ver_.Exists(key, branch)) {
    LOG(WARNING) << "Branch \"" << branch << "\" for Key \"" << key
                 << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  head_ver_.RemoveBranch(key, branch);
  return ErrorCode::kOK;
}

ErrorCode Worker::PutUnkeyed(const Slice& ptt_key, const Value& value,
                             Hash* version) {
  switch (value.type) {
    case UType::kBlob: return WriteBlob(value, version);
    case UType::kList: return WriteList(value, version);
    case UType::kMap: return WriteMap(value, version);
    case UType::kSet: return WriteSet(value, version);
    default:
      LOG(WARNING) << "Unsupported data type: " << static_cast<int>(value.type);
      return ErrorCode::kTypeUnsupported;
  }
}

ErrorCode Worker::GetChunk(const Slice& ptt_key, const Hash& ver,
                           Chunk* chunk) const {
  static const auto chunk_store = store::GetChunkStore();
  *chunk = chunk_store->Get(ver);
  if (chunk->empty()) return ErrorCode::kChunkNotExists;
  return ErrorCode::kOK;
}

ErrorCode Worker::GetStorageInfo(std::vector<StoreInfo>* info) const {
#ifdef ENABLE_STORE_INFO
  static const auto chunk_store = store::GetChunkStore();
  info->push_back(chunk_store->GetInfo());
  return ErrorCode::kOK;
#else
  return ErrorCode::kStoreInfoUnavailable;
#endif
}

ErrorCode Worker::ListKeys(std::vector<std::string>* keys) const {
  keys->clear();
  for (auto && k : head_ver_.ListKey()) {
    keys->push_back(std::move(k));
  }
  return ErrorCode::kOK;
}

ErrorCode Worker::ListBranches(const Slice& key,
                               std::vector<std::string>* branches) const {
  branches->clear();
  for (auto && b : head_ver_.ListBranch(key)) {
    branches->push_back(std::move(b));
  }
  return ErrorCode::kOK;
}

bool Worker::Exists(const Hash& ver) const {
  static const auto chunk_store = store::GetChunkStore();
  return chunk_store->Exists(ver);
}

}  // namespace ustore
