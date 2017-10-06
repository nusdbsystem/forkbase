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

namespace ustore {

namespace fs = boost::filesystem;

Worker::Worker(const WorkerID& id, const Partitioner* ptt, bool pst)
  : StoreInitializer(id, pst), id_(id), factory_(ptt) {
  if (persist()) {
    // load head file
    std::string head_path = dataPath() + ".head";
    if (fs::exists(fs::path(head_path))) head_ver_.LoadBranchVersion(head_path);

    // load latest versions
    auto store = store::GetChunkStore();
    for (auto it = store->begin(); it != store->end(); ++it) {
      Chunk chunk = *it;
      if (chunk.type() == ChunkType::kCell)
        UpdateLatestVersion(UCell(std::move(chunk)));
    }
  }
}

Worker::~Worker() {
  // dump head file
  if (persist()) {
    std::string head_path = dataPath() + ".head";
    head_ver_.DumpBranchVersion(head_path);
  }
}

ErrorCode Worker::Get(const Slice& key, const Slice& branch, UCell* ucell)
    const {
  const auto& version_opt = head_ver_.GetBranch(key, branch);
  if (!version_opt) {
    if (Exists(key)) {
      LOG(WARNING) << "Branch \"" << branch << "\" for Key \"" << key
                   << "\" does not exist!";
      return ErrorCode::kBranchNotExists;
    } {
      LOG(WARNING) << "Key \"" << key << "\" does not exist!";
      return ErrorCode::kKeyNotExists;
    }
  }
  return Get(key, *version_opt, ucell);
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
         ? Write(key, val, prev_ver, empty_hash, ver)
         : ErrorCode::kReferringVersionNotExist;
}

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Slice& tgt_branch, const Slice& ref_branch,
                        Hash* ver) {
  const auto& ref_ver_opt = head_ver_.GetBranch(key, ref_branch);
  if (!ref_ver_opt) {
    LOG(ERROR) << "Branch \"" << ref_branch << "\" for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  return Merge(key, val, tgt_branch, *ref_ver_opt, ver);
}

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Slice& tgt_branch, const Hash& ref_ver,
                        Hash* ver) {
  const auto& tgt_ver_opt = head_ver_.GetBranch(key, tgt_branch);
  if (!tgt_ver_opt) {
    LOG(ERROR) << "Branch \"" << tgt_branch << "\" for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  auto ec = Merge(key, val, *tgt_ver_opt, ref_ver, ver);
  if (ec == ErrorCode::kOK) head_ver_.PutBranch(key, tgt_branch, *ver);
  return ec;
}

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Hash& ref_ver1, const Hash& ref_ver2, Hash* ver) {
  return (Exists(ref_ver1) && Exists(ref_ver2))
         ? Write(key, val, ref_ver1, ref_ver2, ver)
         : ErrorCode::kReferringVersionNotExist;
}

ErrorCode Worker::Write(const Slice& key, const Value& val,
                        const Hash& prev_ver1, const Hash& prev_ver2,
                        Hash* ver) {
  ErrorCode ec = ErrorCode::kTypeUnsupported;
  switch (val.type) {
    case UType::kBlob:
      ec = WriteBlob(key, val, prev_ver1, prev_ver2, ver);
      break;
    case UType::kString:
      ec = WriteString(key, val, prev_ver1, prev_ver2, ver);
      break;
    case UType::kList:
      ec = WriteList(key, val, prev_ver1, prev_ver2, ver);
      break;
    case UType::kMap:
      ec = WriteMap(key, val, prev_ver1, prev_ver2, ver);
      break;
    case UType::kSet:
      ec = WriteSet(key, val, prev_ver1, prev_ver2, ver);
      break;
    default:
      LOG(WARNING) << "Unsupported data type: " << static_cast<int>(val.type);
  }
  return ec;
}

ErrorCode Worker::WriteBlob(const Slice& key, const Value& val,
                            const Hash& prev_ver1, const Hash& prev_ver2,
                            Hash* ver) {
  DCHECK(val.type == UType::kBlob);
  if (val.base.empty()) {  // new insertion
    if (val.vals.size() != 1) return ErrorCode::kInvalidValue;
    SBlob sblob = factory_.Create<SBlob>(val.vals.front());
    if (sblob.empty()) {
      LOG(ERROR) << "Failed to create SBlob for Key \"" << key << "\"";
      return ErrorCode::kFailedCreateSBlob;
    }
    return CreateUCell(key, UType::kBlob, sblob.hash(), prev_ver1, prev_ver2,
                       ver);
  } else if (!val.dels && val.vals.empty()) {  // origin
    return CreateUCell(key, UType::kBlob, val.base, prev_ver1, prev_ver2, ver);
  } else {  // update
    if (val.vals.size() > 1) return ErrorCode::kInvalidValue;
    SBlob sblob = factory_.Load<SBlob>(val.base);
    Slice slice = val.vals.empty() ? Slice() : val.vals.front();
    auto data_hash = sblob.Splice(val.pos, val.dels, slice.data(), slice.len());
    if (data_hash == Hash::kNull) return ErrorCode::kFailedModifySBlob;
    return CreateUCell(key, UType::kBlob, data_hash, prev_ver1, prev_ver2, ver);
  }
}

ErrorCode Worker::WriteString(const Slice& key, const Value& val,
                              const Hash& prev_ver1, const Hash& prev_ver2,
                              Hash* ver) {
  DCHECK(val.type == UType::kString);
  if (val.base.empty()) {  // new insertion
    if (val.vals.size() != 1) return ErrorCode::kInvalidValue;
    return CreateUCell(key, UType::kString, val.vals.front(), prev_ver1,
                       prev_ver2, ver);
  } else if (!val.dels && val.vals.empty()) {  // origin
    LOG(WARNING) << "String type does not support loading original value";
    return ErrorCode::kInvalidValue;
  } else {  // update
    LOG(WARNING) << val.dels << " " << val.vals.size();
    return ErrorCode::kInvalidValue;
  }
}

ErrorCode Worker::WriteList(const Slice& key, const Value& val,
                            const Hash& prev_ver1, const Hash& prev_ver2,
                            Hash* ver) {
  DCHECK(val.type == UType::kList);
  if (val.base.empty()) {  // new insertion
    SList list = factory_.Create<SList>(val.vals);
    if (list.empty()) {
      LOG(ERROR) << "Failed to create SList for Key \"" << key << "\"";
      return ErrorCode::kFailedCreateSList;
    }
    return CreateUCell(key, UType::kList, list.hash(), prev_ver1, prev_ver2,
                       ver);
  } else if (!val.dels && val.vals.empty()) {  // origin
    return CreateUCell(key, UType::kList, val.base, prev_ver1, prev_ver2, ver);
  } else {  // update
    SList list = factory_.Load<SList>(val.base);
    auto data_hash = list.Splice(val.pos, val.dels, val.vals);
    if (data_hash == Hash::kNull) return ErrorCode::kFailedModifySList;
    return CreateUCell(key, UType::kList, data_hash, prev_ver1, prev_ver2, ver);
  }
}

ErrorCode Worker::WriteMap(const Slice& key, const Value& val,
                           const Hash& prev_ver1, const Hash& prev_ver2,
                           Hash* ver) {
  DCHECK(val.type == UType::kMap);
  if (val.base.empty()) {  // new insertion
    if (val.keys.size() != val.vals.size()) return ErrorCode::kInvalidValue;
    SMap map = factory_.Create<SMap>(val.keys, val.vals);
    if (map.empty()) {
      LOG(ERROR) << "Failed to create SMap for Key \"" << key << "\"";
      return ErrorCode::kFailedCreateSMap;
    }
    return CreateUCell(key, UType::kMap, map.hash(), prev_ver1, prev_ver2,
                       ver);
  } else if (!val.dels && val.vals.empty()) {  // origin
    return CreateUCell(key, UType::kMap, val.base, prev_ver1, prev_ver2, ver);
  } else {  // update
    if (val.keys.size() != 1 || val.keys.size() < val.vals.size())
      return ErrorCode::kInvalidValue;
    auto mkey = val.keys.front();
    SMap map = factory_.Load<SMap>(val.base);
    Hash data_hash;
    if (val.dels) {
      data_hash = map.Remove(mkey);
    } else {
      DCHECK_EQ(val.vals.size(), 1);
      data_hash = map.Set(mkey, val.vals.front());
    }
    if (data_hash == Hash::kNull) return ErrorCode::kFailedModifySMap;
    return CreateUCell(key, UType::kMap, data_hash, prev_ver1, prev_ver2, ver);
  }
}

ErrorCode Worker::WriteSet(const Slice& key, const Value& val,
                           const Hash& prev_ver1, const Hash& prev_ver2,
                           Hash* ver) {
  DCHECK(val.type == UType::kSet);
  if (val.base.empty()) {  // new insertion
    SSet set = factory_.Create<SSet>(val.keys);
    if (set.empty()) {
      LOG(ERROR) << "Failed to create SSet for Key \"" << key << "\"";
      return ErrorCode::kFailedCreateSSet;
    }
    return CreateUCell(key, UType::kSet, set.hash(), prev_ver1, prev_ver2,
                       ver);
  } else if (!val.dels && val.keys.empty()) {  // origin
    return CreateUCell(key, UType::kSet, val.base, prev_ver1, prev_ver2, ver);
  } else {  // update
    if (val.keys.size() != 1)
      return ErrorCode::kInvalidValue;
    auto mkey = val.keys.front();
    SSet set = factory_.Load<SSet>(val.base);
    Hash data_hash;
    if (val.dels) {
      data_hash = set.Remove(mkey);
    } else {
      DCHECK_EQ(val.keys.size(), 1);
      data_hash = set.Set(mkey);
    }
    if (data_hash == Hash::kNull) return ErrorCode::kFailedModifySSet;
    return CreateUCell(key, UType::kSet, data_hash, prev_ver1, prev_ver2, ver);
  }
}

ErrorCode Worker::CreateUCell(const Slice& key, const UType& utype,
                              const Slice& utype_data, const Hash& prev_ver1,
                              const Hash& prev_ver2, Hash* ver) {
  UCell ucell(UCell::Create(utype, key, utype_data, prev_ver1, prev_ver2));
  if (ucell.empty()) {
    LOG(ERROR) << "Failed to create UCell(Primitive) for Key \"" << key << "\"";
    return ErrorCode::kFailedCreateUCell;
  }
  *ver = ucell.hash().Clone();
  UpdateLatestVersion(ucell);
  return ErrorCode::kOK;
}

ErrorCode Worker::CreateUCell(const Slice& key, const UType& utype,
                              const Hash& utype_hash, const Hash& prev_ver1,
                              const Hash& prev_ver2, Hash* ver) {
  UCell ucell(UCell::Create(utype, key, utype_hash, prev_ver1, prev_ver2));
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
  const auto& version_opt = head_ver_.GetBranch(key, old_branch);
  if (!version_opt) {
    LOG(ERROR) << "Branch \"" << old_branch << "\" for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  return Branch(key, *version_opt, new_branch);
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

ErrorCode Worker::GetChunk(const Slice& key, const Hash& ver, Chunk* chunk)
    const {
  static const auto chunk_store = store::GetChunkStore();
  *chunk = chunk_store->Get(ver);
  if (chunk->empty()) return ErrorCode::kChunkNotExists;
  return ErrorCode::kOK;
}

ErrorCode Worker::GetStorageInfo(std::vector<StoreInfo>* info) const {
  static const auto chunk_store = store::GetChunkStore();
  info->push_back(chunk_store->GetInfo());
  return ErrorCode::kOK;
}

ErrorCode Worker::ListKeys(std::vector<std::string>* keys) const {
  keys->clear();
  for (auto& k : head_ver_.ListKey()) {
    keys->emplace_back(k.ToString());
  }
  return ErrorCode::kOK;
}

ErrorCode Worker::ListBranches(const Slice& key,
                               std::vector<std::string>* branches) const {
  branches->clear();
  for (auto& b : head_ver_.ListBranch(key)) {
    branches->emplace_back(b.ToString());
  }
  return ErrorCode::kOK;
}

bool Worker::Exists(const Hash& ver) const {
  static const auto chunk_store = store::GetChunkStore();
  return chunk_store->Exists(ver);
}

}  // namespace ustore
