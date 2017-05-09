// Copyright (c) 2017 The Ustore Authors.

#include "worker/worker.h"

#include <memory>
#include "spec/blob.h"
#include "types/ublob.h"
#include "types/ustring.h"
#include "utils/logging.h"

namespace ustore {

const Hash Worker::GetBranchHead(const Slice& key, const Slice& branch) const {
  const auto& ver_opt = head_ver_.GetBranch(key, branch);
  return ver_opt ? *ver_opt : Hash::kNull;
}

ErrorCode Worker::Get(const Slice& key, const Slice& branch, Value* val) {
  UCell ucell;
  const auto ec = Get(key, branch, &ucell);
  return ec == ErrorCode::kOK ? Read(ucell, val) : ec;
}

ErrorCode Worker::Get(const Slice& key, const Hash& ver, Value* val) {
  UCell ucell;
  const auto ec = Get(key, ver, &ucell);
  return ec == ErrorCode::kOK ? Read(ucell, val) : ec;
}

ErrorCode Worker::Get(const Slice& key, const Slice& branch, UCell* ucell) {
  const auto& version_opt = head_ver_.GetBranch(key, branch);
  if (!version_opt) {
    LOG(WARNING) << "Branch \"" << branch << "\" for Key \"" << key
                 << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  return Get(key, *version_opt, ucell);
}

ErrorCode Worker::Get(const Slice& key, const Hash& ver, UCell* ucell) {
  DCHECK_NE(ver, Hash::kNull);
  *ucell = UCell::Load(ver);
  if (ucell->empty()) {
    LOG(WARNING) << "Data version \"" << ver << "\" does not exists!";
    return ErrorCode::kUCellNotfound;
  }
  if (ucell->key() != key) {
    LOG(ERROR) << "Inconsistent data key: [Expected] " << key
               << ", [Actual] " << ucell->key();
    return ErrorCode::kInconsistentKey;
  }
  return ErrorCode::kOK;
}

ErrorCode Worker::Read(const UCell& ucell, Value* val) const {
  DCHECK(!ucell.empty());
  ErrorCode ec = ErrorCode::kTypeUnsupported;
  switch (ucell.type()) {
    case UType::kBlob:
      ec = ReadBlob(ucell, val);
      break;
    case UType::kString:
      ec = ReadString(ucell, val);
      break;
    default:
      LOG(WARNING) << "Unsupported data type: "
                   << static_cast<int>(ucell.type());
  }
  return ec;
}

ErrorCode Worker::ReadBlob(const UCell& ucell, Value* val) const {
  SBlob sblob(ucell.dataHash());
  size_t n_bytes = sblob.size();
  byte_t* buffer = new byte_t[n_bytes];  // Note: potential memory leak
  n_bytes = sblob.Read(0, n_bytes, buffer);
  DCHECK_EQ(sblob.size(), n_bytes);
  *val = Value(Blob(buffer, n_bytes));
  return ErrorCode::kOK;
}

ErrorCode Worker::ReadString(const UCell& ucell, Value* val) const {
  SString sstring(ucell.dataHash());
  size_t n_bytes = sstring.len();
  char* buffer = new char[n_bytes];  // Note: potential memory leak
  n_bytes = sstring.data(reinterpret_cast<byte_t*>(buffer));
  DCHECK_EQ(sstring.len(), n_bytes);
  *val = Value(Slice(buffer, n_bytes));
  return ErrorCode::kOK;
}

ErrorCode Worker::Put(const Slice& key, const Value& val, const Slice& branch,
                      Hash* ver) {
  return Put(key, val, branch, GetBranchHead(key, branch), ver);
}

ErrorCode Worker::Put(const Slice& key, const Value& val, const Slice& branch,
                      const Hash& prev_ver, Hash* ver) {
  if (branch.empty()) return Put(key, val, prev_ver, ver);
  const auto ec = Write(key, val, prev_ver, Hash::kNull, ver);
  if (ec == ErrorCode::kOK) head_ver_.PutBranch(key, branch, *ver);
  return ec;
}

ErrorCode Worker::Put(const Slice& key, const Value& val, const Hash& prev_ver,
                      Hash* ver) {
  return Write(key, val, prev_ver, Hash::kNull, ver);
}

ErrorCode Worker::Write(const Slice& key, const Value& val,
                        const Hash& prev_ver1, const Hash& prev_ver2,
                        Hash* ver) {
  ErrorCode ec = ErrorCode::kTypeUnsupported;
  switch (val.type()) {
    case UType::kBlob:
      ec = WriteBlob(key, val, prev_ver1, prev_ver2, ver);
      break;
    case UType::kString:
      ec = WriteString(key, val, prev_ver1, prev_ver2, ver);
      break;
    default:
      LOG(WARNING) << "Unsupported data type: " << static_cast<int>(val.type());
  }
  return ec;
}

ErrorCode Worker::WriteBlob(const Slice& key, const Value& val,
                            const Hash& prev_ver1, const Hash& prev_ver2,
                            Hash* ver) {
  Blob blob = val.blob();

  // TO change later
  const Slice slice(reinterpret_cast<const char*>(blob.data()),
                    blob.size());
  const SBlob sblob(slice);
  if (sblob.empty()) {
    LOG(ERROR) << "Failed to create SBlob for Key \"" << key << "\"";
    return ErrorCode::kFailedCreateSBlob;
  }
  return CreateUCell(key, UType::kBlob, sblob.hash(), prev_ver1, prev_ver2,
                     ver);
}

ErrorCode Worker::WriteString(const Slice& key, const Value& val,
                              const Hash& prev_ver1, const Hash& prev_ver2,
                              Hash* ver) {
  const Slice slice = val.slice();
  const SString sstring(slice);
  if (sstring.empty()) {
    LOG(ERROR) << "Failed to create ustring for Key \"" << key << "\"";
    return ErrorCode::kFailedCreateSString;
  }
  return CreateUCell(key, UType::kString, sstring.hash(), prev_ver1, prev_ver2,
                     ver);
}

ErrorCode Worker::Put(const Slice& key, const Value2& val,
                      const Slice& branch, Hash* ver) {
  return Put(key, val, branch, GetBranchHead(key, branch), ver);
}

ErrorCode Worker::Put(const Slice& key, const Value2& val, const Slice& branch,
                      const Hash& prev_ver, Hash* ver) {
  if (branch.empty()) return Put(key, val, prev_ver, ver);
  ErrorCode ec = Write(key, val, prev_ver, Hash::kNull, ver);
  if (ec == ErrorCode::kOK) head_ver_.PutBranch(key, branch, *ver);
  return ec;
}

ErrorCode Worker::Put(const Slice& key, const Value2& val,
                      const Hash& prev_ver, Hash* ver) {
  return Write(key, val, prev_ver, Hash::kNull, ver);
}

ErrorCode Worker::Write(const Slice& key, const Value2& val,
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
    default:
      LOG(WARNING) << "Unsupported data type: " << static_cast<int>(val.type);
  }
  return ec;
}

ErrorCode Worker::WriteBlob(const Slice& key, const Value2& val,
                            const Hash& prev_ver1, const Hash& prev_ver2,
                            Hash* ver) {
  DCHECK(val.type == UType::kBlob);
  if (val.vals.size() != 1) return ErrorCode::kInvalidValue2;
  const Slice slice = val.vals.front();
  if (val.base == Hash::kNull) { // new insertion
    SBlob sblob(slice);
    if (sblob.empty()) {
      LOG(ERROR) << "Failed to create SBlob for Key \"" << key << "\"";
      return ErrorCode::kFailedCreateSBlob;
    }
    return CreateUCell(key, UType::kBlob, sblob.hash(), prev_ver1, prev_ver2,
                       ver);
  } else { // update
    SBlob sblob(val.base);
    const auto data = reinterpret_cast<const byte_t*>(slice.data());
    const auto data_hash = sblob.Splice(val.pos, val.dels, data, slice.len());
    if (*ver == Hash::kNull) return ErrorCode::kFailedSpliceSBlob;
    return CreateUCell(key, UType::kBlob, data_hash, prev_ver1, prev_ver2,
                       ver);
  }
}

ErrorCode Worker::WriteString(const Slice& key, const Value2& val,
                              const Hash& prev_ver1, const Hash& prev_ver2,
                              Hash* ver) {
  DCHECK(val.type == UType::kString);
  if (val.vals.size() != 1) return ErrorCode::kInvalidValue2;
  SString sstr(val.vals.front());
  if (sstr.empty()) {
    LOG(ERROR) << "Failed to create SString for Key \"" << key << "\"";
    return ErrorCode::kFailedCreateSString;
  }
  return CreateUCell(key, UType::kString, sstr.hash(), prev_ver1, prev_ver2,
                     ver);
}

ErrorCode Worker::CreateUCell(const Slice& key, const UType& utype,
                              const Hash& utype_hash, const Hash& prev_ver1,
                              const Hash& prev_ver2, Hash* ver) {
  UCell ucell(UCell::Create(utype, key, utype_hash, prev_ver1, prev_ver2));
  if (ucell.empty()) {
    LOG(ERROR) << "Failed to create UCell for Key \"" << key << "\"";
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
  head_ver_.PutBranch(key, new_branch, ver);
  return ErrorCode::kOK;
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

}  // namespace ustore
