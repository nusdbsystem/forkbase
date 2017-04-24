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
  auto& version_opt = head_ver_.GetBranch(key, branch);
  if (!version_opt) {
    LOG(WARNING) << "Branch \"" << branch << "\" for Key \"" << key
                 << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  return Get(key, *version_opt, val);
}

ErrorCode Worker::Get(const Slice& key, const Hash& ver, Value* val) {
  DCHECK_NE(ver, Hash::kNull);
  UCell ucell(UCell::Load(ver));
  if (ucell.empty()) {
    LOG(WARNING) << "Data version \"" << ver << "\" does not exists!";
    return ErrorCode::kUCellNotfound;
  }
  return Read(ucell, val);
}

ErrorCode Worker::Read(const UCell& ucell, Value* val) const {
  DCHECK_NE(ucell, nullptr);
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
  UBlob ublob(UBlob::Load(ucell.dataHash()));
  size_t n_bytes = ublob.size();
  byte_t* buffer = new byte_t[n_bytes];  // Note: potential memory leak
  n_bytes = ublob.Read(0, n_bytes, buffer);
  DCHECK_EQ(ublob.size(), n_bytes);
  *val = Value(Blob(buffer, n_bytes));
  return ErrorCode::kOK;
}

ErrorCode Worker::ReadString(const UCell& ucell, Value* val) const {
  UString ustring(UString::Load(ucell.dataHash()));
  size_t n_bytes = ustring.len();
  char* buffer = new char[n_bytes];  // Note: potential memory leak
  n_bytes = ustring.data(reinterpret_cast<byte_t*>(buffer));
  DCHECK_EQ(ustring.len(), n_bytes);
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
  ErrorCode ec = Write(key, val, prev_ver, Hash::kNull, ver);
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
  UBlob ublob(UBlob::Create(blob.data(), blob.size()));
  if (ublob.empty()) {
    LOG(ERROR) << "Failed to create UBlob for Key \"" << key << "\"";
    return ErrorCode::kFailedCreateUBlob;
  }
  return CreateUCell(key, UType::kBlob, ublob.hash(), prev_ver1, prev_ver2,
                     ver);
}

ErrorCode Worker::WriteString(const Slice& key, const Value& val,
                              const Hash& prev_ver1, const Hash& prev_ver2,
                              Hash* ver) {
  Slice slice = val.slice();
  UString ustring(UString::Create(reinterpret_cast<const byte_t*>(slice.data()),
                                  slice.len()));
  if (ustring.empty()) {
    LOG(ERROR) << "Failed to create UString for Key \"" << key << "\"";
    return ErrorCode::kFailedCreateUString;
  }
  return CreateUCell(key, UType::kString, ustring.hash(), prev_ver1, prev_ver2,
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
  auto& version_opt = head_ver_.GetBranch(key, old_branch);
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

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Slice& tgt_branch, const Slice& ref_branch,
                        Hash* ver) {
  auto& ref_ver_opt = head_ver_.GetBranch(key, ref_branch);
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
  auto& tgt_ver_opt = head_ver_.GetBranch(key, tgt_branch);
  if (!tgt_ver_opt) {
    LOG(ERROR) << "Branch \"" << tgt_branch << "\" for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  ErrorCode ec = Write(key, val, *tgt_ver_opt, ref_ver, ver);
  if (ec == ErrorCode::kOK) head_ver_.PutBranch(key, tgt_branch, *ver);
  return ec;
}

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Hash& ref_ver1, const Hash& ref_ver2, Hash* ver) {
  return Write(key, val, ref_ver1, ref_ver2, ver);
}

}  // namespace ustore
