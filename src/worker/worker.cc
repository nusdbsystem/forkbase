// Copyright (c) 2017 The Ustore Authors.

#include "worker/worker.h"

#include <memory>
#include "spec/blob.h"
#include "types/ublob.h"
#include "types/ustring.h"
#include "utils/logging.h"

namespace ustore {

const Slice Worker::kNullBranch{""};

ErrorCode Worker::EitherBranchOrVersion(
  const Slice& branch, std::function<ErrorCode()> f_run_for_branch,
  const Hash& ver, std::function<ErrorCode()> f_run_for_version) const {
  if (ver.empty() && branch.empty()) {
    LOG(ERROR) << "Branch and version cannot be both empty!";
    return ErrorCode::kInvalidParameters;
  }
  if (!ver.empty() && !branch.empty()) {
    LOG(ERROR) << "Branch and version cannot be both set!";
    return ErrorCode::kInvalidParameters;
  }
  return ver.empty() ? f_run_for_branch() : f_run_for_version();
}

const Hash Worker::GetBranchHead(const Slice& key, const Slice& branch) const {
  auto& ver_opt = head_ver_.Get(key, branch);
  return ver_opt ? *ver_opt : Hash::kNull;
}

ErrorCode Worker::Get(const Slice& key, const Slice& branch, const Hash& ver,
                      Value* val) const {
  return EitherBranchOrVersion(
  branch, [&]() { return Get(key, branch, val); },
  ver, [&]() { return Get(key, ver, val); });
}

ErrorCode Worker::Get(const Slice& key, const Slice& branch, Value* val) const {
  auto& version_opt = head_ver_.Get(key, branch);
  if (!version_opt) {
    LOG(WARNING) << "Branch \"" << branch << "for Key \"" << key
                 << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  return Get(key, *version_opt, val);
}

ErrorCode Worker::Get(const Slice& key, const Hash& ver, Value* val) const {
  DCHECK_NE(ver, Hash::kNull);
  // ensure to release memory
  std::unique_ptr<const UCell> ucell(UCell::Load(ver));
  if (ucell == nullptr) {
    LOG(WARNING) << "Data ver \"" << ver << "\" does not exists!";
    return ErrorCode::kUCellNotfound;
  }
  return Read(ucell.get(), val);
}

ErrorCode Worker::Read(const UCell* ucell, Value* val) const {
  DCHECK_NE(ucell, nullptr);
  ErrorCode ec = ErrorCode::kTypeUnsupported;
  switch (ucell->type()) {
    case UType::kBlob:
      ec = ReadBlob(ucell, val);
      break;
    case UType::kString:
      ec = ReadString(ucell, val);
      break;
    default:
      LOG(WARNING) << "Unsupported data type: "
                   << static_cast<int>(ucell->type());
  }
  return ec;
}

ErrorCode Worker::ReadBlob(const UCell* ucell, Value* val) const {
  // ensure to release memory
  std::unique_ptr<const UBlob> ublob(UBlob::Load(ucell->dataHash()));
  size_t n_bytes = ublob->size();
  byte_t* buffer = new byte_t[n_bytes];  // Note: potential memory leak
  n_bytes = ublob->Read(0, n_bytes, buffer);
  DCHECK_EQ(ublob->size(), n_bytes);
  *val = Value(Blob(buffer, n_bytes));
  return ErrorCode::kOK;
}

ErrorCode Worker::ReadString(const UCell* ucell, Value* val) const {
  // ensure to release memory
  std::unique_ptr<const UString> ustring(UString::Load(ucell->dataHash()));
  size_t n_bytes = ustring->len();
  char* buffer = new char[n_bytes];  // Note: potential memory leak
  n_bytes = ustring->data(reinterpret_cast<byte_t*>(buffer));
  DCHECK_EQ(ustring->len(), n_bytes);
  *val = Value(Slice(buffer, n_bytes));
  return ErrorCode::kOK;
}

ErrorCode Worker::Put(const Slice& key, const Value& val, const Slice& branch,
                      const Hash& prev_ver, Hash* ver) {
  if (branch.empty()) return Put(key, val, prev_ver, ver);
  ErrorCode ec = Write(key, val, prev_ver, Hash(), ver);
  if (ec == ErrorCode::kOK) head_ver_.Put(key, branch, *ver);
  return ec;
}

ErrorCode Worker::Put(const Slice& key, const Value& val,
                      const Hash& prev_ver, Hash* ver) {
  ErrorCode ec = Write(key, val, prev_ver, Hash(), ver);
  if (ec == ErrorCode::kOK) head_ver_.Put(key, prev_ver, *ver);
  return ec;
}

ErrorCode Worker::Write(const Slice& key, const Value& val,
                        const Hash& prev_ver1, const Hash& prev_ver2,
                        Hash* ver) const {
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
                            Hash* ver) const {
  Blob blob = val.blob();
  // ensure to release memory
  std::unique_ptr<const UBlob> ublob(UBlob::Create(blob.data(), blob.size()));
  if (ublob == nullptr) {
    LOG(ERROR) << "Failed to create UBlob for Key \"" << key << "\"";
    return ErrorCode::kFailedCreateUBlob;
  }
  return CreateUCell(key, UType::kBlob, ublob->hash(), prev_ver1, prev_ver2,
                     ver);
}

ErrorCode Worker::WriteString(const Slice& key, const Value& val,
                              const Hash& prev_ver1, const Hash& prev_ver2,
                              Hash* ver) const {
  Slice slice = val.slice();
  // ensure to release memory
  std::unique_ptr<const UString> ustring(
      UString::Create(reinterpret_cast<const byte_t*>(slice.data()),
                      slice.len()));
  if (ustring == nullptr) {
    LOG(ERROR) << "Failed to create UString for Key \"" << key << "\"";
    return ErrorCode::kFailedCreateUString;
  }
  return CreateUCell(key, UType::kString, ustring->hash(), prev_ver1, prev_ver2,
                     ver);
}

ErrorCode Worker::CreateUCell(const Slice& key, const UType& utype,
                              const Hash& utype_hash, const Hash& prev_ver1,
                              const Hash& prev_ver2, Hash* ver) const {
  // ensure to release memory
  std::unique_ptr<const UCell> ucell(UCell::Create(utype, utype_hash, prev_ver1,
                                                   prev_ver2));
  if (ucell == nullptr) {
    LOG(ERROR) << "Failed to create UCell for Key \"" << key << "\"";
    return ErrorCode::kFailedCreateUCell;
  }
  *ver = ucell->hash().Clone();
  return ErrorCode::kOK;
}

ErrorCode Worker::Branch(const Slice& key, const Slice& old_branch,
                         const Hash& ver, const Slice& new_branch) {
  return EitherBranchOrVersion(
  old_branch, [&]() { return Branch(key, old_branch, new_branch); },
  ver, [&]() { return Branch(key, ver, new_branch); });
}

ErrorCode Worker::Branch(const Slice& key, const Slice& old_branch,
                         const Slice& new_branch) {
  auto& version_opt = head_ver_.Get(key, old_branch);
  if (!version_opt) {
    LOG(ERROR) << "Branch \"" << old_branch << "for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  return Branch(key, *version_opt, new_branch);
}

ErrorCode Worker::Branch(const Slice& key, const Hash& ver,
                         const Slice& new_branch) {
  if (head_ver_.Exists(key, new_branch)) {
    LOG(ERROR) << "Branch \"" << new_branch << "for Key \"" << key
               << "\" already exists!";
    return ErrorCode::kBranchExists;
  }
  head_ver_.Put(key, new_branch, ver, false);
  return ErrorCode::kOK;
}

ErrorCode Worker::Move(const Slice& key, const Slice& old_branch,
                       const Slice& new_branch) {
  if (!head_ver_.Exists(key, old_branch)) {
    LOG(ERROR) << "Branch \"" << old_branch << "for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  if (head_ver_.Exists(key, new_branch)) {
    LOG(ERROR) << "Branch \"" << new_branch << "for Key \"" << key
               << "\" already exists!";
    return ErrorCode::kBranchExists;
  }
  head_ver_.RenameBranch(key, old_branch, new_branch);
  return ErrorCode::kOK;
}

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Slice& tgt_branch, const Slice& ref_branch,
                        const Hash& ref_ver, Hash* ver) {
  return EitherBranchOrVersion(
  ref_branch, [&]() { return Merge(key, val, tgt_branch, ref_branch, ver); },
  ref_ver, [&]() { return Merge(key, val, tgt_branch, ref_ver, ver); });
}

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Slice& tgt_branch, const Slice& ref_branch,
                        Hash* ver) {
  auto& ref_ver_opt = head_ver_.Get(key, ref_branch);
  if (!ref_ver_opt) {
    LOG(ERROR) << "Branch \"" << ref_branch << "for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  return Merge(key, val, tgt_branch, *ref_ver_opt, ver);
}

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Slice& tgt_branch, const Hash& ref_ver,
                        Hash* ver) {
  auto& tgt_ver_opt = head_ver_.Get(key, tgt_branch);
  if (!tgt_ver_opt) {
    LOG(ERROR) << "Branch \"" << tgt_branch << "for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  ErrorCode ec = Write(key, val, *tgt_ver_opt, ref_ver, ver);
  if (ec == ErrorCode::kOK) head_ver_.Put(key, tgt_branch, *ver);
  return ec;
}

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Hash& ref_ver1, const Hash& ref_ver2,
                        Hash* ver) {
  auto ec = Write(key, val, ref_ver1, ref_ver2, ver);
  if (ec == ErrorCode::kOK) head_ver_.Merge(key, ref_ver1, ref_ver2, *ver);
  return ec;
}

}  // namespace ustore
