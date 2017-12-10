// Copyright (c) 2017 The Ustore Authors.

#include <utility>

#include "utils/logging.h"
#include "utils/utils.h"
#include "worker/rocksdb_head_version.h"

namespace ustore {

RocksDBHeadVersion::RocksDBHeadVersion() {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = false;

  auto db_stat = rocksdb::DB::Open(options, "/tmp/testdb", &db_);
  if (db_stat.ok()) {
    LOG(INFO) << "RocksDB is successfully opened";
  } else {
    LOG(ERROR) << db_stat.ToString();
  }
}

RocksDBHeadVersion::~RocksDBHeadVersion() {
  delete db_;
}

boost::optional<Hash> RocksDBHeadVersion::GetBranch(const Slice& key,
    const Slice& branch) const {
  std::string db_key(DBKey(key, branch));
  std::string ver;
  auto db_stat =
    db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(db_key), &ver);

  return db_stat.ok()
         ? boost::make_optional(SLICE_TO_HASH(Slice(ver)))
         : boost::none;
}

std::vector<Hash> RocksDBHeadVersion::GetLatest(const Slice& key) const {
  std::string db_key(DBKey(key));
  std::string versions_str;
  auto db_stat =
    db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(db_key), &versions_str);

  if (db_stat.ok()) {
    std::vector<Hash> latest;
    for (size_t i = 0; i < versions_str.size(); i += Hash::kByteLength) {
      auto ver = versions_str.substr(i, Hash::kByteLength);
      latest.emplace_back(SLICE_TO_HASH(Slice(ver)));
    }
    return latest;
  } else {
    DLOG(INFO) << "No data exists for Key \"" << key << "\"";
    static const std::vector<Hash> empty;
    return empty;
  }
}

void RocksDBHeadVersion::PutBranch(const Slice& key, const Slice& branch,
                                   const Hash& ver) {
  std::string db_key(DBKey(key, branch));
  std::string ver_str(HASH_TO_SLICE(ver).ToString());
  auto db_stat = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(db_key),
                          rocksdb::Slice(ver_str));
  DCHECK(db_stat.ok()) << "fail to insert new branch into head table";
}

void RocksDBHeadVersion::PutLatest(const Slice& key, const Hash& prev_ver1,
                                   const Hash& prev_ver2, const Hash& ver) {
  std::string db_key(DBKey(key));
  std::string ver_str(HASH_TO_SLICE(ver).ToString());
  // retrieve current versions
  std::string versions_str;
  auto db_stat = db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(db_key),
                          &versions_str);
  // update versions
  if (db_stat.ok()) {
    std::stringstream ss;
    for (size_t i = 0; i < versions_str.size(); i += Hash::kByteLength) {
      auto v_str = versions_str.substr(i, Hash::kByteLength);
      auto v = SLICE_TO_HASH(Slice(v_str));
      if (v != prev_ver1 && (prev_ver2.empty() || v != prev_ver2)) ss << v;
    }
    ss << ver_str;
    db_stat = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(db_key),
                       rocksdb::Slice(ss.str()));
  } else { // this is an initial put
    db_stat = db_->Put(rocksdb::WriteOptions(), rocksdb::Slice(db_key),
                       rocksdb::Slice(ver_str));
  }
  DCHECK(db_stat.ok());
}

void RocksDBHeadVersion::DeleteBranch(const Slice& key, const Slice& branch) {
  std::string db_key(DBKey(key));
  auto db_stat = db_->Delete(rocksdb::WriteOptions(), rocksdb::Slice(db_key));
  CHECK(db_stat.ok()) << db_stat.ToString();
}

void RocksDBHeadVersion::RemoveBranch(const Slice& key, const Slice& branch) {
  if (Exists(key, branch)) {
    DeleteBranch(key, branch);
  } else {
    LOG(WARNING) << "Branch \"" << branch << "for Key \"" << key
                 << "\" does not exist!";
  }
}

void RocksDBHeadVersion::RenameBranch(const Slice& key, const Slice& old_branch,
                                      const Slice& new_branch) {
  DCHECK(Exists(key, old_branch)) << ": Branch \"" << old_branch
                                  << "\" for Key \"" << key
                                  << "\" does not exist!";
  DCHECK(!Exists(key, new_branch)) << ": Branch \"" << new_branch
                                   << "\" for Key \"" << key
                                   << "\" already exists!";
  auto head_opt = GetBranch(key, old_branch);
  DCHECK(head_opt);
  PutBranch(key, new_branch, *head_opt);
  DeleteBranch(key, old_branch);
}

bool RocksDBHeadVersion::Exists(const Slice& key) const {
  std::string db_key(DBKey(key));
  static std::string versions_str;
  return db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(db_key),
                  &versions_str).ok();
}

bool RocksDBHeadVersion::Exists(const Slice& key, const Slice& branch) const {
  std::string db_key(DBKey(key, branch));
  static std::string ver;
  return db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(db_key), &ver).ok();
}

bool RocksDBHeadVersion::IsBranchHead(const Slice& key, const Slice& branch,
                                      const Hash& ver) const {
  auto head_opt = GetBranch(key, branch);
  return head_opt ? *head_opt == ver : false;
}

bool RocksDBHeadVersion::IsLatest(const Slice& key, const Hash& ver) const {
  std::string db_key(DBKey(key));
  std::string ver_str(HASH_TO_SLICE(ver).ToString());
  std::string versions_str;
  auto db_stat =
    db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(db_key), &versions_str);
  if (!db_stat.ok()) return false;
  for (size_t i = 0; i < versions_str.size(); i += Hash::kByteLength) {
    if (versions_str.substr(i, Hash::kByteLength) == ver_str) return true;
  }
  return false;
}

// std::vector<Slice> RocksDBHeadVersion::ListKey() const {
// }

// std::vector<Slice> RocksDBHeadVersion::ListBranch(const Slice& key) const {
// }

}  // namespace ustore
