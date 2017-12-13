// Copyright (c) 2017 The Ustore Authors.

#include <iomanip>
#include <utility>
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"
#include "utils/logging.h"
#include "utils/utils.h"

#include "worker/rocksdb_head_version.h"

namespace ustore {

RocksDBHeadVersion::RocksDBHeadVersion()
  : db_(nullptr),
    db_opts_(rocksdb::Options()),
    db_read_opts_(rocksdb::ReadOptions()),
    db_write_opts_(rocksdb::WriteOptions()),
    db_flush_opts_(rocksdb::FlushOptions()) {
  db_opts_.create_if_missing = true;
  db_opts_.error_if_exists = false;
  db_opts_.prefix_extractor.reset(NewMetaPrefixTransform());
  rocksdb::BlockBasedTableOptions tab_opts;
  tab_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
  db_opts_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tab_opts));

  db_write_opts_.disableWAL = true;
}

RocksDBHeadVersion::~RocksDBHeadVersion() {}

void RocksDBHeadVersion::CloseDB() {
  if (db_ != nullptr) delete db_;
  db_ = nullptr;
}

bool RocksDBHeadVersion::DeleteDB(const std::string& db_path) {
  auto db_stat = rocksdb::DestroyDB(db_path, rocksdb::Options());
  if (db_stat.ok()) {
    LOG(INFO) << "DB at \"" << db_path << "\" has been deleted";
    return true;
  } else {
    LOG(ERROR) << "Failed to delete DB: " << db_path;
    return false;
  }
}

bool RocksDBHeadVersion::DeleteDB() {
  CloseDB();
  bool success = db_path_.empty() ? false : DeleteDB(db_path_);
  if (success) db_path_.clear();
  return success;
}

bool RocksDBHeadVersion::FlushDB() const {
  auto stat = db_->Flush(db_flush_opts_);
  bool success = stat.ok();
  CHECK(success) << "Failed to flush DB: " << stat.ToString();
  return success;
}

bool RocksDBHeadVersion::LoadBranchVersion(const std::string& db_path) {
  if (db_ != nullptr) {
    LOG(ERROR) << "DB is already opened";
    return false;
  }

  auto db_stat = rocksdb::DB::Open(db_opts_, db_path, &db_);
  if (db_stat.ok()) {
    db_path_ = db_path;
    LOG(INFO) << "DB is successfully opened: " << db_path_;
    return true;
  } else {
    LOG(ERROR) << "Failed to open DB: " << db_stat.ToString();
    return false;
  }
}

bool RocksDBHeadVersion::DumpBranchVersion(const std::string& log_path) {
  return FlushDB();
}

bool RocksDBHeadVersion::GetBranch(const Slice& key, const Slice& branch,
                                   Hash* ver) const {
  std::string db_key(DBKey(key, branch));
  std::string ver_str;
  auto db_stat = db_->Get(db_read_opts_, rocksdb::Slice(db_key), &ver_str);
  if (db_stat.ok()) {
    *ver = Hash(ver_str).Clone();
    return true;
  } else {
    *ver = Hash::kNull;
    return false;
  }
}

std::vector<Hash> RocksDBHeadVersion::GetLatest(const Slice& key) const {
  std::string db_key(DBKey(key));
  std::string vers_str;
  auto db_stat = db_->Get(db_read_opts_, rocksdb::Slice(db_key), &vers_str);
  if (db_stat.ok()) {
    auto vers_bytes = reinterpret_cast<const byte_t*>(vers_str.c_str());
    std::vector<Hash> latest;
    for (size_t i = 0; i < vers_str.size(); i += Hash::kByteLength) {
      latest.emplace_back(Hash(&vers_bytes[i]).Clone());
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
  std::string ver_str = ver.ToString();
  auto db_stat = db_->Put(db_write_opts_, rocksdb::Slice(db_key),
                          rocksdb::Slice(ver_str));
  DCHECK(db_stat.ok()) << "Failed to insert new branch into head table: "
                       << db_stat.ToString();
}

void RocksDBHeadVersion::PutLatest(const Slice& key, const Hash& prev_ver1,
                                   const Hash& prev_ver2, const Hash& ver) {
  std::string db_key(DBKey(key));
  std::string ver_str = ver.ToString();
  // retrieve current versions
  std::string vers_str;
  auto db_stat = db_->Get(db_read_opts_, rocksdb::Slice(db_key), &vers_str);
  // update versions
  if (db_stat.ok()) {
    auto vers_bytes = reinterpret_cast<const byte_t*>(vers_str.c_str());
    std::stringstream ss;
    ss << ver_str;
    DCHECK(!prev_ver1.empty());
    bool found_prev_ver1(false), found_prev_ver2(prev_ver2.empty());
    for (size_t i = 0; i < vers_str.size(); i += Hash::kByteLength) {
      if (found_prev_ver1 && found_prev_ver2) {
        ss << vers_str.substr(i);
        break;
      }
      auto v = Hash(&vers_bytes[i]);
      if (!found_prev_ver1 && v == prev_ver1) {
        found_prev_ver1 = true;
        continue;
      }
      if (!found_prev_ver2 && v == prev_ver2) {
        found_prev_ver2 = true;
        continue;
      }
      ss << vers_str.substr(i, Hash::kByteLength);
    }
    db_stat = db_->Put(db_write_opts_, rocksdb::Slice(db_key),
                       rocksdb::Slice(ss.str()));
  } else { // this is an initial put
    db_stat = db_->Put(db_write_opts_, rocksdb::Slice(db_key),
                       rocksdb::Slice(ver_str));
  }
  DCHECK(db_stat.ok()) << db_stat.ToString();
}

void RocksDBHeadVersion::DeleteBranch(const Slice& key, const Slice& branch) {
  std::string db_key(DBKey(key, branch));
  auto db_stat = db_->Delete(db_write_opts_, rocksdb::Slice(db_key));
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
  Hash head;
  bool success = GetBranch(key, old_branch, &head);
  DCHECK(success);
  PutBranch(key, new_branch, head);
  DeleteBranch(key, old_branch);
}

bool RocksDBHeadVersion::Exists(const Slice& key) const {
  std::string db_key(DBKey(key));
  static std::string versions_str;
  return db_->Get(db_read_opts_, rocksdb::Slice(db_key),
                  &versions_str).ok();
}

bool RocksDBHeadVersion::Exists(const Slice& key, const Slice& branch) const {
  std::string db_key(DBKey(key, branch));
  static std::string ver;
  return db_->Get(db_read_opts_, rocksdb::Slice(db_key), &ver).ok();
}

bool RocksDBHeadVersion::IsBranchHead(const Slice& key, const Slice& branch,
                                      const Hash& ver) const {
  Hash head;
  return GetBranch(key, branch, &head) ? head == ver : false;
}

bool RocksDBHeadVersion::IsLatest(const Slice& key, const Hash& ver) const {
  std::string db_key(DBKey(key));
  std::string ver_str = ver.ToString();
  std::string versions_str;
  auto db_stat =
    db_->Get(db_read_opts_, rocksdb::Slice(db_key), &versions_str);
  if (!db_stat.ok()) return false;
  for (size_t i = 0; i < versions_str.size(); i += Hash::kByteLength) {
    if (versions_str.substr(i, Hash::kByteLength) == ver_str) return true;
  }
  return false;
}

std::vector<std::string> RocksDBHeadVersion::ListKey() const {
  std::vector<std::string> keys;
  if (!FlushDB()) return keys; // must flush DB prior to perform prefix scan
  auto db_seek_key = DBKey(Slice("*"));
  auto prefix = rocksdb::Slice(db_seek_key.data(), db_seek_key.size() - 1);
  auto it = db_->NewIterator(db_read_opts_);
  for (it->Seek(rocksdb::Slice(db_seek_key)); it->Valid(); it->Next()) {
    auto db_key = it->key();
    if (db_key.starts_with(prefix)) keys.emplace_back(ExtractKey(db_key));
  }
  delete it;
  return keys;
}

std::vector<std::string> RocksDBHeadVersion::ListBranch(
  const Slice& key) const {
  std::vector<std::string> keys;
  if (!FlushDB()) return keys; // must flush DB prior to perform prefix scan
  auto db_seek_key = DBKey(key, Slice("*"));
  auto prefix = rocksdb::Slice(db_seek_key.data(), db_seek_key.size() - 1);
  auto it = db_->NewIterator(db_read_opts_);
  for (it->Seek(rocksdb::Slice(db_seek_key)); it->Valid(); it->Next()) {
    auto db_key = it->key();
    if (db_key.starts_with(prefix)) keys.emplace_back(ExtractBranch(db_key));
  }
  delete it;
  return keys;
}

std::string RocksDBHeadVersion::DBKey(const Slice& key) const {
  return "$" + key.ToString();
}

std::string RocksDBHeadVersion::ExtractKey(
  const rocksdb::Slice& db_key) const {
  DCHECK(db_key.starts_with(rocksdb::Slice("$")));
  return std::string(&db_key.data()[1], db_key.size() - 1);
}

std::string RocksDBHeadVersion::DBKey(const Slice& key,
                                      const Slice& branch) const {
  std::stringstream ss;
  ss << std::hex << std::setfill('0') << std::setw(8) << key.len() << key
     << branch;
  return ss.str();
}
std::string RocksDBHeadVersion::ExtractBranch(
  const rocksdb::Slice& db_key) const {
  const char* data = db_key.data();
  size_t prefix_len = 8 + std::stoi(std::string(data, 8), nullptr, 16);
  return std::string(&data[prefix_len], db_key.size() - prefix_len);
}

class MetaPrefixTransform : public rocksdb::SliceTransform {
 public:
  MetaPrefixTransform() = default;
  ~MetaPrefixTransform() = default;

  const char* Name() const { return "ustore.rocksdb.MetaPrefix"; }

  rocksdb::Slice Transform(const rocksdb::Slice& src) const {
    const char* data = src.data();
    if (data[0] == '$') return rocksdb::Slice(data, 1);
    size_t key_len = std::stoi(std::string(data, 8), nullptr, 16);
    return rocksdb::Slice(data, 8 + key_len);
  }

  bool InDomain(const rocksdb::Slice& src) const { return true; }

  bool InRange(const rocksdb::Slice& dst) const { return true; }

  bool SameResultWhenAppended(const rocksdb::Slice& prefix) const {
    return false;
  }
};

const rocksdb::SliceTransform* RocksDBHeadVersion::NewMetaPrefixTransform() {
  return new MetaPrefixTransform;
}

}  // namespace ustore
