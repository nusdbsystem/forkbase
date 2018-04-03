// Copyright (c) 2017 The Ustore Authors.

#if defined(USE_ROCKSDB)

#include <cstring>
#include <utility>
#include "utils/logging.h"
#include "utils/utils.h"

#include "worker/rocks_head_version.h"

namespace ustore {

static const size_t kSharedCacheSizeBytes(64 << 20);

using key_size_t = uint16_t;
static const size_t kKeySizeBytes(sizeof(key_size_t));

static const size_t kVersionSizeBytes(Hash::kByteLength);
static const size_t kTwoVersionsSizeBytes(kVersionSizeBytes << 1);
static const size_t kThreeVersionsSizeBytes(kVersionSizeBytes * 3);

RocksBranchVersionDB::RocksBranchVersionDB() {
  db_opts_.error_if_exists = false;
  db_write_opts_.disableWAL = true;
}

RocksBranchVersionDB::RocksBranchVersionDB(
  const std::shared_ptr<rocksdb::Cache>& cache) : RocksBranchVersionDB() {
  db_blk_tab_opts_.block_cache = cache;
}

std::string RocksBranchVersionDB::DBKey(const Slice& key,
                                        const Slice& branch) const {
  const size_t key_len = key.len();
  const size_t branch_len = branch.len();
  std::string db_key;
  db_key.resize(kKeySizeBytes + key_len + branch_len);
  char* p = &(db_key.at(0));
  *(reinterpret_cast<key_size_t*>(p)) = static_cast<key_size_t>(key_len);
  p += kKeySizeBytes;
  std::memcpy(p, key.data(), key_len);
  p += key_len;
  std::memcpy(p, branch.data(), branch_len);
  return db_key;
}

std::string RocksBranchVersionDB::ExtractBranch(
  const rocksdb::Slice& db_key) const {
  const char* data = db_key.data();
  const auto& key_len = *(reinterpret_cast<const key_size_t*>(&data[0]));
  const size_t prefix_len = kKeySizeBytes + key_len;
  return std::string(&data[prefix_len], db_key.size() - prefix_len);
}

class BranchPrefixTransform : public rocksdb::SliceTransform {
 public:
  BranchPrefixTransform() = default;
  ~BranchPrefixTransform() = default;

  const char* Name() const override { return "ustore.rocksdb.BranchPrefix"; }

  rocksdb::Slice Transform(const rocksdb::Slice& src) const override {
    const char* data = src.data();
    const auto& key_len = *(reinterpret_cast<const key_size_t*>(&data[0]));
    return rocksdb::Slice(data, kKeySizeBytes + key_len);
  }

  bool InDomain(const rocksdb::Slice& src) const override { return true; }

  bool InRange(const rocksdb::Slice& dst) const override { return true; }

  bool SameResultWhenAppended(const rocksdb::Slice& prefix) const override {
    return false;
  }
};

const rocksdb::SliceTransform*
RocksBranchVersionDB::NewPrefixTransform() const {
  return new BranchPrefixTransform;
}

bool RocksBranchVersionDB::Get(const Slice& key, const Slice& branch,
                               Hash* ver) const {
  const auto db_key = DBKey(key, branch);
  rocksdb::PinnableSlice version;
  auto success = DBGet(rocksdb::Slice(db_key), &version);
  *ver = success
         ? Hash(reinterpret_cast<const byte_t*>(version.data())).Clone()
         : Hash::kNull;
  return success;
}

bool RocksBranchVersionDB::Exists(const Slice& key, const Slice& branch) const {
  const auto db_key = DBKey(key, branch);
  return DBExists(rocksdb::Slice(db_key));
}

bool RocksBranchVersionDB::Put(const Slice& key, const Slice& branch,
                               const rocksdb::Slice& ver_slice) {
  const auto db_key = DBKey(key, branch);
  return DBPut(rocksdb::Slice(db_key), ver_slice);
}

bool RocksBranchVersionDB::Delete(const Slice& key, const Slice& branch) {
  const auto db_key = DBKey(key, branch);
  return DBDelete(rocksdb::Slice(db_key));
}

bool RocksBranchVersionDB::Move(const Slice& key, const Slice& old_branch,
                                const Slice& new_branch) {
  // retrieve head version from the old_branch
  const auto db_key_old = DBKey(key, old_branch);
  const auto db_key_old_slice = rocksdb::Slice(db_key_old);
  rocksdb::PinnableSlice head;
  GUARD(DBGet(db_key_old_slice, &head));
  // perform move in a batch update
  rocksdb::WriteBatch batch;
  const auto db_key_new = DBKey(key, new_branch);
  batch.Put(rocksdb::Slice(db_key_new), head);
  batch.Delete(db_key_old_slice);
  return DBWrite(&batch);
}

std::vector<std::string>
RocksBranchVersionDB::GetBranches(const Slice& key) const {
  const auto db_seek_key = DBKey(key, Slice("*"));
  std::vector<std::string> branches;
  auto f_proc_entry = [this, &branches](const rocksdb::Iterator * it) {
    branches.emplace_back(ExtractBranch(it->key()));
  };
  DBPrefixScan(db_seek_key, f_proc_entry);
  return branches;
}

RocksLatestVersionDB::RocksLatestVersionDB() {
  db_opts_.error_if_exists = false;
  db_write_opts_.disableWAL = true;
}

RocksLatestVersionDB::RocksLatestVersionDB(
  const std::shared_ptr<rocksdb::Cache>& cache) : RocksLatestVersionDB() {
  db_blk_tab_opts_.block_cache = cache;
}

class LatestVersionUpdater : public rocksdb::AssociativeMergeOperator {
 public:
  LatestVersionUpdater() = default;
  ~LatestVersionUpdater() = default;

  const char* Name() const override { return "LatestVersionUpdater"; }

  bool Merge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value,
             const rocksdb::Slice& value, std::string* new_value,
             rocksdb::Logger* logger) const override {
    if (!existing_value) {  // initial put
      *new_value = value.ToString();
      return true;
    }
    // initialize flags for the search of previous versions
    const size_t value_size = value.size();
    DCHECK(value_size >= kVersionSizeBytes);
    bool found_prev_ver1 = (value_size < kTwoVersionsSizeBytes);
    DCHECK(found_prev_ver1 || value_size >= kTwoVersionsSizeBytes);
    bool found_prev_ver2 = (value_size < kThreeVersionsSizeBytes);
    DCHECK(found_prev_ver2 || value_size == kThreeVersionsSizeBytes);
    // initialize value pointers of previous versions
    const auto value_data = value.data();
    const auto prev_ver1_data =
      (found_prev_ver1 ? nullptr : &value_data[kVersionSizeBytes]);
    const auto prev_ver2_data =
      (found_prev_ver2 ? nullptr : &value_data[kTwoVersionsSizeBytes]);
    // allocate buffer for new versions
    const auto old_versions = existing_value->data();
    const size_t old_versions_len = existing_value->size();
    std::string new_versions_str(value_data, kVersionSizeBytes);
    size_t new_versions_len = old_versions_len + Hash::kByteLength;
    new_versions_str.resize(new_versions_len);
    auto new_versions = &new_versions_str.at(0);
    // initialize indices for memory copy
    size_t cpy_from = 0;  // applied to old_versions[]
    size_t cpy_to = kVersionSizeBytes;  // applied to new_versions[]
    // function of memory copy
    auto f_cpy = [&old_versions, &cpy_from, &new_versions, &cpy_to](size_t i) {
      const size_t n_bytes = i - cpy_from;
      if (n_bytes > 0) {
        std::memcpy(&new_versions[cpy_to], &old_versions[cpy_from], n_bytes);
        cpy_to += n_bytes;
      }
      cpy_from = i + kVersionSizeBytes;
    };
    // scan current latest versions and delete the given previous ones
    // that become obsolete along with this Merge operation
    // NOTE: The given latest versions are not necessary to be the
    //       current latest.
    for (size_t i = 0; i < old_versions_len; i += kVersionSizeBytes) {
      const auto lv_data = &old_versions[i];
      if (!found_prev_ver1 &&
          std::memcmp(lv_data, prev_ver1_data, kVersionSizeBytes) == 0) {
        found_prev_ver1 = true;
        f_cpy(i);
        new_versions_len -= Hash::kByteLength;
        continue;
      }
      if (!found_prev_ver2 &&
          std::memcmp(lv_data, prev_ver2_data, kVersionSizeBytes) == 0) {
        found_prev_ver2 = true;
        f_cpy(i);
        new_versions_len -= Hash::kByteLength;
        continue;
      }
      if (std::memcmp(lv_data, value_data, kVersionSizeBytes) == 0) {
        f_cpy(i);
        new_versions_len -= Hash::kByteLength;
      }
    } {  // copy the residual versions
      size_t n_bytes = old_versions_len - cpy_from;
      if (n_bytes > 0)
        std::memcpy(&new_versions[cpy_to], &old_versions[cpy_from], n_bytes);
    }
    new_versions_str.resize(new_versions_len);
    *new_value = std::move(new_versions_str);
    return true;
  }
};

rocksdb::MergeOperator* RocksLatestVersionDB::NewMergeOperator() const {
  return new LatestVersionUpdater;
}

std::vector<Hash> RocksLatestVersionDB::Get(const Slice& key) const {
  rocksdb::PinnableSlice versions;
  if (DBGet(ToRocksSlice(key), &versions)) {
    auto vers_bytes = reinterpret_cast<const byte_t*>(versions.data());
    std::vector<Hash> latest;
    for (size_t i = 0; i < versions.size(); i += kVersionSizeBytes) {
      latest.emplace_back(Hash(&vers_bytes[i]).Clone());
    }
    return latest;
  } else {
    DLOG(INFO) << "No data exists for Key \"" << key << "\"";
    static const std::vector<Hash> empty;
    return empty;
  }
}

bool RocksLatestVersionDB::Exists(const Slice& key) const {
  return DBExists(ToRocksSlice(key));
}

bool RocksLatestVersionDB::Exists(const Slice& key, const Hash& ver) const {
  const auto ver_data = ver.value();
  rocksdb::PinnableSlice versions;
  GUARD(DBGet(ToRocksSlice(key), &versions));
  const auto versions_data = versions.data();
  for (size_t i = 0; i < versions.size(); i += kVersionSizeBytes) {
    if (std::memcmp(&versions_data[i], ver_data, kVersionSizeBytes) == 0) {
      return true;
    }
  }
  return false;
}

bool RocksLatestVersionDB::Merge(const Slice& key, const Hash& prev_ver1,
                                 const Hash& prev_ver2, const Hash& ver) {
  char ver_update[kThreeVersionsSizeBytes];
  auto f_merge = [this, &key, &ver_update](const size_t n_bytes) {
    return DBMerge(ToRocksSlice(key), rocksdb::Slice(ver_update, n_bytes));
  };
  // include the new version
  std::memcpy(ver_update, ver.value(), kVersionSizeBytes);
  DCHECK(!prev_ver1.empty());
  if (prev_ver1 == Hash::kNull) {
    return f_merge(kVersionSizeBytes);
  }
  // include the 1st previous version
  std::memcpy(
    &ver_update[kVersionSizeBytes], prev_ver1.value(), kVersionSizeBytes);
  if (prev_ver2.empty() || prev_ver2 == Hash::kNull) {
    return f_merge(kTwoVersionsSizeBytes);
  }
  // include the 2nd previous version
  std::memcpy(
    &ver_update[kTwoVersionsSizeBytes], prev_ver2.value(), kVersionSizeBytes);
  {
    return f_merge(kThreeVersionsSizeBytes);
  }
}

std::vector<std::string> RocksLatestVersionDB::GetKeys() const {
  std::vector<std::string> keys;
  DBFullScan([this, &keys](const rocksdb::Iterator * it) {
    keys.emplace_back(it->key().ToString());
  });
  return keys;
}

RocksHeadVersion::RocksHeadVersion()
  : db_shared_cache_(rocksdb::NewLRUCache(kSharedCacheSizeBytes)),
    branch_db_(db_shared_cache_),
    latest_db_(db_shared_cache_) {}

void RocksHeadVersion::CloseDB() {
  branch_db_.CloseDB();
  latest_db_.CloseDB();
}

bool RocksHeadVersion::DestroyDB(const std::string& db_path) {
  GUARD(RocksDB::DestroyDB(db_path + ".branch"));
  GUARD(RocksDB::DestroyDB(db_path + ".latest"));
  return true;
}

bool RocksHeadVersion::DestroyDB() {
  GUARD(branch_db_.DestroyDB());
  GUARD(latest_db_.DestroyDB());
  return true;
}

bool RocksHeadVersion::Load(const std::string& db_path) {
  GUARD(branch_db_.OpenDB(db_path + ".branch"));
  GUARD(latest_db_.OpenDB(db_path + ".latest"));
  return true;
}

bool RocksHeadVersion::Dump(const std::string& db_path) {
  GUARD(branch_db_.FlushDB());
  GUARD(latest_db_.FlushDB());
  return true;
}

void RocksHeadVersion::PutBranch(const Slice& key, const Slice& branch,
                                 const Hash& ver) {
  if (!branch_db_.Put(key, branch, RocksDB::ToRocksSlice(ver))) {
    LOG(WARNING) << "Failed to update/insert branch \"" << branch
                 << "\" of key \"" << key << "\" with version \""
                 << ver << "\"";
  }
}

void RocksHeadVersion::PutLatest(const Slice& key, const Hash& prev_ver1,
                                 const Hash& prev_ver2, const Hash& ver) {
  if (!latest_db_.Merge(key, prev_ver1, prev_ver2, ver)) {
    LOG(WARNING) << "Failed to update/insert key \"" << key
                 << "\" with latest version \"" << ver << "\"";
  }
}

void RocksHeadVersion::RemoveBranch(const Slice& key, const Slice& branch) {
  if (Exists(key, branch)) {
    branch_db_.Delete(key, branch);
  } else {
    LOG(WARNING) << "Branch \"" << branch << "for Key \"" << key
                 << "\" does not exist!";
  }
}

void RocksHeadVersion::RenameBranch(const Slice& key, const Slice& old_branch,
                                    const Slice& new_branch) {
  DCHECK(Exists(key, old_branch)) << ": Branch \"" << old_branch
                                  << "\" for key \"" << key
                                  << "\" does not exist!";
  DCHECK(!Exists(key, new_branch)) << ": Branch \"" << new_branch
                                   << "\" for key \"" << key
                                   << "\" already exists!";

  if (!branch_db_.Move(key, old_branch, new_branch)) {
    LOG(WARNING) << "Failed to rename branch \"" << old_branch
                 << "\" of key \"" << key << "\" to \"" << new_branch << "\"";
  }
}

}  // namespace ustore

#endif  // USE_ROCKSDB
