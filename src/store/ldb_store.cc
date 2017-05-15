// Copyright (c) 2017 The Ustore Authors.

#ifdef USE_LEVELDB

#include "store/ldb_store.h"

#include <algorithm>
#include "leveldb/slice.h"
#include "utils/logging.h"

namespace ustore {

// Change the following path if you do not have permissions
LDBStore::LDBStore() : LDBStore("/tmp/ustore-testdb") {}

LDBStore::LDBStore(const std::string& dbpath) {
  opt_.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(opt_, dbpath, &db_);
  CHECK(status.ok()) << "Unable to open/create test database '" << dbpath
                     << "'";
}

LDBStore::~LDBStore() { delete db_; }

Chunk LDBStore::Get(const Hash& key) {
  std::string val;
  auto s = db_->Get(
      rd_opt_,
      leveldb::Slice(reinterpret_cast<char*>(const_cast<byte_t*>(key.value())),
                     Hash::kByteLength),
      &val);
  if (s.ok()) {
    std::unique_ptr<byte_t[]> buf(new byte_t[val.size()]);
    std::copy(val.begin(), val.end(), buf.get());
    Chunk c(std::move(buf));
    CHECK(key == c.hash());
    return c;
  } else {
    LOG(ERROR) << "Leveldb chunck storage internal error: " << s.ToString();
  }
  return Chunk();
}

bool LDBStore::Put(const Hash& key, const Chunk& chunk) {
  CHECK(key == chunk.hash());
  std::string val;
  auto s = db_->Get(
      rd_opt_,
      leveldb::Slice(reinterpret_cast<char*>(const_cast<byte_t*>(key.value())),
                     Hash::kByteLength),
      &val);
  if (s.ok()) {
    // TODO(wangji): Add a compile option later to check those
    // byte_t* buf = new byte_t[val.size()];
    // std::copy(val.begin(), val.end(), buf);
    // Hash h;
    // h.Compute(buf, val.size());
    // CHECK_EQ(h.ToString(), key.ToString());
    return true;
  } else if (s.IsNotFound()) {
    auto ws = db_->Put(
        wr_opt_, leveldb::Slice(
                     reinterpret_cast<char*>(const_cast<byte_t*>(key.value())),
                     Hash::kByteLength),
        leveldb::Slice(
            reinterpret_cast<char*>(const_cast<byte_t*>(chunk.head())),
            chunk.numBytes()));
    CHECK(ws.ok()) << "Leveldb chunck storage internal error: "
                   << ws.ToString();
    return true;
  } else {
    LOG(ERROR) << "Leveldb chunck storage internal error: " << s.ToString();
  }
  return false;
}

const StoreInfo& LDBStore::GetInfo() const {
  static StoreInfo info;
  return info;
}

}  // namespace ustore

#endif  // USE_LEVELDB
