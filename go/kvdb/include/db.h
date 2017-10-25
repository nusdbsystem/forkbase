// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_KVDB_DB_H_
#define USTORE_KVDB_DB_H_

#include <string>
#include <utility>
#include <vector>
#include <map>
#include "status.h"
#include "write_batch.h"
#include "spec/object_db.h"
#include "spec/slice.h"
#include "worker/worker.h"
#include "store/chunk_store.h"
#include "utils/timer.h"

namespace ustore_kvdb {

class Iterator;
class MapIterator;

// a key-value store wrapper utilizing Blob data type.
class KVDB {
 public:
  explicit KVDB(ustore::WorkerID id = 42, const std::string& cfname="default") 
          noexcept : wk_(id, nullptr, true), odb_(&wk_), cfname_(cfname) {
        ustore::SetStderrLogging(ustore::ERROR);
  }

  KVDB(const KVDB&) = delete;
  void operator=(const KVDB&) = delete;

  Status Get(const std::string& key, std::string* value);
  std::pair<Status, std::string> Get(const std::string& key);
  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Write(WriteBatch* updates);
  bool Exist(const std::string& key);
  uint64_t GetSize(); 
  Iterator* NewIterator();
  MapIterator* NewMapIterator(const std::string& key, const std::string& version);

  std::string GetCFName() { return cfname_; }

  // top-level map
  Status InitMap(const std::string& mapkey);
  Status StartMapBatch(const std::string& mapkey);
  // put to the current batch
  std::pair<Status, std::string> PutMap(const std::string& key, const std::string& value);
  // write the current batch
  std::pair<Status, std::string> SyncMap();
  // write to the top-level Map
  std::pair<Status, std::string> WriteMap();
  std::pair<Status, std::string> PutBlob(const std::string& key, const std::string& value);


  // get the latest value of UMap[key] where the UMap is identified by (mapkey)
  std::pair<Status, std::string> GetMap(const std::string& mapkey, const std::string& key);
  // return the value of UMap[key] where u is identified by (mapkey, version)
  std::pair<Status, std::string> GetMap(const std::string& mapkey, const std::string& key, const std::string& version);

  // get the iterator of UMap object identified by mapkey 
  std::pair<Status, MapIterator*> GetMapIterator(const std::string& mapkey, const std::string&version);

  // return the previous version of ANY object identified by (key, version)
  std::pair<Status, std::string> GetPreviousVersion(const std::string& key, const std::string& version);

  std::pair<Status, std::string> GetBlob(const std::string& key);
  std::pair<Status, std::string> GetBlob(const std::string& key, const std::string& version);

 private:
  ustore::Worker wk_;
  ustore::ObjectDB odb_;

  std::string cfname_, mapkey_, current_key_;
  static const ustore::Slice DEFAULT_BRANCH;
  std::vector<std::string> skeys_, svalues_;
  std::vector<std::string> mapKeys_, mapValues_;
  std::map<std::string, bool> allMapKeys_;
};

class MapIterator {
 public:
  MapIterator() {}
  MapIterator(ustore::ObjectDB* odb, const std::string& key, const std::string& version);
  
  void SeekToFirst();
  bool Valid();
  bool Next();
  std::string key() const;
  std::string value() const;
 private:
  ustore::ObjectDB* odb_;
  std::vector<ustore::UMap::Iterator> iterator_;
  ustore::VMap temp_map_;
  std::string key_, version_;
};

class Iterator {
 public:
  virtual ~Iterator();
  // commit suicide
  void Release();

  int GetTime();
  Iterator(const Iterator&) = delete;
  void operator=(const Iterator&) = delete;

  virtual void SetRange(const std::string&, const std::string&);

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool Valid();
  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst();
  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast();
  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const std::string& key);
  //  Moves to the next entry in the source.  After this call,
  //  Valid() is true iff the iterator was not positioned at the last entry in
  //  source.
  //  REQUIRES: Valid()
  virtual bool Next();
  //  Moves to the previous entry in the source.  After this call,
  //  Valid() is true iff the iterator was not positioned at the first entry in
  //  source.
  //  REQUIRES: Valid()
  virtual bool Prev();
  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual std::string key() ;

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual std::string value() ;

 protected:
  friend class KVDB;
  friend Iterator* NewEmptyIterator();

  Iterator();
  Iterator(KVDB* db, ustore::Worker* wk);

  ustore::Timer timer_;
  size_t total_time_;
  std::string r_first_ = "";
  std::string r_last_ = "";
  bool valid_;
  bool first_call_ = true;
  KVDB* db_;
  ustore::Worker* wk_;
  const std::map<ustore::PSlice, ustore::Hash> *keys_;
  std::map<ustore::PSlice, ustore::Hash>::const_iterator iterator_;
  // position pointer of the keys vector.
  size_t pos_, first_valid_pos_;
};

extern Iterator* NewEmptyIterator();

}  // namespace ustore_kvdb

#endif  // USTORE_KVDB_DB_H_
