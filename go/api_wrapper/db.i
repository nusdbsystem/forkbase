%module ustore
%include <std_string.i>
%include <std_pair.i>

%{
#include "db.h"
%}

%template(PairStatusString) std::pair<ustore_kvdb::Status, std::string>;

namespace ustore_kvdb {

%nodefaultctor Iterator;
class Iterator;

class MapIterator;
class KVDB {
 public:
  explicit KVDB(unsigned int id = 42, const std::string& cfname = "default");

  std::pair<Status, std::string> Get(const std::string& key);
  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Write(WriteBatch* updates);
  bool Exist(const std::string& key);
  Iterator* NewIterator();
  MapIterator* NewMapIterator(const std::string& key, const std::string& version);
  size_t GetSize();
  std::string GetCFName();
  Status InitMap(const std::string& mapkey);
  Status StartMapBatch(const std::string& mapkey);
  std::pair<Status, std::string> PutMap(const std::string& key, const std::string& value);
  std::pair<Status, std::string> PutBlob(const std::string& key, const std::string& value);
  std::pair<Status, std::string> GetMap(const std::string& mapkey, const std::string& key);
  std::pair<Status, std::string> GetMap(const std::string& mapkey, const std::string& key, const std::string& version);
  std::pair<Status, MapIterator*> GetMapIterator(const std::string& mapkey, const std::string& version);
  std::pair<Status, std::string> GetPreviousVersion(const std::string& key, const std::string& version);

  std::pair<Status, std::string> SyncMap();
  std::pair<Status, std::string> WriteMap();
  std::pair<Status, std::string> GetBlob(const std::string& key);
  std::pair<Status, std::string> GetBlob(const std::string& key, const std::string& version);
};

class MapIterator {
 public:
  MapIterator();
  MapIterator(ustore::ObjectDB *odb, const std::string& key, const std::string& version);
  
  void SeekToFirst();
  bool Valid();
  bool Next();
  std::string key() const;
  std::string value() const;
};

class Iterator {
 public:
  virtual ~Iterator();
  
  int GetTime();
  void Release();
  virtual void SetRange(const std::string& a, const std::string& b);
  virtual bool Valid();
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Seek(const std::string& key);
  virtual bool Next();
  virtual bool Prev();
  virtual std::string key();
  virtual std::string value();
};

extern Iterator* NewEmptyIterator();

}
