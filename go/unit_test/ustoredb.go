// DB wrapper for USTORE
package ustoredb

import (
	"fmt"
	"ustore"
  "sync"
)

// column family namespace
type ColumnFamilyHandle struct {
  db ustore.KVDB // pointer to DB partition
  name  string
}

// wrap write batch, indexed by ColumnFamily name
type WriteBatch struct {
  updates map[string]ustore.WriteBatch
}

type UStoreDB struct {
  db ustore.KVDB  // default DB partition
	cFamilies map[string]*ColumnFamilyHandle
  ncfs      uint32 // number of column families
}


var once sync.Once

func OpenDB() (*UStoreDB, error) {
    db := ustore.NewKVDB(uint(0))
    return  &UStoreDB{db, make(map[string]*ColumnFamilyHandle), 1}, nil
}

func Close(db *UStoreDB) {
  ustore.DeleteKVDB(db.db)

  /*
  for cf := range db.cFamilies {
    delete(db.cFamilies, cf)
  }*/
}

func NewWriteBatch() (*WriteBatch, error) {
  return &WriteBatch{make(map[string]ustore.WriteBatch)}, nil
}

func DeleteWriteBatch(batch *WriteBatch) {
  for _, b := range(batch.updates) {
    ustore.DeleteWriteBatch(b)
  }
}

func GetIterator(cfh *ColumnFamilyHandle) (ustore.Iterator, error) {
  return cfh.db.NewIterator(), nil
}

func (cfh *ColumnFamilyHandle) GetCFName() string {
  return cfh.name
}

func DeleteIterator(it ustore.Iterator) {
  ustore.DeleteIterator(it)
}

func (writebatch *WriteBatch) DeleteCF(cfh *ColumnFamilyHandle, key string) {
  if wb, ok := writebatch.updates[cfh.name]; ok {
    wb.Delete(key)
  }
}

func (writebatch *WriteBatch) Clear() {
  for _,wb := range writebatch.updates {
    wb.Clear()
  }
}

func (writebatch *WriteBatch) PutCF(cfh *ColumnFamilyHandle, key string, value string) error {
  // gathering updates
  if wb, ok := writebatch.updates[cfh.name]; ok {
    // CF existed
    wb.Put(key, value)
  } else {
    tmp := ustore.NewWriteBatch()
    tmp.Put(key, value)
    writebatch.updates[cfh.name] =  tmp
  }
  return nil
}

func (db *UStoreDB) GetDB() ustore.KVDB {
  return db.db
}

func (db *UStoreDB) GetSize() uint64 {
  return uint64(db.db.GetSize())
}
func (db *UStoreDB) InitMap(mapkey string) error {
  if !db.db.InitMap(mapkey).Ok() {
    panic("Failed to init Map")
  }
  return nil
}

func (db *UStoreDB) StartMapBatch(mapkey string) error {
  if !db.db.StartMapBatch(mapkey).Ok() {
    panic("Failed to start Map batch")
  }
  return nil
}

func (db *UStoreDB) GetBlob(ss ...[]byte) (string, error) {
  var res ustore.PairStatusString
  if len(ss) == 1 {
    res = db.db.GetBlob(string(ss[0][:]))
  } else {
    res = db.db.GetBlob(string(ss[0][:]), string(ss[1][:]))
  }
  if !res.GetFirst().Ok() {
    return "", fmt.Errorf("Failed to get map")
  } else {
    return res.GetSecond(), nil
  }
}

func (db *UStoreDB) PutBlob(key, value []byte) (string, error) {
  if res := db.db.PutBlob(string(key[:]), string(value[:])); res.GetFirst().Ok() {
    return res.GetSecond(), nil
  } else {
    panic("Failed to Put Blob")
  }
  return "", nil
}

func (db *UStoreDB) GetMap(ss ...[]byte) (string, error) {
  var res ustore.PairStatusString
  if len(ss) == 2 {
    res = db.db.GetMap(string(ss[0][:]), string(ss[1][:]))
  } else {
    res = db.db.GetMap(string(ss[0][:]), string(ss[1][:]), string(ss[2][:]))
  }
  if !res.GetFirst().Ok() {
    return "", fmt.Errorf("Failed to get map")
  } else {
    return res.GetSecond(), nil
  }
}

func (db *UStoreDB) PutMap(key, value []byte) (string, error) {
  if res := db.db.PutMap(string(key[:]), string(value[:])); res.GetFirst().Ok() {
    return res.GetSecond(), nil
  } else {
    panic("Failed to Put Map")
  }
  return "", nil
}

func (db *UStoreDB) SyncMap() (string, error) {
  if res := db.db.SyncMap(); res.GetFirst().Ok() {
    return res.GetSecond(), nil
  } else {
    panic("Failed to Sync Map")
  }
  return "", nil
}

func (db *UStoreDB) WriteMap() (string, error) {
  if res := db.db.WriteMap(); res.GetFirst().Ok() {
    return res.GetSecond(), nil
  } else {
    panic("Failed to Sync Map")
  }
  return "", nil
}

func (db *UStoreDB) CreateColumnFamily(cfname string) (*ColumnFamilyHandle, error) {
  if _, ok := db.cFamilies[cfname]; ok {
    return nil, fmt.Errorf("Column family %v already existed", cfname)
  } else {
    cfh := &ColumnFamilyHandle{ustore.NewKVDB(uint(db.ncfs), cfname), cfname}
    db.ncfs++
    db.cFamilies[cfname] = cfh
    return cfh, nil
  }
}

func (db *UStoreDB) DropColumnFamily(cfh *ColumnFamilyHandle) error {
  delete(db.cFamilies, cfh.name)
  return nil
}

func DeleteColumnFamilyHandle(cfh *ColumnFamilyHandle) {
  ustore.DeleteKVDB(cfh.db)
}

func (db *UStoreDB) PutCF(cfh *ColumnFamilyHandle, key string, value string) error {
  if err := cfh.db.Put(key, value); err.Ok() {
    return nil
  } else {
    return fmt.Errorf("Error during Put")
  }
}

func (db *UStoreDB) Write(writebatch *WriteBatch) error {
  for k, v := range(writebatch.updates) {
    db.cFamilies[k].db.Write(v)
  }
  return nil
}

func (db *UStoreDB) DeleteCF(cfh *ColumnFamilyHandle, key string) error {
  if err := cfh.db.Delete(key); err.Ok() {
    return nil
  } else {
    return fmt.Errorf("Error during Delete")
  }
}

func (db *UStoreDB) ExistCF(cfh *ColumnFamilyHandle, key string) error {
  if err := cfh.db.Exist(key); err {
    return nil
  } else {
    return fmt.Errorf("Error during Exist")
  }
}

func (db *UStoreDB) GetCF(cfh *ColumnFamilyHandle, key string) (string, error) {
  if err := cfh.db.Get(key); err.GetFirst().Ok() {
    return err.GetSecond(), nil
  } else {
    return "", fmt.Errorf("Error during Get")
  }
}
