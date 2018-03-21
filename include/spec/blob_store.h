// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_SPEC_BLOB_STORE_H_
#define USTORE_SPEC_BLOB_STORE_H_

#include <string>
#include <vector>
#include "spec/object_db.h"
#include "utils/utils.h"

namespace ustore {

using Dataset = VMap;
using DataEntry = VBlob;
using DatasetDiffIterator = DuallyDiffKeyIterator;

class BlobStore {
 public:
  explicit BlobStore(DB* db) noexcept : odb_(db) {}
  ~BlobStore() = default;

  ErrorCode ExistsDataset(const std::string& ds_name, bool* exists);

  ErrorCode ExistsDataset(const std::string& ds_name,
                          const std::string& branch, bool* exists);

  ErrorCode CreateDataset(const std::string& ds_name,
                          const std::string& branch);

  ErrorCode GetDataset(const std::string& ds_name, const std::string& branch,
                       Dataset* ds);

  ErrorCode BranchDataset(const std::string& ds_name,
                          const std::string& old_branch,
                          const std::string& new_branch);

  ErrorCode ListDatasetBranch(const std::string& ds_name,
                              std::vector<std::string>* branches);

  inline DatasetDiffIterator DiffDataset(const Dataset& lhs,
                                         const Dataset& rhs) {
    return UMap::DuallyDiff(lhs, rhs);
  }

  ErrorCode DeleteDataset(const std::string& ds_name,
                          const std::string& branch);

  ErrorCode ExistsDataEntry(const std::string& ds_name,
                            const std::string& branch,
                            const std::string& entry_name, bool* exists);

  ErrorCode GetDataEntry(const std::string& ds_name,
                         const std::string& branch,
                         const std::string& entry_name, DataEntry* entry);

  ErrorCode PutDataEntry(const std::string& ds_name,
                         const std::string& branch,
                         const std::string& entry_name,
                         const std::string& entry_val,
                         Hash* entry_ver);

  inline ErrorCode PutDataEntry(const std::string& ds_name,
                                const std::string& branch,
                                const std::string& entry_name,
                                const std::string& entry_val) {
    Hash entry_ver;
    return PutDataEntry(ds_name, branch, entry_name, entry_val, &entry_ver);
  }

  ErrorCode DeleteDataEntry(const std::string& ds_name,
                            const std::string& branch,
                            const std::string& entry_name);

  template<class T1, class T2>
  static inline std::string GlobalKey(const T1& ds_name,
                                      const T2& entry_name) {
    return Utils::ToString(ds_name) + "::" + Utils::ToString(entry_name);
  }

 private:
  ErrorCode ReadDataset(const Slice& ds_name, const Slice& branch, Dataset* ds);

  ErrorCode ReadDataEntry(const std::string& ds_name,
                          const std::string& entry_name, const Hash& entry_ver,
                          DataEntry* entry_val);

  ErrorCode WriteDataEntry(const std::string& ds_name,
                           const std::string& entry_name,
                           const std::string& entry_val,
                           const Hash& prev_entry_ver,
                           Hash* entry_ver);

  ObjectDB odb_;
};

}  // namespace ustore

#endif  // USTORE_SPEC_BLOB_STORE_H_
