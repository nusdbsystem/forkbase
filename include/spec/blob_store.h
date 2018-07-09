// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_SPEC_BLOB_STORE_H_
#define USTORE_SPEC_BLOB_STORE_H_

// #define __BLOB_STORE_USE_MAP_MULTI_SET_OP__
#define __BLOB_STORE_USE_SET_FOR_DS_LIST__

#include <boost/filesystem.hpp>
#include <string>
#include <vector>

#include "spec/object_db.h"
#include "utils/utils.h"

namespace ustore {

using Dataset = VMap;
using DataEntry = VBlob;

class BlobStore {
 public:
  explicit BlobStore(DB* db) noexcept : odb_(db) {}
  virtual ~BlobStore() = default;

  virtual ErrorCode ListDataset(std::vector<std::string>* datasets);

  virtual ErrorCode ExistsDataset(const std::string& ds_name,
                                  bool* exists) const;

  virtual ErrorCode ExistsDataset(const std::string& ds_name,
                                  const std::string& branch,
                                  bool* exists) const;

  virtual ErrorCode CreateDataset(const std::string& ds_name,
                                  const std::string& branch);

  virtual ErrorCode GetDataset(const std::string& ds_name,
                               const std::string& branch, Dataset* ds) const;

  virtual ErrorCode ExportDatasetBinary(
    const std::string& ds_name, const std::string& branch,
    const boost::filesystem::path& file_path,
    size_t* n_entries, size_t* n_bytes) const;

  inline virtual ErrorCode ExportDatasetBinary(
    const std::string& ds_name, const std::string& branch,
    const boost::filesystem::path& file_path) const {
    size_t n_entries, n_bytes;
    return ExportDatasetBinary(
             ds_name, branch, file_path, &n_entries, &n_bytes);
  }

  virtual ErrorCode BranchDataset(const std::string& ds_name,
                                  const std::string& old_branch,
                                  const std::string& new_branch);

  virtual ErrorCode ListDatasetBranch(const std::string& ds_name,
                                      std::vector<std::string>* branches) const;

  virtual ErrorCode DiffDataset(const std::string& lhs_ds_name,
                                const std::string& lhs_branch,
                                const std::string& rhs_ds_name,
                                const std::string& rhs_branch,
                                std::vector<std::string>* diff_keys) const;

  virtual ErrorCode DeleteDataset(const std::string& ds_name,
                                  const std::string& branch);

  virtual ErrorCode ExistsDataEntry(const std::string& ds_name,
                                    const std::string& entry_name,
                                    bool* exists) const;

  virtual ErrorCode ExistsDataEntry(const std::string& ds_name,
                                    const std::string& branch,
                                    const std::string& entry_name,
                                    bool* exists) const;

  virtual ErrorCode GetDataEntry(const std::string& ds_name,
                                 const std::string& branch,
                                 const std::string& entry_name,
                                 DataEntry* entry) const;

  virtual ErrorCode GetDataEntryBatch(const std::string& ds_name,
                                      const std::string& branch,
                                      const boost::filesystem::path& dir_path,
                                      size_t* n_entries,
                                      size_t* n_bytes) const;

  inline virtual ErrorCode GetDataEntryBatch(const std::string& ds_name,
      const std::string& branch,
      const boost::filesystem::path& dir_path) const {
    size_t n_entries, n_bytes;
    return GetDataEntryBatch(ds_name, branch, dir_path, &n_entries, &n_bytes);
  }

  virtual ErrorCode PutDataEntry(const std::string& ds_name,
                                 const std::string& branch,
                                 const std::string& entry_name,
                                 const std::string& entry_val,
                                 Hash* entry_ver);

  inline virtual ErrorCode PutDataEntry(const std::string& ds_name,
                                        const std::string& branch,
                                        const std::string& entry_name,
                                        const std::string& entry_val) {
    Hash entry_ver;
    return PutDataEntry(ds_name, branch, entry_name, entry_val, &entry_ver);
  }

  virtual ErrorCode PutDataEntryBatch(const std::string& ds_name,
                                      const std::string& branch,
                                      const boost::filesystem::path& dir_path,
                                      size_t* n_entries, size_t* n_bytes);

  inline virtual ErrorCode PutDataEntryBatch(const std::string& ds_name,
      const std::string& branch,
      const boost::filesystem::path& dir_path) {
    size_t n_entries, n_bytes;
    return PutDataEntryBatch(ds_name, branch, dir_path, &n_entries, &n_bytes);
  }

  virtual ErrorCode PutDataEntryByCSV(const std::string& ds_name,
                                      const std::string& branch,
                                      const boost::filesystem::path& file_path,
                                      const int64_t idx_entry_name,
                                      size_t* n_entries, size_t* n_bytes);

  inline virtual ErrorCode PutDataEntryByCSV(const std::string& ds_name,
      const std::string& branch, const boost::filesystem::path& file_path,
      const int64_t idx_entry_name) {
    size_t n_entries, n_bytes;
    return PutDataEntryByCSV(ds_name, branch, file_path, idx_entry_name,
                             &n_entries, &n_bytes);
  }

  virtual ErrorCode DeleteDataEntry(const std::string& ds_name,
                                    const std::string& branch,
                                    const std::string& entry_name);

  virtual ErrorCode ListDataEntryBranch(
    const std::string& ds_name, const std::string& entry_name,
    std::vector<std::string>* branches) const;

  template<class T1, class T2>
  static inline std::string GlobalKey(const T1& ds_name,
                                      const T2& entry_name) {
    return Utils::ToString(ds_name) + "::" + Utils::ToString(entry_name);
  }

 protected:
#if defined(__BLOB_STORE_USE_SET_FOR_DS_LIST__)
  virtual ErrorCode GetDatasetList(VSet* ds_list);
#else
  virtual ErrorCode GetDatasetList(VMap* ds_list);
#endif

  virtual ErrorCode UpdateDatasetList(const Slice& ds_name,
                                      bool to_delete = false);

  virtual ErrorCode ReadDataset(const Slice& ds_name, const Slice& branch,
                                Dataset* ds) const;

  virtual ErrorCode ReadDataEntryHash(const std::string& ds_name,
                                      const std::string& entry_name,
                                      const Hash& entry_ver,
                                      Hash* entry_hash) const;

  virtual ErrorCode ReadDataEntry(const std::string& ds_name,
                                  const std::string& entry_name,
                                  const Hash& entry_ver,
                                  DataEntry* entry_val) const;

  virtual ErrorCode WriteDataEntry(const std::string& ds_name,
                                   const std::string& entry_name,
                                   const std::string& entry_val,
                                   const Hash& prev_entry_ver,
                                   Hash* entry_ver);

  ObjectDB odb_;
};

}  // namespace ustore

#endif  // USTORE_SPEC_BLOB_STORE_H_
