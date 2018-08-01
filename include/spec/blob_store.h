// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_SPEC_BLOB_STORE_H_
#define USTORE_SPEC_BLOB_STORE_H_

#define __BLOB_STORE_USE_MAP_MULTI_SET_OP__
#define __BLOB_STORE_USE_SET_FOR_DS_LIST__

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <string>
#include <vector>
#include "spec/object_db.h"
#include "spec/object_meta.h"
#include "utils/utils.h"

namespace ustore {

using Dataset = VMap;
using DataEntry = VBlob;

class BlobStore : protected ObjectMeta {
 public:
  explicit BlobStore(DB* db) noexcept : ObjectMeta(db), odb_(db) {}
  virtual ~BlobStore() = default;

  ErrorCode ListDataset(std::vector<std::string>* datasets);

  ErrorCode ExistsDataset(const std::string& ds_name,
                          bool* exists) const;

  ErrorCode ExistsDataset(const std::string& ds_name,
                          const std::string& branch,
                          bool* exists) const;

  ErrorCode CreateDataset(const std::string& ds_name,
                          const std::string& branch);

  ErrorCode GetDataset(const std::string& ds_name,
                       const std::string& branch, Dataset* ds) const;

  ErrorCode ExportDatasetBinary(
    const std::string& ds_name, const std::string& branch,
    const boost::filesystem::path& file_path,
    size_t* n_entries, size_t* n_bytes) const;

  inline ErrorCode ExportDatasetBinary(
    const std::string& ds_name, const std::string& branch,
    const boost::filesystem::path& file_path) const {
    size_t n_entries, n_bytes;
    return ExportDatasetBinary(
             ds_name, branch, file_path, &n_entries, &n_bytes);
  }

  ErrorCode BranchDataset(const std::string& ds_name,
                          const std::string& old_branch,
                          const std::string& new_branch);

  ErrorCode ListDatasetBranch(const std::string& ds_name,
                              std::vector<std::string>* branches) const;

  ErrorCode DiffDataset(const std::string& lhs_ds_name,
                        const std::string& lhs_branch,
                        const std::string& rhs_ds_name,
                        const std::string& rhs_branch,
                        std::vector<std::string>* diff_keys) const;

  ErrorCode DeleteDataset(const std::string& ds_name,
                          const std::string& branch);

  inline ErrorCode ExistsDataEntry(const std::string& ds_name,
                                   const std::string& branch,
                                   const std::string& entry_name,
                                   bool* exists) const {
    return CallExistsDataEntry(ds_name, branch, entry_name, exists);
  }

  inline ErrorCode ExistsDataEntry(const std::string& ds_name,
                                   const std::string& branch,
                                   const std::vector<std::string>& entry_name,
                                   bool* exists) const {
    return CallExistsDataEntry(ds_name, branch, entry_name, exists);
  }

  inline ErrorCode ExistsDataEntry(const std::string& ds_name,
                                   const std::string& entry_name,
                                   bool* exists) const {
    return CallExistsDataEntry(ds_name, entry_name, exists);
  }

  inline ErrorCode ExistsDataEntry(const std::string& ds_name,
                                   const std::vector<std::string>& entry_name,
                                   bool* exists) const {
    return CallExistsDataEntry(ds_name, entry_name, exists);
  }

  inline ErrorCode GetDataEntry(const std::string& ds_name,
                                const std::string& branch,
                                const std::string& entry_name,
                                DataEntry* entry) const {
    return CallGetDataEntry(ds_name, branch, entry_name, entry);
  }

  inline ErrorCode GetDataEntry(const std::string& ds_name,
                                const std::string& branch,
                                const std::vector<std::string>& entry_name,
                                DataEntry* entry) const {
    return CallGetDataEntry(ds_name, branch, entry_name, entry);
  }

  ErrorCode GetDataEntryBatch(const std::string& ds_name,
                              const std::string& branch,
                              const boost::filesystem::path& dir_path,
                              size_t* n_entries,
                              size_t* n_bytes) const;

  inline ErrorCode GetDataEntryBatch(
    const std::string& ds_name,
    const std::string& branch,
    const boost::filesystem::path& dir_path) const {
    size_t n_entries, n_bytes;
    return GetDataEntryBatch(ds_name, branch, dir_path, &n_entries, &n_bytes);
  }

  inline ErrorCode PutDataEntry(const std::string& ds_name,
                                const std::string& branch,
                                const std::string& entry_name,
                                const std::string& entry_val,
                                Hash* entry_ver) {
    return CallPutDataEntry(ds_name, branch, entry_name, entry_val, entry_ver);
  }

  inline ErrorCode PutDataEntry(const std::string& ds_name,
                                const std::string& branch,
                                const std::string& entry_name,
                                const std::string& entry_val) {
    Hash entry_ver;
    return PutDataEntry(ds_name, branch, entry_name, entry_val, &entry_ver);
  }

  inline ErrorCode PutDataEntry(const std::string& ds_name,
                                const std::string& branch,
                                const std::vector<std::string>& entry_name,
                                const std::string& entry_val,
                                Hash* entry_ver) {
    return CallPutDataEntry(ds_name, branch, entry_name, entry_val, entry_ver);
  }

  inline ErrorCode PutDataEntry(const std::string& ds_name,
                                const std::string& branch,
                                const std::vector<std::string>& entry_name,
                                const std::string& entry_val) {
    Hash entry_ver;
    return PutDataEntry(ds_name, branch, entry_name, entry_val, &entry_ver);
  }

  ErrorCode PutDataEntryBatch(const std::string& ds_name,
                              const std::string& branch,
                              const boost::filesystem::path& dir_path,
                              size_t* n_entries, size_t* n_bytes);

  inline ErrorCode PutDataEntryBatch(const std::string& ds_name,
                                     const std::string& branch,
                                     const boost::filesystem::path& dir_path) {
    size_t n_entries, n_bytes;
    return PutDataEntryBatch(ds_name, branch, dir_path, &n_entries, &n_bytes);
  }

  ErrorCode PutDataEntryByCSV(const std::string& ds_name,
                              const std::string& branch,
                              const boost::filesystem::path& file_path,
                              const std::vector<size_t>& idxs_entry_name,
                              size_t* n_entries, size_t* n_bytes,
                              bool with_schema = false);

  inline ErrorCode PutDataEntryByCSV(const std::string& ds_name,
                                     const std::string& branch,
                                     const boost::filesystem::path& file_path,
                                     const std::vector<size_t> idxs_entry_name,
                                     bool with_schema = false) {
    size_t n_entries, n_bytes;
    return PutDataEntryByCSV(ds_name, branch, file_path, idxs_entry_name,
                             &n_entries, &n_bytes, with_schema);
  }

  inline ErrorCode PutDataEntryByCSV(const std::string& ds_name,
                                     const std::string& branch,
                                     const boost::filesystem::path& file_path,
                                     const size_t idx_entry_name,
                                     size_t* n_entries, size_t* n_bytes,
                                     bool with_schema = false) {
    return PutDataEntryByCSV(ds_name, branch, file_path, {idx_entry_name},
                             n_entries, n_bytes, with_schema);
  }

  inline ErrorCode PutDataEntryByCSV(const std::string& ds_name,
                                     const std::string& branch,
                                     const boost::filesystem::path& file_path,
                                     const size_t idx_entry_name,
                                     bool with_schema = false) {
    size_t n_entries, n_bytes;
    return PutDataEntryByCSV(ds_name, branch, file_path, idx_entry_name,
                             &n_entries, &n_bytes, with_schema);
  }

  inline ErrorCode DeleteDataEntry(const std::string& ds_name,
                                   const std::string& branch,
                                   const std::string& entry_name) {
    return CallDeleteDataEntry(ds_name, branch, entry_name);
  }

  inline ErrorCode DeleteDataEntry(const std::string& ds_name,
                                   const std::string& branch,
                                   const std::vector<std::string>& entry_name) {
    return CallDeleteDataEntry(ds_name, branch, entry_name);
  }

  inline ErrorCode ListDataEntryBranch(
    const std::string& ds_name,
    const std::string& entry_name,
    std::vector<std::string>* branches) const {
    return CallListDataEntryBranch(ds_name, entry_name, branches);
  }

  inline ErrorCode ListDataEntryBranch(
    const std::string& ds_name,
    const std::vector<std::string>& entry_name,
    std::vector<std::string>* branches) const {
    return CallListDataEntryBranch(ds_name, entry_name, branches);
  }

  inline ErrorCode GetDatasetSchema(const std::string& ds_name,
                                    const std::string& branch,
                                    std::string* schema) const {
    return GetMeta(ds_name, branch, "SCHEMA", schema);
  }

  template<class T1, class T2>
  static inline std::string GlobalKey(const T1& ds_name,
                                      const T2& entry_name_store) {
    return Utils::ToString(ds_name) + "::" + Utils::ToString(entry_name_store);
  }

  virtual const std::string EntryNameForDisplay(
    const std::string& entry_name_store) const;

  virtual const std::string EntryNameForDisplay(
    const Slice& entry_name_store) const;

 protected:
  virtual const std::string EntryNameForStore(
    const std::vector<std::string>& attrs) const;

  virtual const std::string EntryNameForStore(
    const std::string& entry_name) const;

  virtual ErrorCode ValidateEntryName(const std::string& entry_name) const;

  ErrorCode ValidateEntryName(
    const std::vector<std::string>& entry_name) const;

  virtual ErrorCode ValidateEntryNameAttr(const std::string& attr) const;

  virtual std::string RegularizeSchema(const std::string& origin) const;

#if defined(__BLOB_STORE_USE_SET_FOR_DS_LIST__)
  ErrorCode GetDatasetList(VSet* ds_list);
#else
  ErrorCode GetDatasetList(VMap* ds_list);
#endif

  ErrorCode UpdateDatasetList(const Slice& ds_name,
                              bool to_delete = false);

  ErrorCode ReadDataset(const Slice& ds_name, const Slice& branch,
                        Dataset* ds) const;

  ErrorCode ReadDataEntryHash(const std::string& ds_name,
                              const std::string& entry_name,
                              const Hash& entry_ver,
                              Hash* entry_hash) const;

  ErrorCode ReadDataEntry(const std::string& ds_name,
                          const std::string& entry_name,
                          const Hash& entry_ver,
                          DataEntry* entry_val) const;

  ErrorCode WriteDataEntry(const std::string& ds_name,
                           const std::string& entry_name,
                           const std::string& entry_val,
                           const Hash& prev_entry_ver,
                           Hash* entry_ver);

  ErrorCode ImplExistsDataEntry(const std::string& ds_name,
                                const std::string& branch,
                                const std::string& entry_name,
                                bool* exists) const;

  template<typename T>
  inline ErrorCode CallExistsDataEntry(const std::string& ds_name,
                                       const std::string& branch,
                                       const T& entry_name,
                                       bool* exists) const {
    USTORE_GUARD(ValidateEntryName(entry_name));
    return ImplExistsDataEntry(
             ds_name, branch, EntryNameForStore(entry_name), exists);
  }

  ErrorCode ImplExistsDataEntry(const std::string& ds_name,
                                const std::string& entry_name,
                                bool* exists) const;

  template<typename T>
  inline ErrorCode CallExistsDataEntry(const std::string& ds_name,
                                       const T& entry_name,
                                       bool* exists) const {
    USTORE_GUARD(ValidateEntryName(entry_name));
    return ImplExistsDataEntry(ds_name, EntryNameForStore(entry_name), exists);
  }

  ErrorCode ImplGetDataEntry(const std::string& ds_name,
                             const std::string& branch,
                             const std::string& entry_name,
                             DataEntry* entry) const;

  template<typename T>
  inline ErrorCode CallGetDataEntry(const std::string& ds_name,
                                    const std::string& branch,
                                    const T& entry_name,
                                    DataEntry* entry) const {
    USTORE_GUARD(ValidateEntryName(entry_name));
    return ImplGetDataEntry(
             ds_name, branch, EntryNameForStore(entry_name), entry);
  }

  ErrorCode ImplPutDataEntry(const std::string& ds_name,
                             const std::string& branch,
                             const std::string& entry_name,
                             const std::string& entry_val,
                             Hash* entry_ver);

  template<typename T>
  inline ErrorCode CallPutDataEntry(const std::string& ds_name,
                                    const std::string& branch,
                                    const T& entry_name,
                                    const std::string& entry_val,
                                    Hash* entry_ver) {
    USTORE_GUARD(ValidateEntryName(entry_name));
    return ImplPutDataEntry(ds_name, branch, EntryNameForStore(entry_name),
                            entry_val, entry_ver);
  }

  ErrorCode ImplDeleteDataEntry(const std::string& ds_name,
                                const std::string& branch,
                                const std::string& entry_name);

  template<typename T>
  inline ErrorCode CallDeleteDataEntry(const std::string& ds_name,
                                       const std::string& branch,
                                       const T& entry_name) {
    USTORE_GUARD(ValidateEntryName(entry_name));
    return ImplDeleteDataEntry(ds_name, branch, EntryNameForStore(entry_name));
  }

  ErrorCode ImplListDataEntryBranch(const std::string& ds_name,
                                    const std::string& entry_name,
                                    std::vector<std::string>* branches) const;

  template<typename T>
  inline ErrorCode CallListDataEntryBranch(
    const std::string& ds_name,
    const T& entry_name,
    std::vector<std::string>* branches) const {
    USTORE_GUARD(ValidateEntryName(entry_name));
    return ImplListDataEntryBranch(
             ds_name, EntryNameForStore(entry_name), branches);
  }

  ObjectDB odb_;
};

}  // namespace ustore

#endif  // USTORE_SPEC_BLOB_STORE_H_
