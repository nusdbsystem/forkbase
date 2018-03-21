// Copyright (c) 2017 The UStore Authors.

#include "spec/blob_store.h"

namespace ustore {

ErrorCode BlobStore::ExistsDataset(const std::string& ds_name,
                                   bool* exists) {
  auto rst = odb_.ListBranches(Slice(ds_name));
  *exists = !rst.value.empty();
  return rst.stat;
}

ErrorCode BlobStore::ExistsDataset(const std::string& ds_name,
                                   const std::string& branch,
                                   bool* exists) {
  auto rst = odb_.Exists(Slice(ds_name), Slice(branch));
  *exists = rst.value;
  return rst.stat;
}

ErrorCode BlobStore::CreateDataset(const std::string& ds_name,
                                   const std::string& branch) {
  auto rst = odb_.Exists(Slice(ds_name), Slice(branch));
  auto& ds_exist = rst.value;
  return ds_exist
         ? ErrorCode::kBranchExists
         : odb_.Put(Slice(ds_name), Dataset(), Slice(branch)).stat;
}

ErrorCode BlobStore::GetDataset(const std::string& ds_name,
                                const std::string& branch, Dataset* ds) {
  return  ReadDataset(Slice(ds_name), Slice(branch), ds);
}

ErrorCode BlobStore::ReadDataset(const Slice& ds_name, const Slice& branch,
                                 Dataset* ds) {
  auto rst = odb_.Get(ds_name, branch);
  auto& ec = rst.stat;
  if (ec == ErrorCode::kOK) {
    *ds = rst.value.Map();
  } else {
    ERROR_CODE_FWD(ec, kKeyNotExists, kDatasetNotExists);
  }
  return ec;
}

ErrorCode BlobStore::BranchDataset(const std::string& ds_name,
                                   const std::string& old_branch,
                                   const std::string& new_branch) {
  return odb_.Branch(Slice(ds_name), Slice(old_branch), Slice(new_branch));
}

ErrorCode BlobStore::ListDatasetBranch(
  const std::string& ds_name, std::vector<std::string>* branches) {
  auto rst = odb_.ListBranches(Slice(ds_name));
  USTORE_GUARD(rst.stat);
  *branches = std::move(rst.value);
  return ErrorCode::kOK;
}

ErrorCode BlobStore::DeleteDataset(const std::string& ds_name,
                                   const std::string& branch) {
  return odb_.Delete(Slice(ds_name), Slice(branch));
}

ErrorCode BlobStore::ExistsDataEntry(const std::string& ds_name,
                                     const std::string& branch,
                                     const std::string& entry_name,
                                     bool* exists) {
  Dataset ds;
  USTORE_GUARD(
    ReadDataset(Slice(ds_name), Slice(branch), &ds));
  *exists = !ds.Get(Slice(entry_name)).empty();
  return ErrorCode::kOK;
}

ErrorCode BlobStore::ReadDataEntry(const std::string& ds_name,
                                   const std::string& entry_name,
                                   const Hash& entry_ver,
                                   DataEntry* entry_val) {
  const auto entry_key = GlobalKey(ds_name, entry_name);
  auto entry_rst = odb_.Get(Slice(entry_key), entry_ver);
  USTORE_GUARD(entry_rst.stat);
  *entry_val = entry_rst.value.Blob();
  return ErrorCode::kOK;
}

ErrorCode BlobStore::GetDataEntry(
  const std::string& ds_name, const std::string& branch,
  const std::string& entry_name, DataEntry* entry) {
  Dataset ds;
  // fetch version of data entry
  USTORE_GUARD(
    ReadDataset(Slice(ds_name), Slice(branch), &ds));
  auto entry_ver = Utils::ToHash(ds.Get(Slice(entry_name)));
  if (entry_ver.empty()) {
    LOG(WARNING) << "Data Entry \"" << entry_name
                 << "\" does not exist in Dataset \""
                 << ds_name << "\" of Branch \"" << branch << "\"";
    return ErrorCode::kDataEntryNotExists;
  }
  // read data entry
  return ReadDataEntry(ds_name, entry_name, entry_ver, entry);
}

ErrorCode BlobStore::WriteDataEntry(const std::string& ds_name,
                                    const std::string& entry_name,
                                    const std::string& entry_val,
                                    const Hash& prev_entry_ver,
                                    Hash* entry_ver) {
  Slice entry_val_slice(entry_val);
  DataEntry entry(entry_val_slice);
  auto entry_key = GlobalKey(ds_name, entry_name);
  auto entry_rst = odb_.Put(Slice(entry_key), entry, prev_entry_ver);
  USTORE_GUARD(entry_rst.stat);
  *entry_ver = std::move(entry_rst.value);
  return ErrorCode::kOK;
}

ErrorCode BlobStore::PutDataEntry(const std::string& ds_name,
                                  const std::string& branch,
                                  const std::string& entry_name,
                                  const std::string& entry_val,
                                  Hash* entry_ver) {
  Slice ds_name_slice(ds_name), entry_name_slice(entry_name),
        branch_slice(branch);
  Dataset ds;
  // fetch existing version of the data entry
  USTORE_GUARD(
    ReadDataset(ds_name_slice, branch_slice, &ds));
  auto prev_entry_ver = Utils::ToHash(ds.Get(entry_name_slice));
  if (prev_entry_ver.empty()) prev_entry_ver = Hash::kNull;
  // write the data entry to storage
  USTORE_GUARD(
    WriteDataEntry(ds_name, entry_name, entry_val, prev_entry_ver, entry_ver));
  // update dataset
  ds.Set(entry_name_slice, Utils::ToSlice(*entry_ver));
  return odb_.Put(ds_name_slice, ds, branch_slice).stat;
}

ErrorCode BlobStore::DeleteDataEntry(const std::string& ds_name,
                                     const std::string& branch,
                                     const std::string& entry_name) {
  // delete the data entry in the dataset
  Slice ds_name_slice(ds_name), branch_slice(branch);
  Dataset ds;
  USTORE_GUARD(
    ReadDataset(ds_name_slice, branch_slice, &ds));
  ds.Remove(Slice(entry_name));
  return odb_.Put(ds_name_slice, ds, branch_slice).stat;
}

}  // namespace ustore
