// Copyright (c) 2017 The UStore Authors.

#include <fstream>
#include "spec/blob_store.h"

namespace ustore {

static const std::string DATASET_LIST_NAME = "__DATASET_LIST__";
static const std::string DATASET_LIST_BRANCH = "master";

namespace boost_fs = boost::filesystem;

#if defined(__BLOB_STORE_USE_SET_FOR_DS_LIST__)
ErrorCode BlobStore::GetDatasetList(VSet* ds_list) {
#else
ErrorCode BlobStore::GetDatasetList(VMap* ds_list) {
#endif
  static const Slice ds_list_name(DATASET_LIST_NAME);
  static const Slice ds_list_branch(DATASET_LIST_BRANCH);
  auto rst_ds_list = odb_.Get(ds_list_name, ds_list_branch);
  if (rst_ds_list.stat != ErrorCode::kOK) {
#if defined(__BLOB_STORE_USE_SET_FOR_DS_LIST__)
    USTORE_GUARD(
      odb_.Put(ds_list_name, VSet(), ds_list_branch).stat);
#else
    USTORE_GUARD(
      odb_.Put(ds_list_name, VMap(), ds_list_branch).stat);
#endif
    return GetDatasetList(ds_list);
  }
#if defined(__BLOB_STORE_USE_SET_FOR_DS_LIST__)
  *ds_list = rst_ds_list.value.Set();
#else
  *ds_list = rst_ds_list.value.Map();
#endif
  return ErrorCode::kOK;
}

ErrorCode BlobStore::ListDataset(std::vector<std::string>* datasets) {
#if defined(__BLOB_STORE_USE_SET_FOR_DS_LIST__)
  VSet ds_list;
#else
  VMap ds_list;
#endif
  USTORE_GUARD(
    GetDatasetList(&ds_list));
  std::vector<std::string> dss;
  for (auto it = ds_list.Scan(); !it.end(); it.next()) {
    dss.emplace_back(it.key().ToString());
  }
  *datasets = std::move(dss);
  return ErrorCode::kOK;
}

ErrorCode BlobStore::ExistsDataset(const std::string& ds_name, bool* exists) {
  auto rst = odb_.ListBranches(Slice(ds_name));
  *exists = !rst.value.empty();
  return rst.stat;
}

ErrorCode BlobStore::ExistsDataset(const std::string& ds_name,
                                   const std::string& branch, bool* exists) {
  auto rst = odb_.Exists(Slice(ds_name), Slice(branch));
  *exists = rst.value;
  return rst.stat;
}

ErrorCode BlobStore::UpdateDatasetList(const Slice& ds_name, bool to_delete) {
  static const Slice ds_list_name(DATASET_LIST_NAME);
  static const Slice ds_list_branch(DATASET_LIST_BRANCH);
  // retrieve dataset list
#if defined(__BLOB_STORE_USE_SET_FOR_DS_LIST__)
  VSet ds_list;
#else
  VMap ds_list;
#endif
  USTORE_GUARD(
    GetDatasetList(&ds_list));
  // update dataset list
#if defined(__BLOB_STORE_USE_SET_FOR_DS_LIST__)
  to_delete ? ds_list.Remove(ds_name) : ds_list.Set(ds_name);
#else
  to_delete ? ds_list.Remove(ds_name) : ds_list.Set(ds_name, Slice(""));
#endif
  return odb_.Put(ds_list_name, ds_list, ds_list_branch).stat;
}

ErrorCode BlobStore::CreateDataset(const std::string& ds_name,
                                   const std::string& branch) {
  const Slice ds_name_slice(ds_name), branch_slice(branch);
  // check if dataset branch exists
  auto rst_ds_exists = odb_.Exists(ds_name_slice, branch_slice);
  USTORE_GUARD(rst_ds_exists.stat);
  auto& ds_exist = rst_ds_exists.value;
  if (ds_exist) {
    return ErrorCode::kBranchExists;
  } else {
    // create new dataset
    auto rst_ds_put = odb_.Put(ds_name_slice, Dataset(), branch_slice);
    USTORE_GUARD(rst_ds_put.stat);
    // record the new dataset
    return UpdateDatasetList(ds_name_slice);
  }
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

ErrorCode BlobStore::GetDataset(const std::string& ds_name,
                                const std::string& branch, Dataset* ds) {
  return ReadDataset(Slice(ds_name), Slice(branch), ds);
}

ErrorCode BlobStore::ExportDatasetBinary(const std::string& ds_name,
    const std::string& branch, const boost::filesystem::path& file_path,
    size_t* n_entries, size_t* n_bytes) {
  *n_entries = 0;
  *n_bytes = 0;
  // open the output file
  try {
    boost_fs::create_directories(file_path.parent_path());
  } catch (const boost_fs::filesystem_error& e) {
    LOG(ERROR) << e.what();
    return ErrorCode::kIOFault;
  }
  std::ofstream ofs(file_path.native(), std::ios::out | std::ios::trunc);
  // retrieve the operating dataset
  Dataset ds;
  USTORE_GUARD(
    GetDataset(ds_name, branch, &ds));
  // iterate the dataset
  for (auto it = ds.Scan(); !it.end(); it.next()) {
    // retrieve data entry
    const auto entry_name = it.key().ToString();
    const auto entry_ver = Utils::ToHash(it.value());
    DCHECK(!entry_ver.empty());
    DataEntry entry;
    USTORE_GUARD(
      ReadDataEntry(ds_name, entry_name, entry_ver, &entry));
    // output data entry to file
    ofs << entry << std::endl;
    *n_bytes += entry.size();
  }
  ofs.close();
  *n_entries = ds.numElements();
  *n_bytes += *n_entries;  // count for std::endl
  return ErrorCode::kOK;
}

ErrorCode BlobStore::BranchDataset(const std::string& ds_name,
                                   const std::string& old_branch,
                                   const std::string& new_branch) {
  return odb_.Branch(Slice(ds_name), Slice(old_branch), Slice(new_branch));
}

ErrorCode BlobStore::ListDatasetBranch(const std::string& ds_name,
                                       std::vector<std::string>* branches) {
  auto rst = odb_.ListBranches(Slice(ds_name));
  USTORE_GUARD(rst.stat);
  *branches = std::move(rst.value);
  return ErrorCode::kOK;
}

ErrorCode BlobStore::DiffDataset(const std::string& lhs_ds_name,
                                 const std::string& lhs_branch,
                                 const std::string& rhs_ds_name,
                                 const std::string& rhs_branch,
                                 std::vector<std::string>* diff_keys) {
  // retrieve datasets
  Dataset lhs_ds;
  USTORE_GUARD(
    GetDataset(lhs_ds_name, lhs_branch, &lhs_ds));
  Dataset rhs_ds;
  USTORE_GUARD(
    GetDataset(rhs_ds_name, rhs_branch, &rhs_ds));
  // diff dataset
  diff_keys->clear();
  for (auto it_diff = UMap::DuallyDiff(lhs_ds, rhs_ds);
       !it_diff.end(); it_diff.next()) {
    const auto en_name = it_diff.key().ToString();
    const auto lhs_en_ver_slice = it_diff.lhs_value();
    const auto rhs_en_ver_slice = it_diff.rhs_value();
    // diff at the data version level
    if (lhs_en_ver_slice.empty() || rhs_en_ver_slice.empty()) {
      diff_keys->push_back(std::move(en_name));
      continue;
    }
    // diff at the data content level
    const auto lhs_en_ver = Utils::ToHash(lhs_en_ver_slice);
    Hash lhs_en_hash;
    USTORE_GUARD(
      ReadDataEntryHash(lhs_ds_name, en_name, lhs_en_ver, &lhs_en_hash));
    const auto rhs_en_ver = Utils::ToHash(rhs_en_ver_slice);
    Hash rhs_en_hash;
    USTORE_GUARD(
      ReadDataEntryHash(rhs_ds_name, en_name, rhs_en_ver, &rhs_en_hash));
    DCHECK(lhs_en_ver != rhs_en_ver);
    if (lhs_en_hash != rhs_en_hash) {
      diff_keys->push_back(std::move(en_name));
    }
  }
  return ErrorCode::kOK;
}

ErrorCode BlobStore::DeleteDataset(const std::string& ds_name,
                                   const std::string& branch) {
  const Slice ds_name_slice(ds_name), branch_slice(branch);
  // delete dataset from storage
  USTORE_GUARD(
    odb_.Delete(ds_name_slice, branch_slice));
  // remove the record of the dataset if all its branches have been deleted
  bool exists;
  USTORE_GUARD(
    ExistsDataset(ds_name, &exists));
  return (exists ? ErrorCode::kOK : UpdateDatasetList(ds_name_slice, true));
}

ErrorCode BlobStore::ExistsDataEntry(const std::string& ds_name,
                                     const std::string& entry_name,
                                     bool* exists) {
  // retrieve branch candidates
  std::vector<std::string> ds_branches;
  USTORE_GUARD(
    ListDatasetBranch(ds_name, &ds_branches));
  // check if any branch contains the data entry
  *exists = false;
  for (auto& b : ds_branches) {
    USTORE_GUARD(
      ExistsDataEntry(ds_name, b, entry_name, exists));
    if (*exists) break;
  }
  return ErrorCode::kOK;
}

ErrorCode BlobStore::ExistsDataEntry(const std::string& ds_name,
                                     const std::string& branch,
                                     const std::string& entry_name,
                                     bool* exists) {
  Dataset ds;
  USTORE_GUARD(
    GetDataset(ds_name, branch, &ds));
  *exists = !ds.Get(Slice(entry_name)).empty();
  return ErrorCode::kOK;
}

ErrorCode BlobStore::ReadDataEntryHash(const std::string& ds_name,
                                       const std::string& entry_name,
                                       const Hash& entry_ver,
                                       Hash* entry_hash) {
  const auto entry_key = GlobalKey(ds_name, entry_name);
  auto entry_rst = odb_.Get(Slice(entry_key), entry_ver);
  USTORE_GUARD(entry_rst.stat);
  *entry_hash = entry_rst.value.cell().dataHash().Clone();
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

ErrorCode BlobStore::GetDataEntry(const std::string& ds_name,
                                  const std::string& branch,
                                  const std::string& entry_name,
                                  DataEntry* entry) {
  // fetch version of data entry
  Dataset ds;
  USTORE_GUARD(
    GetDataset(ds_name, branch, &ds));
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

ErrorCode BlobStore::GetDataEntryBatch(const std::string& ds_name,
                                       const std::string& branch,
                                       const boost_fs::path& dir_path,
                                       size_t* n_entries, size_t* n_bytes) {
  *n_entries = 0;
  *n_bytes = 0;
  // retrieve the operating dataset
  Dataset ds;
  USTORE_GUARD(
    GetDataset(ds_name, branch, &ds));
  try {
    boost_fs::create_directories(dir_path);
    // iterate the dataset
    for (auto it = ds.Scan(); !it.end(); it.next()) {
      // retrieve data entry
      const auto entry_name = it.key().ToString();
      const auto entry_ver = Utils::ToHash(it.value());
      DCHECK(!entry_ver.empty());
      DataEntry entry;
      USTORE_GUARD(
        ReadDataEntry(ds_name, entry_name, entry_ver, &entry));
      // output data entry to file
      const boost_fs::path filename(entry_name);
      if (filename.has_parent_path()) {
        boost_fs::create_directories(dir_path / filename.parent_path());
      }
      std::ofstream ofs((dir_path / filename).native(),
                        std::ios::out | std::ios::trunc);
      ofs << entry;
      *n_bytes += ofs.tellp();
      ofs.close();
    }
    *n_entries = ds.numElements();
    return ErrorCode::kOK;
  } catch (const boost_fs::filesystem_error& e) {
    LOG(ERROR) << e.what();
    return ErrorCode::kIOFault;
  }
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

ErrorCode BlobStore::PutDataEntryBatch(const std::string& ds_name,
                                       const std::string& branch,
                                       const boost_fs::path& dir_path,
                                       size_t* n_entries, size_t* n_bytes) {
  *n_entries = 0;
  *n_bytes = 0;
  const Slice ds_name_slice(ds_name), branch_slice(branch);
  // retrieve the operating dataset
  Dataset ds;
  USTORE_GUARD(
    ReadDataset(ds_name_slice, branch_slice, &ds));
  // procedure: put single file (i.e. a data entry) to storage
  std::vector<std::string> ds_entry_names;
  std::vector<Hash> ds_entry_vers;
  const auto f_put_file =
  [&](const boost_fs::path & path, const boost_fs::path & rlt_path) {
    // construct data entry name and value
    const auto entry_name = rlt_path.native();
    std::string entry_val;
    USTORE_GUARD(
      Utils::GetFileContents(path.native(), &entry_val));
    // fetch existing version of the data entry
    auto prev_entry_ver = Utils::ToHash(ds.Get(Slice(entry_name)));
    if (prev_entry_ver.empty()) prev_entry_ver = Hash::kNull;
    // write the data entry to storage
    Hash entry_ver;
    USTORE_GUARD(WriteDataEntry(
                   ds_name, entry_name, entry_val, prev_entry_ver, &entry_ver));
    // archive updates
    ds_entry_names.push_back(std::move(entry_name));
    ds_entry_vers.push_back(std::move(entry_ver));
    *n_bytes += entry_val.size();
    return ErrorCode::kOK;
  };
  // iterate the directory and put each file as a data entry
  USTORE_GUARD(
    Utils::IterateDirectory(dir_path, f_put_file, ""));
  // update dataset
  std::vector<Slice> ds_entry_names_slice;
  for (auto& en : ds_entry_names) {
    ds_entry_names_slice.emplace_back(Slice(en));
  }
  std::vector<Slice> ds_entry_vers_slice;
  for (auto& ev : ds_entry_vers) {
    ds_entry_vers_slice.emplace_back(Utils::ToSlice(ev));
  }
#if defined(__BLOB_STORE_USE_MAP_MULTI_SET_OP__)
  ds.Set(ds_entry_names_slice, ds_entry_vers_slice);
  USTORE_GUARD(
    odb_.Put(ds_name_slice, ds, branch_slice).stat);
#else
  for (size_t i = 0; i < ds_entry_names.size(); ++i) {
    Dataset ds_update;
    USTORE_GUARD(
      ReadDataset(ds_name_slice, branch_slice, &ds_update));
    auto& entry_name = ds_entry_names_slice[i];
    auto& entry_ver = ds_entry_vers_slice[i];
    ds_update.Set(entry_name, entry_ver);
    USTORE_GUARD(
      odb_.Put(ds_name_slice, ds_update, branch_slice).stat);
  }
#endif
  *n_entries = ds_entry_names.size();
  return ErrorCode::kOK;
}

ErrorCode BlobStore::PutDataEntryByCSV(const std::string& ds_name,
                                       const std::string& branch,
                                       const boost_fs::path& file_path,
                                       const int64_t idx_entry_name,
                                       size_t* n_entries, size_t* n_bytes) {
  *n_entries = 0;
  *n_bytes = 0;
  const Slice ds_name_slice(ds_name), branch_slice(branch);
  // retrieve the operating dataset
  Dataset ds;
  USTORE_GUARD(
    ReadDataset(ds_name_slice, branch_slice, &ds));
  // procedure: put single line (i.e. a data entry) to storage
  std::vector<std::string> ds_entry_names;
  std::vector<Hash> ds_entry_vers;
  const char delim(',');
  const auto f_put_line = [&](const std::string & line) {
    std::string entry_name;
    USTORE_GUARD(
      Utils::ExtractElement(line, idx_entry_name, &entry_name, delim));
    // fetch existing version of the data entry
    auto prev_entry_ver = Utils::ToHash(ds.Get(Slice(entry_name)));
    if (prev_entry_ver.empty()) prev_entry_ver = Hash::kNull;
    // write the data entry to storage
    Hash entry_ver;
    USTORE_GUARD(WriteDataEntry(
                   ds_name, entry_name, line, prev_entry_ver, &entry_ver));
    // archive updates
    ds_entry_names.push_back(std::move(entry_name));
    ds_entry_vers.push_back(std::move(entry_ver));
    *n_bytes += line.size();
    return ErrorCode::kOK;
  };
  USTORE_GUARD(
    Utils::IterateFileByLine(file_path, f_put_line));
  // update dataset
  std::vector<Slice> ds_entry_names_slice;
  for (auto& en : ds_entry_names) {
    ds_entry_names_slice.emplace_back(Slice(en));
  }
  std::vector<Slice> ds_entry_vers_slice;
  for (auto& ev : ds_entry_vers) {
    ds_entry_vers_slice.emplace_back(Utils::ToSlice(ev));
  }
#if defined(__BLOB_STORE_USE_MAP_MULTI_SET_OP__)
  ds.Set(ds_entry_names_slice, ds_entry_vers_slice);
  USTORE_GUARD(
    odb_.Put(ds_name_slice, ds, branch_slice).stat);
#else
  for (size_t i = 0; i < ds_entry_names.size(); ++i) {
    Dataset ds_update;
    USTORE_GUARD(
      ReadDataset(ds_name_slice, branch_slice, &ds_update));
    auto& entry_name = ds_entry_names_slice[i];
    auto& entry_ver = ds_entry_vers_slice[i];
    ds_update.Set(entry_name, entry_ver);
    USTORE_GUARD(
      odb_.Put(ds_name_slice, ds_update, branch_slice).stat);
  }
#endif
  *n_entries = ds_entry_names.size();
  return ErrorCode::kOK;
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

ErrorCode BlobStore::ListDataEntryBranch(const std::string& ds_name,
    const std::string& entry_name, std::vector<std::string>* branches) {
  // retrieve branch candidates
  std::vector<std::string> ds_branches;
  USTORE_GUARD(
    ListDatasetBranch(ds_name, &ds_branches));
  // filter branches that contain the data entry
  branches->clear();
  for (auto& b : ds_branches) {
    bool exists;
    USTORE_GUARD(
      ExistsDataEntry(ds_name, b, entry_name, &exists));
    if (exists) branches->emplace_back(b);
  }
  return ErrorCode::kOK;
}

}  // namespace ustore
