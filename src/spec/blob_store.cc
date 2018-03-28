// Copyright (c) 2017 The UStore Authors.

#include <fstream>
#include "spec/blob_store.h"

namespace ustore {

namespace boost_fs = boost::filesystem;

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
  return odb_.Delete(Slice(ds_name), Slice(branch));
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
    ReadDataset(Slice(ds_name), Slice(branch), &ds));
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

ErrorCode BlobStore::GetDataEntryBatch(const std::string& ds_name,
                                       const std::string& branch,
                                       const boost_fs::path& dir_path,
                                       size_t* n_entries) {
  *n_entries = 0;
  // retrieve the operating dataset
  Dataset ds;
  USTORE_GUARD(
    ReadDataset(Slice(ds_name), Slice(branch), &ds));
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
                                       size_t* n_entries) {
  *n_entries = 0;
  const Slice ds_name_slice(ds_name), branch_slice(branch);
  // retrieve the operating dataset
  Dataset ds;
  USTORE_GUARD(
    ReadDataset(ds_name_slice, branch_slice, &ds));
  try {
    if (boost_fs::exists(dir_path) && boost_fs::is_directory(dir_path)) {
      std::vector<std::string> ds_entry_names;
      std::vector<Hash> ds_entry_vers;
      // iterate the directory containing data entry files
      for (auto && file_entry : boost_fs::directory_iterator(dir_path)) {
        // construct data entry name and value
        const auto file_path = file_entry.path();
        const std::string entry_name(file_path.filename().native());
        std::string entry_val;
        USTORE_GUARD(
          Utils::GetFileContents(file_path.native(), &entry_val));
        // fetch existing version of the data entry
        auto prev_entry_ver = Utils::ToHash(ds.Get(Slice(entry_name)));
        if (prev_entry_ver.empty()) prev_entry_ver = Hash::kNull;
        // write the data entry to storage
        Hash entry_ver;
        USTORE_GUARD(WriteDataEntry(ds_name, entry_name, entry_val,
                                    prev_entry_ver, &entry_ver));
        // archive updates
        ds_entry_names.push_back(std::move(entry_name));
        ds_entry_vers.push_back(std::move(entry_ver));
      }
      // update dataset
      std::vector<Slice> ds_entry_names_slice;
      for (auto& en : ds_entry_names) {
        ds_entry_names_slice.emplace_back(Slice(en));
      }
      std::vector<Slice> ds_entry_vers_slice;
      for (auto& ev : ds_entry_vers) {
        ds_entry_vers_slice.emplace_back(Utils::ToSlice(ev));
      }
      ds.Set(ds_entry_names_slice, ds_entry_vers_slice);
      USTORE_GUARD(
        odb_.Put(ds_name_slice, ds, branch_slice).stat);
      *n_entries = ds_entry_names.size();
      return ErrorCode::kOK;
    } else {
      LOG(ERROR) << "Invalid directory: " << dir_path;
      return ErrorCode::kInvalidPath;
    }
  } catch (const boost_fs::filesystem_error& e) {
    LOG(ERROR) << e.what();
    return ErrorCode::kIOFault;
  }
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
