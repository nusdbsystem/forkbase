// Copyright (c) 2017 The Ustore Authors.

#include "lucene_blob_store.h"

namespace ustore {
namespace example {
namespace lucene_client {

namespace boost_fs = boost::filesystem;

ErrorCode LuceneBlobStore::PutDataEntryByCSV(
  const std::string& ds_name,
  const std::string& branch,
  const boost_fs::path& file_path,
  const int64_t idx_entry_name,
  const std::vector<int64_t>& idxs_search,
  size_t* n_entries, size_t* n_bytes) {
  *n_entries = 0;
  *n_bytes = 0;
  const Slice ds_name_slice(ds_name), branch_slice(branch);
  // retrieve the operating dataset
  Dataset ds;
  USTORE_GUARD(
    ReadDataset(ds_name_slice, branch_slice, &ds));
  // create file of lucene index input
  // TODO (yuecong): configure the path of the lucene index input file
  const boost_fs::path lucene_index_input_path(
    "/tmp/ustore/lucene_client/lucene_index_input.csv");
  try {
    boost_fs::create_directories(lucene_index_input_path.parent_path());
  } catch (const boost_fs::filesystem_error& e) {
    LOG(ERROR) << e.what();
    return ErrorCode::kIOFault;
  }
  std::ofstream ofs_lucene_index(
    lucene_index_input_path.native(), std::ios::out | std::ios::trunc);
  // procedure: put single line (i.e. a data entry) to storage
  std::vector<std::string> ds_entry_names;
  std::vector<Hash> ds_entry_vers;
  size_t line_cnt(0);
  const char delim(',');
  const auto f_put_line = [&](const std::string & line) {
    ++line_cnt;
    const auto elements = Utils::Split(line, delim);
    if (idx_entry_name >= static_cast<int64_t>(elements.size())) {
      LOG(ERROR) << "Failed to extract entry name in line " << line_cnt
                 << ": \"" << line << "\"";
      return ErrorCode::kIndexOutOfRange;
    }
    auto& entry_name = elements[idx_entry_name];
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
    // add lucene index entry
    ofs_lucene_index << entry_name;
    for (size_t i = 0; i < idxs_search.size(); ++i) {
      ofs_lucene_index << ',' << elements[idxs_search[i]];
    }
    ofs_lucene_index << std::endl;
    return ErrorCode::kOK;
  };
  USTORE_GUARD(
    Utils::IterateFileByLine(file_path, f_put_line));
  ofs_lucene_index.close();
  USTORE_GUARD(
    LuceneIndexDataEntries(ds_name, branch, lucene_index_input_path));
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

ErrorCode LuceneBlobStore::LuceneIndexDataEntries(
  const std::string& ds_name,
  const std::string& branch,
  const boost_fs::path& lucene_index_input_path) const {
  // TODO (yuecong): implement the remote procedure call
  std::cout << CYAN("[TODO: Lucene RPC] ")
            << "Dataset: \"" << ds_name << "\", "
            << "Branch: \"" << branch << "\", "
            << "Index Input: " << lucene_index_input_path.native()
            << std::endl;
  return ErrorCode::kOK;
}

}  // namespace lucene_client
}  // namespace example
}  // namespace ustore
