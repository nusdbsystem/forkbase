// Copyright (c) 2017 The Ustore Authors.

#include "lucene_blob_store.h"

#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <utility>
#include "utils/env.h"

#include "http/http_client.h"

namespace ustore {
namespace example {
namespace lucene_cli {

namespace boost_fs = boost::filesystem;

LuceneBlobStore::LuceneBlobStore(DB* db) noexcept
  : BlobStore(db),
    lucene_file_dir_(Utils::FullPath(Env::Instance()->config().data_dir())) {}

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
  time_t now;
  time(&now);
  const std::string lucene_file_path(
    lucene_file_dir_ + "/lucene/docs/lucene_index_input_" +
    std::to_string(now) + ".csv");
  const boost_fs::path lucene_index_input_path(lucene_file_path);
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
  const auto f_put_line = [&](const std::string & line) {
    ++line_cnt;
    const auto elements = Utils::Split(line, ',');
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
      try {
        ofs_lucene_index << ',' << elements.at(idxs_search[i]);
      } catch (const std::out_of_range& e) {
        LOG(ERROR) << "No element " << idxs_search[i] << " in line "
                   << line_cnt << ": \"" << line << "\"";
        return ErrorCode::kIndexOutOfRange;
      }
    }
    ofs_lucene_index << std::endl;
    return ErrorCode::kOK;
  };
  // iterate the input file by line
  const auto ec = Utils::IterateFileByLine(file_path, f_put_line);
  ofs_lucene_index.close();
  if (ec != ErrorCode::kOK) {
    Utils::DeleteFile(lucene_file_path);
    return ec;
  }
  // call lucene to index the data
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
  ustore::http::HttpClient hc;
  ustore::http::Request request("/index", ustore::http::Verb::kPost);
  // connect
  hc.Connect("localhost", "61000");
  // send
  std::string body = "{\"dataset\":\"" + ds_name + "\","
                     + "\"branch\":\"" + branch + "\", "
                     + "\"dir\":\"" + lucene_index_input_path.native() + "\"}";
  request.SetHeaderField("host", "localhost");
  request.SetHeaderField("Content-Type", "application/json");
  request.SetBody(body);
  hc.Send(&request);
  // read response
  ustore::http::Response response;
  hc.Receive(&response);
  // extract data
  std::stringstream json_str(response.body());
  boost::property_tree::ptree pt;
  boost::property_tree::read_json(json_str, pt);
  int status = pt.get<int>("status");
  string msg = pt.get<std::string>("msg");
  // close client
  hc.Shutdown();
  if (status != 0) {
    LOG(ERROR) << msg;
    switch (status) {
      case 1:
        return ErrorCode::kInvalidPath;
      case 2:
        return ErrorCode::kIOFault;
      default:
        return ErrorCode::kUnknownOp;
    }
  }
  return ErrorCode::kOK;
}

ErrorCode LuceneBlobStore::GetDataEntryByIndexQuery(
  const std::string& ds_name, const std::string& branch,
  const std::string& query_predicate, std::ostream& os,
  size_t* n_entries, size_t* n_bytes) const {
  *n_entries = 0;
  *n_bytes = 0;
  // retrieve the operating dataset
  Dataset ds;
  USTORE_GUARD(
    GetDataset(ds_name, branch, &ds));
  // retrieve entry names associated with the query keywords
#if defined(__LUCENE_BLOB_STORE_DEDUP_QUERY_RESULTS__)
  std::unordered_set<std::string> ds_entry_names;
#else
  std::vector<std::string> ds_entry_names;
#endif
  USTORE_GUARD(
    LuceneQuery(ds_name, branch, query_predicate, &ds_entry_names));
  // retrieve data entries
  for (auto& entry_name : ds_entry_names) {
    auto entry_ver = Utils::ToHash(ds.Get(Slice(entry_name)));
    if (entry_ver.empty()) {
      LOG(WARNING) << "Data Entry \"" << entry_name
                   << "\" does not exist in Dataset \""
                   << ds_name << "\" of Branch \"" << branch << "\"";
      return ErrorCode::kDataEntryNotExists;
    }
    // read data entry
    DataEntry entry;
    USTORE_GUARD(
      ReadDataEntry(ds_name, entry_name, entry_ver, &entry));
    os << entry << std::endl;
    ++(*n_entries);
    *n_bytes += entry.size();
  }
  *n_bytes += *n_entries;  // count for std::endl
  return ErrorCode::kOK;
}

#if defined(__LUCENE_BLOB_STORE_DEDUP_QUERY_RESULTS__)
ErrorCode LuceneBlobStore::LuceneQuery(
  const std::string& ds_name, const std::string& branch,
  const std::string& query_predicate,
  std::unordered_set<std::string>* entry_names) const
#else
ErrorCode LuceneBlobStore::LuceneQuery(
  const std::string& ds_name, const std::string& branch,
  const std::string& query_predicate,
  std::vector<std::string>* entry_names) const
#endif
{
  entry_names->clear();
  // parse post data
  std::string body = "{\"dataset\":\"" + ds_name + "\",\"branch\":\"" +
                     branch + "\",\"query\":\"" + query_predicate + "\"}";
  // connect
  ustore::http::HttpClient hc;
  ustore::http::Request request("/search", ustore::http::Verb::kPost);
  hc.Connect("localhost", "61000");
  // send
  request.SetHeaderField("host", "localhost");
  request.SetHeaderField("Content-Type", "application/json");
  request.SetBody(body);
  hc.Send(&request);
  // receive
  ustore::http::Response response;
  hc.Receive(&response);
  // extract data
  std::stringstream json_str(response.body());
  boost::property_tree::ptree pt;
  boost::property_tree::read_json(json_str, pt);
  int status = pt.get<int>("status");
  string msg = pt.get<std::string>("msg");
  BOOST_FOREACH(
    boost::property_tree::ptree::value_type & entry, pt.get_child("docs")) {
#if defined(__LUCENE_BLOB_STORE_DEDUP_QUERY_RESULTS__)
    entry_names->emplace(entry.second.get<std::string>("key"));
#else
    entry_names->emplace_back(entry.second.get<std::string>("key"));
#endif
  }
  // close connection
  hc.Shutdown();
  if (status != 0) {
    LOG(ERROR) << msg;
    switch (status) {
      case 1:
        return ErrorCode::kInvalidPath;
      case 2:
        return ErrorCode::kIOFault;
      default:
        return ErrorCode::kUnknownOp;
    }
  }
  return ErrorCode::kOK;
}

}  // namespace lucene_cli
}  // namespace example
}  // namespace ustore
