// Copyright (c) 2017 The Ustore Authors.

#include "lucene_blob_store.h"

#include <boost/algorithm/string.hpp>
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
  size_t* n_entries,
  size_t* n_bytes) {
  USTORE_GUARD(
    BlobStore::PutDataEntryByCSV(ds_name, branch, file_path, idx_entry_name,
                                 n_entries, n_bytes, true));
  // copy the whole file for indexing
  const std::string lucene_file_path(GenerateLuceneFilePath());
  const boost_fs::path lucene_index_input_path(lucene_file_path);
  Utils::CreateParentDirectories(lucene_index_input_path);
  return Utils::CopyFile(file_path, lucene_index_input_path);
}

ErrorCode LuceneBlobStore::PutDataEntryByCSV(
  const std::string& ds_name,
  const std::string& branch,
  const boost_fs::path& file_path,
  const int64_t idx_entry_name,
  const std::vector<int64_t>& idxs_search,
  size_t* n_entries,
  size_t* n_bytes) {
  *n_entries = 0;
  *n_bytes = 0;
  const Slice ds_name_slice(ds_name), branch_slice(branch);
  // retrieve the operating dataset
  Dataset ds;
  USTORE_GUARD(
    ReadDataset(ds_name_slice, branch_slice, &ds));
  // create file of lucene index input
  const std::string lucene_file_path(GenerateLuceneFilePath());
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
  char delim(',');
  std::unordered_map<std::string, Hash> updates;
  size_t line_cnt(0);
  const auto f_put_line = [&](const std::string & line) {
    ++line_cnt;
    const auto elements = Utils::Split(line, delim);
    if (idx_entry_name >= static_cast<int64_t>(elements.size())) {
      LOG(ERROR) << "Failed to extract entry name in line " << line_cnt
                 << ": \"" << line << "\"";
      return ErrorCode::kIndexOutOfRange;
    }
    const auto& entry_name = elements[idx_entry_name];
    if (line_cnt == 1) {  // process schema at the 1st line
      std::string schema;
      USTORE_GUARD(
        GetMeta(ds_name, branch, "SCHEMA", &schema));
      const auto line_trim = boost::trim_copy(line);
      if (schema.empty()) {
        USTORE_GUARD(
          SetMeta(ds_name, branch, "SCHEMA", line_trim));
        *n_bytes += line_trim.size();
      } else if (line_trim != schema) {  // compare with the recorded schema
        return ErrorCode::kDatasetSchemaMismatch;
      }
    } else {  // for 2nd line onwards
      // fetch existing version of the data entry
      auto prev_entry_ver = Utils::ToHash(ds.Get(Slice(entry_name)));
      if (prev_entry_ver.empty()) prev_entry_ver = Hash::kNull;
      // write the data entry to storage
      Hash entry_ver;
      USTORE_GUARD(WriteDataEntry(
                     ds_name, entry_name, line, prev_entry_ver, &entry_ver));
      // archive updates
      updates.emplace(entry_name, std::move(entry_ver));
      *n_bytes += line.size();
    }
    // add lucene index entry
    ofs_lucene_index << entry_name;
    for (size_t i = 0; i < idxs_search.size(); ++i) {
      try {
        ofs_lucene_index << delim << elements.at(idxs_search[i]);
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
#if defined(__BLOB_STORE_USE_MAP_MULTI_SET_OP__)
  ds.Insert(updates);
  USTORE_GUARD(
    odb_.Put(ds_name_slice, ds, branch_slice).stat);
#else
  for (auto& kv : updates) {
    Dataset ds_update;
    USTORE_GUARD(
      ReadDataset(ds_name_slice, branch_slice, &ds_update));
    const auto en_name = Slice(kv.first);
    const auto en_ver = Utils::ToSlice(kv.second);
    ds_update.Set(en_name, en_ver);
    USTORE_GUARD(
      odb_.Put(ds_name_slice, ds_update, branch_slice).stat);
  }
#endif
  *n_entries = updates.size();
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
  // retrieve entry names associated with the query keywords
#if defined(__LUCENE_BLOB_STORE_DEDUP_QUERY_RESULTS__)
  std::unordered_set<std::string> ds_entry_names;
#else
  std::vector<std::string> ds_entry_names;
#endif
  USTORE_GUARD(
    LuceneQuery(ds_name, branch, query_predicate, &ds_entry_names));
  if (ds_entry_names.empty()) {
    LOG(INFO) << "No result is found for query \"" << query_predicate
              << "\" on dataset \"" << ds_name << "\" of branch \"" << branch
              << "\"";
    return ErrorCode::kOK;
  }
  // write schema to the 1st line
  std::string schema;
  USTORE_GUARD(
    GetMeta(ds_name, branch, "SCHEMA", &schema));
  if (schema.empty()) {  // schema constraint
    LOG(ERROR) << "Schema is not found for dataset \"" << ds_name
               << "\" of branch \"" << branch << "\"";
    return ErrorCode::kDatasetSchemaNotFound;
  }
  os << schema << std::endl;
  *n_bytes += schema.size() + 1;
  // retrieve the operating dataset
  Dataset ds;
  USTORE_GUARD(
    GetDataset(ds_name, branch, &ds));
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
