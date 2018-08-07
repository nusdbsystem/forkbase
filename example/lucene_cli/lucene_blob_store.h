// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_LUCENE_CLI_LUCENE_BLOB_STORE_H_
#define USTORE_EXAMPLE_LUCENE_CLI_LUCENE_BLOB_STORE_H_

// #define __LUCENE_BLOB_STORE_DEDUP_QUERY_RESULTS__

#include <string>
#include <vector>
#if defined(__LUCENE_BLOB_STORE_DEDUP_QUERY_RESULTS__)
#include <unordered_set>
#endif

#include "spec/blob_store.h"

namespace ustore {
namespace example {
namespace lucene_cli {

class LuceneBlobStore : public BlobStore {
 public:
  explicit LuceneBlobStore(DB* db) noexcept;
  ~LuceneBlobStore() = default;

  ErrorCode PutDataEntryByCSV(const std::string& ds_name,
                              const std::string& branch,
                              const boost::filesystem::path& file_path,
                              const std::vector<size_t>& idxs_entry_name,
                              const std::vector<size_t>& idxs_search,
                              size_t* n_entries, size_t* n_bytes);

  ErrorCode GetDataEntryByIndexQuery(
    const std::string& ds_name, const std::string& branch,
    const std::string& query_predicate, std::ostream& os,
    size_t* n_entries, size_t* n_bytes) const;

  inline ErrorCode GetDataEntryByIndexQuery(
    const std::string& ds_name, const std::string& branch,
    const std::string& query_predicate, std::ostream& os) const {
    size_t n_entries, n_bytes;
    return GetDataEntryByIndexQuery(
             ds_name, branch, query_predicate, os, &n_entries, &n_bytes);
  }

  inline ErrorCode GetLuceneSearchIndices(
    const std::string& ds_name,
    const std::string& branch,
    std::string* str_idxs_search) const {
    return GetMeta(ds_name, branch, "SEARCH INDICES", str_idxs_search);
  }

 protected:
  std::string RegularizeSchema(const std::string& origin) const override;

 private:
  inline std::string GenerateLuceneFilePath() {
    time_t now;
    time(&now);
    return lucene_file_dir_ + "/lucene/docs/lucene_index_input_" +
           std::to_string(now) + ".csv";
  }

  ErrorCode LuceneIndexDataEntries(
    const std::string& ds_name, const std::string& branch,
    const boost::filesystem::path& lucene_index_input_path) const;

#if defined(__LUCENE_BLOB_STORE_DEDUP_QUERY_RESULTS__)
  ErrorCode LuceneQuery(
    const std::string& ds_name, const std::string& branch,
    const std::string& query_predicate,
    std::unordered_set<std::string>* entry_names) const;
#else
  ErrorCode LuceneQuery(
    const std::string& ds_name, const std::string& branch,
    const std::string& query_predicate,
    std::vector<std::string>* entry_names) const;
#endif

  const std::string lucene_file_dir_;
};

}  // namespace lucene_cli
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_LUCENE_CLI_LUCENE_BLOB_STORE_H_
