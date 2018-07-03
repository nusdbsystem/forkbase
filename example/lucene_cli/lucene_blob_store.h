// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_LUCENE_CLI_LUCENE_BLOB_STORE_H_
#define USTORE_EXAMPLE_LUCENE_CLI_LUCENE_BLOB_STORE_H_

#include <string>
#include <vector>

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
                              const int64_t idx_entry_name,
                              const std::vector<int64_t>& idxs_search,
                              size_t* n_entries, size_t* n_bytes);

  ErrorCode GetDataEntryByIndexQuery(
    const std::string& ds_name, const std::string& branch,
    const std::vector<std::string>& query_keywords,
    std::ostream& os, size_t* n_entries, size_t* n_bytes) const;

  inline ErrorCode GetDataEntryByIndexQuery(
    const std::string& ds_name, const std::string& branch,
    const std::vector<std::string>& query_keywords, std::ostream& os) const {
    size_t n_entries, n_bytes;
    return GetDataEntryByIndexQuery(
             ds_name, branch, query_keywords, os, &n_entries, &n_bytes);
  }

 private:
  ErrorCode LuceneIndexDataEntries(
    const std::string& ds_name, const std::string& branch,
    const boost::filesystem::path& lucene_index_input_path) const;

  ErrorCode LuceneQueryKeywords(
    const std::string& ds_name, const std::string& branch,
    const std::vector<std::string>& query_keywords,
    std::vector<std::string>* entry_names) const;

  const std::string lucene_file_dir_;
};

}  // namespace lucene_cli
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_LUCENE_CLI_LUCENE_BLOB_STORE_H_
