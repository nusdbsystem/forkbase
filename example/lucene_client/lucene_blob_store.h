// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_LUCENE_CLIENT_LUCENE_BLOB_STORE_H_
#define USTORE_EXAMPLE_LUCENE_CLIENT_LUCENE_BLOB_STORE_H_

#include "spec/blob_store.h"

namespace ustore {
namespace example {
namespace lucene_client {

class LuceneBlobStore : public BlobStore {
 public:
  explicit LuceneBlobStore(DB* db) noexcept : BlobStore(db) {}
  ~LuceneBlobStore() = default;

  ErrorCode PutDataEntryByCSV(const std::string& ds_name,
                              const std::string& branch,
                              const boost::filesystem::path& file_path,
                              const int64_t idx_entry_name,
                              const std::vector<int64_t>& idxs_search,
                              size_t* n_entries, size_t* n_bytes);

 private:
  ErrorCode LuceneIndexDataEntries(
    const std::string& ds_name, const std::string& branch,
    const boost::filesystem::path& lucene_index_input_path) const;
};

}  // namespace lucene_client
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_LUCENE_CLIENT_LUCENE_BLOB_STORE_H_
