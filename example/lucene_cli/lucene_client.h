// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_LUCENE_CLI_LUCENE_CLIENT_H_
#define USTORE_EXAMPLE_LUCENE_CLI_LUCENE_CLIENT_H_

#include <string>
#include <unordered_map>

#include "lucene_blob_store.h"
#include "lucene_cli_arguments.h"

namespace ustore {
namespace example {
namespace lucene_cli {

class LuceneClient {
 public:
  explicit LuceneClient(LuceneCLIArguments& args, DB* db) noexcept;
  ~LuceneClient() = default;

  ErrorCode Run();

 private:
  ErrorCode ExecCommand(const std::string& command);

  ErrorCode ExecPutDataEntryByCSV();
  ErrorCode ExecGetDataEntryByIndexQuery();
  ErrorCode ExecGetDataEntryNameByIndexQuery();
  ErrorCode ExecGetDataEntryByIndexQueryAndJoin();
  ErrorCode ExecGetDatasetSchema();

  LuceneBlobStore bs_;
  LuceneCLIArguments& args_;
  std::unordered_map<std::string, std::function<ErrorCode()>> cmd_exec_;
  std::unordered_map<std::string, std::function<ErrorCode()>*> alias_exec_;
};

}  // namespace lucene_cli
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_LUCENE_CLI_LUCENE_CLIENT_H_
