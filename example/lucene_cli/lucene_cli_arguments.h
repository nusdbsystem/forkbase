// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_LUCENE_CLI_LUCENE_CLI_ARGUMENTS_H_
#define USTORE_EXAMPLE_LUCENE_CLI_LUCENE_CLI_ARGUMENTS_H_

#include <string>
#include <vector>
#include "utils/arguments.h"

namespace ustore {
namespace example {
namespace lucene_cli {

class LuceneCLIArguments : public ::ustore::Arguments {
 public:
  std::string command;
  std::string file;
  std::string dataset;
  std::string branch;
  int64_t idx_entry_name;
  std::string idxs_search;
  std::string query_keywords;

  LuceneCLIArguments() {
    AddPositional(&command, "command",
                  "Lucene client command");
    AddPositional(&file, "file",
                  "path of input file");
    Add(&dataset, "dataset", "t",
        "the operating dataset");
    Add(&branch, "branch", "b",
        "the operating branch");
    Add(&idx_entry_name, "entry-name-index", "i",
        "index of entry name");
    Add(&idxs_search, "idxs-search", "j",
        "indices for search");
    Add(&query_keywords, "query-keywords", "q",
        "query given by keywords (indexed)");
  }

  ~LuceneCLIArguments() = default;

  std::string MoreHelpMessage() override {
    const std::string cmd = "./ustore_lucene_client";
    const std::vector<std::string> args = {
      "put-de-by-csv /path/to/input.csv -t ds_test -b master -i 0 -j \"1,3\"",
      "get-de-by-iq -t ds_test -b master -q \"K1,K2\" /path/to/output.csv"
    };
    std::stringstream ss;
    ss << "Example:" << std::endl;
    for (auto& a : args) {
      ss << "  " << cmd << " " << a << std::endl;
    }
    ss << std::endl;
    return ss.str();
  }
};

}  // namespace lucene_cli
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_LUCENE_CLI_LUCENE_CLI_ARGUMENTS_H_
