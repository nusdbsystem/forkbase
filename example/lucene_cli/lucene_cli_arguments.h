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
  std::string ref_dataset;
  std::string branch;
  std::string idxs_entry_name;
  std::string idxs_search;
  std::string query_predicate;

  LuceneCLIArguments() {
    AddPositional(&command, "command",
                  "Lucene client command");
    AddPositional(&file, "file",
                  "path of input file");
    Add(&dataset, "dataset", "t",
        "the operating dataset");
    Add(&ref_dataset, "ref-dataset", "s",
        "the referring dataset(s)");
    Add(&branch, "branch", "b",
        "the operating branch");
    Add(&idxs_entry_name, "idxs-entry-name", "m",
        "indices of entry name");
    Add(&idxs_search, "idxs-search", "n",
        "indices for search");
    Add(&query_predicate, "query-predicate", "q",
        "Lucene query predicate");
  }

  ~LuceneCLIArguments() = default;

  std::string MoreHelpMessage() override {
    const std::string cmd = "./ustore_lucene_cli";
    const std::vector<std::string> args = {
      "put-de-by-csv /path/to/input.csv -t ds_test -b master -m 0 -n \"1,3\"",
      "get-de-by-iq -t ds_test -b master -q \"w1 AND w2\" /path/to/output.csv",
      "get-den-by-iq -t ds_test -b master -q \"w1 AND w2\" /path/to/output.csv",
      "get-de-by-iqj -t ds_test -b master -q \"w1 AND w2\" -s \"ds1,ds2\" /path/to/output/dir", // NOLINT
      "get-ds-sch -t ds_test -b master"
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
