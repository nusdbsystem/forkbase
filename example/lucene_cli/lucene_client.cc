// Copyright (c) 2017 The Ustore Authors.

#include "lucene_client.h"

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <utility>
#include <vector>

namespace ustore {
namespace example {
namespace lucene_cli {

#define CMD_HANDLER(cmd, handler) do { \
  cmd_exec_[cmd] = [this] { return handler; }; \
} while (0)

#define CMD_ALIAS(cmd, alias) do { \
  alias_exec_[alias] = &cmd_exec_[cmd]; \
} while (0)

namespace boost_fs = boost::filesystem;

LuceneClient::LuceneClient(LuceneCLIArguments& args, DB* db) noexcept
  : bs_(db), args_(args) {
  boost::to_upper(args_.command);
  CMD_HANDLER("PUT_DATA_ENTRY_BY_CSV", ExecPutDataEntryByCSV());
  CMD_ALIAS("PUT_DATA_ENTRY_BY_CSV", "PUT-DATA-ENTRY-BY-CSV");
  CMD_ALIAS("PUT_DATA_ENTRY_BY_CSV", "PUT_DE_BY_CSV");
  CMD_ALIAS("PUT_DATA_ENTRY_BY_CSV", "PUT-DE-BY-CSV");
  CMD_HANDLER("GET_DATA_ENTRY_BY_INDEX_QUERY", ExecGetDataEntryByIndexQuery());
  CMD_ALIAS("GET_DATA_ENTRY_BY_INDEX_QUERY", "GET-DATA-ENTRY-BY-INDEX-QUERY");
  CMD_ALIAS("GET_DATA_ENTRY_BY_INDEX_QUERY", "GET_DE_BY_IQ");
  CMD_ALIAS("GET_DATA_ENTRY_BY_INDEX_QUERY", "GET-DE-BY-IQ");
  CMD_HANDLER("GET_DATA_ENTRY_NAME_BY_INDEX_QUERY", ExecGetDataEntryNameByIndexQuery()); // NOLINT
  CMD_ALIAS("GET_DATA_ENTRY_NAME_BY_INDEX_QUERY", "GET-DATA-ENTRY-NAME-BY-INDEX-QUERY"); // NOLINT
  CMD_ALIAS("GET_DATA_ENTRY_NAME_BY_INDEX_QUERY", "GET_DEN_BY_IQ");
  CMD_ALIAS("GET_DATA_ENTRY_NAME_BY_INDEX_QUERY", "GET-DEN-BY-IQ");
  CMD_HANDLER("GET_DATASET_SCHEMA", ExecGetDatasetSchema());
  CMD_ALIAS("GET_DATASET_SCHEMA", "GET-DATASET-SCHEMA");
  CMD_ALIAS("GET_DATASET_SCHEMA", "GET_DS_SCH");
  CMD_ALIAS("GET_DATASET_SCHEMA", "GET-DS-SCH");
}

ErrorCode LuceneClient::Run() {
  if (args_.command.empty()) {
    std::cout << BOLD_RED("[ERROR] ")
              << "Command missing" << std::endl;
    return ErrorCode::kInvalidCommandArgument;
  }
  return ExecCommand(args_.command);
}

ErrorCode LuceneClient::ExecCommand(const std::string& cmd) {
  auto it_cmd_exec = cmd_exec_.find(cmd);
  if (it_cmd_exec == cmd_exec_.end()) {
    auto it_alias_exec = alias_exec_.find(cmd);
    if (it_alias_exec == alias_exec_.end()) {
      std::cout << BOLD_RED("[ERROR] ")
                << "Unknown command: " << cmd << std::endl;
      return ErrorCode::kUnknownCommand;
    }
    return (*(it_alias_exec->second))();
  }
  return it_cmd_exec->second();
}

ErrorCode LuceneClient::ExecPutDataEntryByCSV() {
  const auto& ds_name = args_.dataset;
  const auto& branch = args_.branch;
  const auto& raw_idxs_en = args_.idxs_entry_name;
  const auto& raw_idxs_search = args_.idxs_search;
  const auto& file_path = args_.file;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cout << BOLD_RED("[INVALID ARGS: PUT_DATA_ENTRY_BY_CSV] ")
              << "Dataset: \"" << ds_name << "\", "
              << "Branch: \"" << branch << "\", "
              << "Indices of Entry Name Attributes: {" << raw_idxs_en << "}, "
              << "Indices for Keyword Search: {"
              << (raw_idxs_search.empty() ? "ALL" : raw_idxs_search) << "}, "
              << "File: \"" << file_path << "\"" << std::endl;
  };
  const auto f_rpt_success = [&](size_t n_entries, size_t n_bytes) {
    std::cout << BOLD_GREEN("[SUCCESS: PUT_DATA_ENTRY_BY_CSV] ")
              << n_entries << " entr" << (n_entries > 1 ? "ies are" : "y is")
              << " updated  "
              << BLUE("[" << Utils::StorageSizeString(n_bytes) << "]")
              << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cout << BOLD_RED("[FAILED: PUT_DATA_ENTRY_BY_CSV] ")
              << "Dataset: \"" << ds_name << "\", "
              << "Branch: \"" << branch << "\", "
              << "Indices of Entry Name Attributes: {" << raw_idxs_en << "}, "
              << "Indices for Keyword Search: {"
              << (raw_idxs_search.empty() ? "ALL" : raw_idxs_search) << "}, "
              << "File: \"" << file_path << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (ds_name.empty() || branch.empty() || file_path.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  std::vector<size_t> idxs_entry_name;
  auto ec = Utils::ToIndices(raw_idxs_en, &idxs_entry_name);
  if (ec != ErrorCode::kOK) {
    f_rpt_fail(ec);
    return ec;
  }
  std::vector<size_t> idxs_search;
  ec = Utils::ToIndices(raw_idxs_search, &idxs_search);
  if (ec != ErrorCode::kOK) {
    f_rpt_fail(ec);
    return ec;
  }
  size_t n_entries, n_bytes;
  ec = bs_.PutDataEntryByCSV(ds_name, branch, boost_fs::path(file_path),
                             idxs_entry_name, idxs_search,
                             &n_entries, &n_bytes);
  ec == ErrorCode::kOK ? f_rpt_success(n_entries, n_bytes) : f_rpt_fail(ec);
  return ec;
}

ErrorCode LuceneClient::ExecGetDataEntryByIndexQuery() {
  const auto& ds_name = args_.dataset;
  const auto& branch = args_.branch;
  const auto& query_predicate = args_.query_predicate;
  const auto& file_path = args_.file;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cout << BOLD_RED("[INVALID ARGS: GET_DATA_ENTRY_BY_INDEX_QUERY] ")
              << "Dataset: \"" << ds_name << "\", "
              << "Branch: \"" << branch << "\", "
              << "Output: "
              << (file_path.empty() ? "<stdout>" : file_path) << ", "
              << "Query: \"" << query_predicate << "\""
              << std::endl;
  };
  const auto f_rpt_success = [&](size_t n_entries, size_t n_bytes) {
    std::cout << BOLD_GREEN("[SUCCESS: GET_DATA_ENTRY_BY_INDEX_QUERY] ");
    if (n_entries == 0) {
      std::cout << "no entry is found";
    } else {
      std::cout << n_entries << " entr" << (n_entries > 1 ? "ies are" : "y is")
                << " retrieved";
      if (!file_path.empty()) {
        std::cout << " --> " << Utils::FullPath(file_path);
      }
      std::cout << BLUE("  [" << Utils::StorageSizeString(n_bytes) << "]");
    }
    std::cout << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cout << BOLD_RED("[FAILED: GET_DATA_ENTRY_BY_INDEX_QUERY] ")
              << "Dataset: \"" << ds_name << "\", "
              << "Branch: \"" << branch << "\", "
              << "Output: "
              << (file_path.empty() ? "<stdout>" : file_path) << ", "
              << "Query: \"" << query_predicate << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (ds_name.empty() || branch.empty() || query_predicate.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (!file_path.empty()) {
    const auto ec = Utils::CreateParentDirectories(file_path);
    if (ec != ErrorCode::kOK) {
      f_rpt_fail(ec);
      return ec;
    }
  }
  std::ofstream ofs(file_path, std::ios::out | std::ios::trunc);
  size_t n_entries, n_bytes;
  auto ec = bs_.GetDataEntryByIndexQuery(
              ds_name, branch, query_predicate,
              (file_path.empty() ? std::cout : ofs), &n_entries, &n_bytes);
  ec == ErrorCode::kOK ? f_rpt_success(n_entries, n_bytes) : f_rpt_fail(ec);
  if (!file_path.empty()) {
    ofs.close();
    if (n_entries == 0) {  // delete the trivial file since no factual output
      Utils::DeleteFile(file_path);
    }
  }
  return ec;
}

ErrorCode LuceneClient::ExecGetDataEntryNameByIndexQuery() {
  const auto& ds_name = args_.dataset;
  const auto& branch = args_.branch;
  const auto& query_predicate = args_.query_predicate;
  const auto& file_path = args_.file;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cout << BOLD_RED("[INVALID ARGS: GET_DATA_ENTRY_NAME_BY_INDEX_QUERY] ")
              << "Dataset: \"" << ds_name << "\", "
              << "Branch: \"" << branch << "\", "
              << "Output: "
              << (file_path.empty() ? "<stdout>" : file_path) << ", "
              << "Query: \"" << query_predicate << "\""
              << std::endl;
  };
  const auto f_rpt_success = [&](size_t n_entries, size_t n_bytes) {
    std::cout << BOLD_GREEN("[SUCCESS: GET_DATA_ENTRY_NAME_BY_INDEX_QUERY] ");
    if (n_entries == 0) {
      std::cout << "no entry is found";
    } else {
      std::cout << n_entries << " entr" << (n_entries > 1 ? "ies are" : "y is")
                << " retrieved";
      if (!file_path.empty()) {
        std::cout << " --> " << Utils::FullPath(file_path);
      }
      std::cout << BLUE("  [" << Utils::StorageSizeString(n_bytes) << "]");
    }
    std::cout << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cout << BOLD_RED("[FAILED: GET_DATA_ENTRY_NAME_BY_INDEX_QUERY] ")
              << "Dataset: \"" << ds_name << "\", "
              << "Branch: \"" << branch << "\", "
              << "Output: "
              << (file_path.empty() ? "<stdout>" : file_path) << ", "
              << "Query: \"" << query_predicate << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (ds_name.empty() || branch.empty() || query_predicate.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (!file_path.empty()) {
    const auto ec = Utils::CreateParentDirectories(file_path);
    if (ec != ErrorCode::kOK) {
      f_rpt_fail(ec);
      return ec;
    }
  }
  std::ofstream ofs(file_path, std::ios::out | std::ios::trunc);
  size_t n_entries, n_bytes;
  auto ec = bs_.GetDataEntryNameByIndexQuery(
              ds_name, branch, query_predicate,
              (file_path.empty() ? std::cout : ofs), &n_entries, &n_bytes);
  ec == ErrorCode::kOK ? f_rpt_success(n_entries, n_bytes) : f_rpt_fail(ec);
  if (!file_path.empty()) {
    ofs.close();
    if (n_entries == 0) {  // delete the trivial file since no factual output
      Utils::DeleteFile(file_path);
    }
  }
  return ec;
}

ErrorCode LuceneClient::ExecGetDatasetSchema() {
  const auto& ds_name = args_.dataset;
  const auto& branch = args_.branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cout << BOLD_RED("[INVALID ARGS: GET_DATASET_SCHEMA] ")
              << "Dataset: \"" << ds_name << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  using ConStr = const std::string;
  const auto f_rpt_success =
  [](ConStr & schema, ConStr & idxs_en, ConStr & idxs_search) {
    std::cout << BOLD_GREEN("[SUCCESS: GET_DATASET_SCHEMA] ")
              << "Schema: \"" << schema << "\", "
              << "Indices of Entry Name Attributes: {" << idxs_en << "}, "
              << "Indices of Attributes for Lucene Search: {"
              << (idxs_search.empty() ? "ALL" : idxs_search) << "}"
              << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cout << BOLD_RED("[FAILED: GET_DATASET_SCHEMA] ")
              << "Dataset: \"" << ds_name << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (ds_name.empty() || branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  std::string schema;
  auto ec = bs_.GetDatasetSchema(ds_name, branch, &schema);
  if (ec != ErrorCode::kOK) {
    f_rpt_fail(ec);
    return ec;
  }
  std::string idxs_en;
  ec = bs_.GetDataEntryNameIndices(ds_name, branch, &idxs_en);
  if (ec != ErrorCode::kOK) {
    f_rpt_fail(ec);
    return ec;
  }
  std::string idxs_search;
  ec = bs_.GetLuceneSearchIndices(ds_name, branch, &idxs_search);
  if (ec != ErrorCode::kOK) {
    f_rpt_fail(ec);
    return ec;
  }
  f_rpt_success(schema, idxs_en, idxs_search);
  return ec;
}

}  // namespace lucene_cli
}  // namespace example
}  // namespace ustore

