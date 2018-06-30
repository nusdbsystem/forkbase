// Copyright (c) 2017 The Ustore Authors.

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <vector>

#include "lucene_client.h"

namespace ustore {
namespace example {
namespace lucene_client {

#define CMD_HANDLER(cmd, handler) do { \
  cmd_exec_[cmd] = [this] { return handler; }; \
} while (0)

#define CMD_ALIAS(cmd, alias) do { \
  alias_exec_[alias] = &cmd_exec_[cmd]; \
} while (0)

namespace boost_fs = boost::filesystem;

LuceneClient::LuceneClient(LuceneClientArguments& args, DB* db) noexcept
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
  const auto& idx_entry_name = args_.idx_entry_name;
  const auto& raw_idxs_search = args_.idxs_search;
  const auto& file_path = args_.file;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cout << BOLD_RED("[INVALID ARGS: PUT_DATA_ENTRY_BY_CSV] ")
              << "Dataset: \"" << ds_name << "\", "
              << "Branch: \"" << branch << "\", "
              << "Index of Entry Name: " << idx_entry_name << ", "
              << "Indexes for Keyword Search: {" << raw_idxs_search << "}, "
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
              << "Branch: \"" << branch << "\""
              << "Index of Entry Name: " << idx_entry_name << ", "
              << "Indexes for Keyword Search: {" << raw_idxs_search << "}, "
              << "File: \"" << file_path << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (ds_name.empty() || branch.empty() || file_path.empty()
      || idx_entry_name < 0 || raw_idxs_search.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  std::vector<int64_t> idxs_search;
  for (auto& idx_str : Utils::Tokenize(raw_idxs_search, "{}[]()|,;: ")) {
    idxs_search.emplace_back(std::stoi(idx_str));
  }
  size_t n_entries, n_bytes;
  auto ec = bs_.PutDataEntryByCSV(
              ds_name, branch, boost_fs::path(file_path), idx_entry_name,
              idxs_search, &n_entries, &n_bytes);
  ec == ErrorCode::kOK ? f_rpt_success(n_entries, n_bytes) : f_rpt_fail(ec);
  return ec;
}

ErrorCode LuceneClient::ExecGetDataEntryByIndexQuery() {
  const auto& ds_name = args_.dataset;
  const auto& branch = args_.branch;
  const auto& raw_query_keywords = args_.query_keywords;
  const auto& file_path = args_.file;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cout << BOLD_RED("[INVALID ARGS: GET_DATA_ENTRY_BY_INDEX_QUERY] ")
              << "Dataset: \"" << ds_name << "\", "
              << "Branch: \"" << branch << "\", "
              << "Output: "
              << (file_path.empty() ? "<stdout>" : file_path) << ", "
              << "Query Keywords: {" << raw_query_keywords << "}"
              << std::endl;
  };
  const auto f_rpt_success = [&](size_t n_entries, size_t n_bytes) {
    std::cout << BOLD_GREEN("[SUCCESS: GET_DATA_ENTRY_BY_INDEX_QUERY] ")
              << n_entries << " entr" << (n_entries > 1 ? "ies are" : "y is")
              << " retrieved  "
              << BLUE("[" << Utils::StorageSizeString(n_bytes) << "]")
              << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cout << BOLD_RED("[FAILED: GET_DATA_ENTRY_BY_INDEX_QUERY] ")
              << "Dataset: \"" << ds_name << "\", "
              << "Branch: \"" << branch << "\", "
              << "Output: "
              << (file_path.empty() ? "<stdout>" : file_path) << ", "
              << "Query Keywords: {" << raw_query_keywords << "}"
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (ds_name.empty() || branch.empty() || raw_query_keywords.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (!file_path.empty()) {
    try {
      boost_fs::create_directories(boost_fs::path(file_path).parent_path());
    } catch (const boost_fs::filesystem_error& e) {
      f_rpt_fail(ErrorCode::kIOFault);
      return ErrorCode::kIOFault;
    }
  }
  std::ofstream ofs(file_path, std::ios::out | std::ios::trunc);
  std::vector<std::string> query_keywords;
  for (auto& kw : Utils::Split(raw_query_keywords, ',')) {
    query_keywords.push_back(std::move(kw));
  }
  size_t n_entries, n_bytes;
  auto ec = bs_.GetDataEntryByIndexQuery(
              ds_name, branch, query_keywords,
              (file_path.empty() ? std::cout : ofs), &n_entries, &n_bytes);
  ec == ErrorCode::kOK ? f_rpt_success(n_entries, n_bytes) : f_rpt_fail(ec);
  if (file_path.empty()) ofs.close();
  return ec;
}

}  // namespace lucene_client
}  // namespace example
}  // namespace ustore

