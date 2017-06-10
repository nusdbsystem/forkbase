// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLI_COMMAND_H_
#define USTORE_CLI_COMMAND_H_

#include <boost/algorithm/string.hpp>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "spec/object_db.h"
#include "spec/relational.h"
#include "cli/config.h"

namespace ustore {
namespace cli {

class Command {
 public:
  static inline void Normalize(std::string* cmd) {
    boost::algorithm::to_upper(*cmd);
  }

  static inline bool IsValid(const std::string& cmd) {
    return kSupportedCommands.find(cmd) != kSupportedCommands.end();
  }

  static void PrintCommandHelp();

  explicit Command(DB* db) noexcept;
  ~Command() = default;

  ErrorCode ExecCommand(const std::string& command);
  ErrorCode ExecScript(const std::string& script);
  ErrorCode ExecConsole();

 private:
  static const std::unordered_set<std::string> kSupportedCommands;

  ErrorCode ExecCommand(int argc, char* argv[], const std::string& ctx);

  ErrorCode ExecGet();
  ErrorCode ExecPut();
  ErrorCode ExecMerge();
  ErrorCode ExecBranch();
  ErrorCode ExecRename();
  ErrorCode ExecDelete();
  ErrorCode ExecListKey();
  ErrorCode ExecListBranch();
  ErrorCode ExecHead();
  ErrorCode ExecLatest();
  ErrorCode ExecIsHead();
  ErrorCode ExecIsLatest();
  ErrorCode ExecExists();
  ErrorCode ExecCreateTable();
  ErrorCode ExecGetTable();
  ErrorCode ExecBranchTable();
  ErrorCode ExecListTableBranch();
  ErrorCode ExecDeleteTable();
  ErrorCode ExecGetColumn();
  ErrorCode ExecListColumnBranch();
  ErrorCode ExecDeleteColumn();
  ErrorCode ExecDiff();
  ErrorCode ExecDiffTable();
  ErrorCode ExecDiffColumn();
  ErrorCode ExecExistsTable();
  ErrorCode ExecExistsColumn();
  ErrorCode ExecLoadCSV();
  ErrorCode ExecDumpCSV();

  ObjectDB odb_;
  ColumnStore cs_;
  std::unordered_map<std::string, std::function<ErrorCode()>> cmd_exec_;
  std::unordered_map<std::string, std::function<ErrorCode()>*> alias_exec_;
};

}  // namespace cli
}  // namespace ustore

#endif  // USTORE_CLI_COMMAND_H_
