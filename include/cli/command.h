// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLI_COMMAND_H_
#define USTORE_CLI_COMMAND_H_

#include <string>
#include <unordered_map>
#include <unordered_set>
#include "spec/object_db.h"
#include "spec/relational.h"
#include "cli/config.h"

namespace ustore {
namespace cli {

#define CMD_HANDLER(cmd, handler) do { \
  cmd_exec_[cmd] = [this]() { return handler(); }; \
} while(0)

#define CMD_ALIAS(cmd, alias) do { \
  alias_exec_[alias] = &cmd_exec_[cmd]; \
} while(0)

class Command {
 public:
  explicit Command(DB* db) noexcept;
  ~Command() = default;

  ErrorCode Run(int argc, char* argv[]);

 protected:
  void PrintCommandHelp(std::ostream& os = std::cout);
  ErrorCode ExecCommand(const std::string& command);
  
  std::unordered_map<std::string, std::function<ErrorCode()>> cmd_exec_;
  std::unordered_map<std::string, std::function<ErrorCode()>*> alias_exec_;

 private:
  ErrorCode ExecScript(const std::string& script);

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
};

}  // namespace cli
}  // namespace ustore

#endif  // USTORE_CLI_COMMAND_H_
