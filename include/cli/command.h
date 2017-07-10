// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLI_COMMAND_H_
#define USTORE_CLI_COMMAND_H_

#include <iomanip>
#include <string>
#include <unordered_map>
#include "spec/object_db.h"
#include "spec/relational.h"
#include "utils/timer.h"
#include "utils/utils.h"
#include "cli/config.h"

namespace ustore {
namespace cli {

#define CMD_HANDLER(cmd, handler) do { \
  cmd_exec_[cmd] = [this] { return handler; }; \
} while (0)

#define CMD_ALIAS(cmd, alias) do { \
  alias_exec_[alias] = &cmd_exec_[cmd]; \
} while (0)

#define FORMAT_CMD(cmd, width) \
  "* " << std::left << std::setw(width) << cmd << " "

class Command {
 public:
  explicit Command(DB* db) noexcept;
  ~Command() = default;

  ErrorCode Run(int argc, char* argv[]);

 protected:
  virtual void PrintHelp();
  void PrintCommandHelp(std::ostream& os = std::cout);

  ErrorCode ExecCommand(const std::string& command);

  inline std::string TimeDisplay(const std::string& prefix = "",
                                 const std::string& suffix = "") {
    std::string time_display("");
    if (Config::time_exec && !Config::is_vert_list) {
      time_display =
        prefix + "(in " + Utils::TimeString(time_ms_) + ")" + suffix;
    }
    return time_display;
  }

  inline void Time(const std::function<void()>& f_exec) {
    time_ms_ = Timer::TimeMilliseconds(f_exec);
  }

  std::unordered_map<std::string, std::function<ErrorCode()>> cmd_exec_;
  std::unordered_map<std::string, std::function<ErrorCode()>*> alias_exec_;
  double time_ms_;

 private:
  ErrorCode ExecScript(const std::string& script);

  ErrorCode ExecHelp();
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
  ErrorCode ExecDiffTable();
  ErrorCode ExecDiffColumn();
  ErrorCode ExecExistsTable();
  ErrorCode ExecExistsColumn();
  ErrorCode ExecLoadCSV();
  ErrorCode ExecDumpCSV();
  ErrorCode ExecGetRow();
  ErrorCode ExecInsertRow();
  ErrorCode ExecUpdateRow();
  ErrorCode ExecDeleteRow();
  ErrorCode ExecInfo();
  ErrorCode ExecMeta();

  ErrorCode ExecDiff();
  ErrorCode ExecAppend();
  ErrorCode ExecInsert();
  ErrorCode ExecReplace();
  ErrorCode ExecUpdate();

  ErrorCode ExecDeleteListElements(const VMeta& meta);
  ErrorCode ExecInsertListElements(const VMeta& meta);
  ErrorCode ExecReplaceListElement(const VMeta& meta);

  ErrorCode ExecGetAll();
  ErrorCode ExecListKeyAll();
  ErrorCode ExecLatestAll();
  ErrorCode ExecGetColumnAll();

  ErrorCode ExecManipMeta(
    const std::function<ErrorCode(const VMeta&)>& f_output_meta);

  ErrorCode ExecPut(const std::string& cmd, const VObject& obj);
  ErrorCode ExecAppend(const VMeta& meta);

  ErrorCode ParseRowString(const std::string& row_str, Row* row);

  static const size_t kDefaultLimitPrintElems;
  static size_t limit_print_elems;

  ObjectDB odb_;
  ColumnStore cs_;
};

}  // namespace cli
}  // namespace ustore

#endif  // USTORE_CLI_COMMAND_H_
