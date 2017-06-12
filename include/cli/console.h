// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLI_CONSOLE_H_
#define USTORE_CLI_CONSOLE_H_

#include <list>
#include "cli/command.h"

namespace ustore {
namespace cli {

class Console : public Command {
 public:
  explicit Console(DB* db) noexcept;
  ~Console() = default;

  ErrorCode Run(int argc, char* argv[]) = delete;
  ErrorCode Run();

 protected:
  void PrintConsoleCommandHelp(std::ostream& os = std::cout);

 private:
  ErrorCode ExecHistory();
  ErrorCode ExecDumpHistory();

  std::list<std::string> history_;
};

}  // namespace cli
}  // namespace ustore

#endif  // USTORE_CLI_CONSOLE_H_
