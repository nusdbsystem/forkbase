// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLI_COMMAND_H_
#define USTORE_CLI_COMMAND_H_

#include <boost/algorithm/string.hpp>
#include <string>
#include <unordered_set>
#include "spec/object_db.h"
#include "cli/config.h"

namespace ustore {
namespace cli {

class Command {
 public:
  static inline void Normalize(std::string& cmd) {
    boost::algorithm::to_upper(cmd);
  }

  static inline bool IsValid(const std::string& cmd) {
    return kSupportedCommands.find(cmd) != kSupportedCommands.end();
  }

  explicit Command(DB* db) noexcept : odb_(db) {}
  ~Command() = default;

  ErrorCode ExecCommand(const std::string& command);

 private:
  static const std::unordered_set<std::string> kSupportedCommands;

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

  ObjectDB odb_;
};

}  // namespace cli
}  // namespace ustore

#endif  // USTORE_CLI_COMMAND_H_
