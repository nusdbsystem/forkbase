// Copyright (c) 2017 The Ustore Authors.

#include "config.h"
#include "table_op.h"

namespace ustore {
namespace example {
namespace table_op {

TableOp::TableOp(DB* db) noexcept : cs_(db) {}

#define TIME_COMPLETE(name, func) \
  TimeComplete(name, [this] { return func; })

ErrorCode TableOp::TimeComplete(const std::string& name,
                                const std::function<ErrorCode()>& f_exec) {
  auto ec = ErrorCode::kUnknownOp;
  auto elapsed_ms = Timer::TimeMilliseconds([&ec, &f_exec] { ec = f_exec(); });
  if (ec == ErrorCode::kOK) {
    std::cout << BOLD_GREEN("[SUCCESS: " << name << "]");
  } else {
    std::cout << BOLD_RED("[FAILED: " << name << "]");
  }
  std::cout << " " << static_cast<long>(elapsed_ms) << " ms" << std::endl;
  return ec;
}

ErrorCode TableOp::Run(int argc, char* argv[]) {
  if (!Config::ParseCmdArgs(argc, argv)) {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Found invalid command-line option" << std::endl;
    return ErrorCode::kInvalidCommandArgument;
  }
  if (Config::is_help) return ErrorCode::kOK;
  // execution
  USTORE_GUARD(Init());
  USTORE_GUARD(Load());
  return ErrorCode::kOK;
}

const std::string table = "Test";
const std::string branch = "master";

ErrorCode TableOp::Init() {
  bool exists;
  USTORE_GUARD(cs_.ExistsTable(table, branch, &exists));
  if (exists) USTORE_GUARD(cs_.DeleteTable(table, branch));
  return cs_.CreateTable(table, branch);
}

ErrorCode TableOp::Load() {
  return TIME_COMPLETE("Load", cs_.LoadCSV(Config::file, table, branch));
}

}  // namespace table_op
}  // namespace example
}  // namespace ustore
