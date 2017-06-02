// Copyright (c) 2017 The Ustore Authors.

#include <iomanip>
#include "cli/command.h"

namespace ustore {
namespace cli {

#define CMD_HANDLER(cmd, handler) do { \
  cmd_exec_[cmd] = [this]() { return handler(); }; \
} while(0)

#define CMD_ALIAS(alias, cmd) do { \
  alias_exec_[alias] = &cmd_exec_[cmd]; \
} while(0)

Command::Command(DB* db) noexcept
  : odb_(db), cs_(db) {
  // basic commands
  CMD_HANDLER("GET", ExecGet);
  CMD_HANDLER("PUT", ExecPut);
  CMD_HANDLER("MERGE", ExecMerge);
  CMD_HANDLER("BRANCH", ExecBranch);
  CMD_HANDLER("RENAME", ExecRename);
  CMD_HANDLER("DELETE", ExecDelete);
  CMD_HANDLER("LIST_KEY", ExecListKey);
  CMD_ALIAS("LIST_KEYS", "LIST_KEY");
  CMD_ALIAS("LIST-KEY", "LIST_KEY");
  CMD_ALIAS("LIST-KEYS", "LIST_KEY");
  CMD_ALIAS("LISTKEY", "LIST_KEY");
  CMD_ALIAS("LISTKEYS", "LIST_KEY");
  CMD_ALIAS("LSKEY", "LIST_KEY");
  CMD_ALIAS("LS_KEY", "LIST_KEY");
  CMD_ALIAS("LS-KEY", "LIST_KEY");
  CMD_ALIAS("LSK", "LIST_KEY");
  CMD_HANDLER("LIST_BRANCH", ExecListBranch);
  CMD_ALIAS("LIST_BRANCHES", "LIST_BRANCH");
  CMD_ALIAS("LIST-BRANCH", "LIST_BRANCH");
  CMD_ALIAS("LIST-BRANCHES", "LIST_BRANCH");
  CMD_ALIAS("LISTBRANCHES", "LIST_BRANCH");
  CMD_ALIAS("LISTBRANCH", "LIST_BRANCH");
  CMD_ALIAS("LSBRANCH", "LIST_BRANCH");
  CMD_ALIAS("LS_BRANCH", "LIST_BRANCH");
  CMD_ALIAS("LS-BRANCH", "LIST_BRANCH");
  CMD_ALIAS("LSB", "LIST_BRANCH");
  CMD_HANDLER("HEAD", ExecHead);
  CMD_HANDLER("LATEST", ExecLatest);
  CMD_HANDLER("EXISTS", ExecExists);
  CMD_ALIAS("EXIST", "EXISTS");
  CMD_HANDLER("IS_HEAD", ExecIsHead);
  CMD_ALIAS("ISHEAD", "IS_HEAD");
  CMD_ALIAS("ISH", "IS_HEAD");
  CMD_HANDLER("IS_LATEST", ExecIsLatest);
  CMD_ALIAS("ISLATEST", "IS_LATEST");
  CMD_ALIAS("ISL", "IS_LATEST");
  // relational commands
  CMD_HANDLER("CREATE_TABLE", ExecCreateTable);
  CMD_ALIAS("CREATE-TABLE", "CREATE_TABLE");
  CMD_ALIAS("CREATETABLE", "CREATE_TABLE");
  CMD_HANDLER("BRANCH_TABLE", ExecBranchTable);
  CMD_ALIAS("BRANCH-TABLE", "BRANCH_TABLE");
  CMD_ALIAS("BRANCHTABLE", "BRANCH_TABLE");
  CMD_HANDLER("GET_COLUMN", ExecGetColumn);
  CMD_ALIAS("GET-COLUMN", "GET_COLUMN");
  CMD_ALIAS("GET_COL", "GET_COLUMN");
  CMD_ALIAS("GET-COL", "GET_COLUMN");
  CMD_ALIAS("GETCOLUMN", "GET_COLUMN");
  CMD_ALIAS("GETCOL", "GET_COLUMN");
  CMD_HANDLER("DELETE_COLUMN", ExecDeleteColumn);
  CMD_ALIAS("DELETE-COLUMN", "DELETE_COLUMN");
  CMD_ALIAS("DELETECOLUMN", "DELETE_COLUMN");
  CMD_ALIAS("DELETE_COL", "DELETE_COLUMN");
  CMD_ALIAS("DELETE-COL", "DELETE_COLUMN");
  CMD_ALIAS("DELETECOL", "DELETE_COLUMN");
  CMD_ALIAS("DEL_COLUMN", "DELETE_COLUMN");
  CMD_ALIAS("DEL-COLUMN", "DELETE_COLUMN");
  CMD_ALIAS("DELCOLUMN", "DELETE_COLUMN");
  CMD_ALIAS("DEL_COL", "DELETE_COLUMN");
  CMD_ALIAS("DEL-COL", "DELETE_COLUMN");
  CMD_ALIAS("DELCOL", "DELETE_COLUMN");
}

const int kPrintBasicCmdWidth = 12;
const int kPrintRelationalCmdWidth = 15;

#define FORMAT_BASIC_CMD(cmd) \
  "* " << std::left << std::setw(kPrintBasicCmdWidth) << cmd << " "

#define FORMAT_RELATIONAL_CMD(cmd) \
  "* " << std::left << std::setw(kPrintRelationalCmdWidth) << cmd << " "

void Command::PrintCommandHelp() {
  std::cout << "UStore Basic Commands:" << std::endl;
  std::cout << FORMAT_BASIC_CMD("GET")
            << "-k <key> [-b <branch> | "
            << std::endl << std::setw(kPrintBasicCmdWidth + 13) << ""
            << "-v <version>]" << std::endl;
  std::cout << FORMAT_BASIC_CMD("PUT")
            << "-k <key> -x <value> {-b <branch> | "
            << std::endl << std::setw(kPrintBasicCmdWidth + 24) << ""
            << "-u <refer_version>}" << std::endl;
  std::cout << FORMAT_BASIC_CMD("BRANCH")
            << "-k <key> -b <new_branch> [-c <base_branch> | "
            << std::endl << std::setw(kPrintBasicCmdWidth + 29) << ""
            << "-u <refer_version>]"
            << std::endl;
  std::cout << FORMAT_BASIC_CMD("MERGE")
            << "-k <key> -x <value> [-b <target_branch> -c <refer_branch> | "
            << std::endl << std::setw(kPrintBasicCmdWidth + 24) << ""
            << "-b <target_branch> -u <refer_version> | "
            << std::endl << std::setw(kPrintBasicCmdWidth + 24) << ""
            << "-u <refer_version> -v <refer_version_2>]"
            << std::endl;
  std::cout << FORMAT_BASIC_CMD("RENAME")
            << "-k <key> "
            << "-c <from_branch> -b <to_branch>" << std::endl;
  std::cout << FORMAT_BASIC_CMD("DELETE")
            << "-k <key> -b <branch>" << std::endl;
  std::cout << FORMAT_BASIC_CMD("HEAD")
            << "-k <key> -b <branch>" << std::endl;
  std::cout << FORMAT_BASIC_CMD("LATEST")
            << "-k <key>" << std::endl;
  std::cout << FORMAT_BASIC_CMD("EXISTS")
            << "-k <key> {-b <branch>}" << std::endl;
  std::cout << FORMAT_BASIC_CMD("IS_HEAD")
            << "-k <key> -b <branch> -v <version>" << std::endl;
  std::cout << FORMAT_BASIC_CMD("IS_LATEST")
            << "-k <key> -v <version>" << std::endl;
  std::cout << FORMAT_BASIC_CMD("LIST_BRANCH")
            << "-k <key>" << std::endl;
  std::cout << FORMAT_BASIC_CMD("LIST_KEY") << std::endl;
  std::cout << std::endl
            << "UStore Relational Commands:" << std::endl;
  std::cout << FORMAT_RELATIONAL_CMD("CREATE_TABLE")
            << "-t <table> -b <branch>" << std::endl;
  std::cout << FORMAT_RELATIONAL_CMD("BRANCH_TABLE")
            << "-t <table> -b <target_branch> -c <refer_branch>" << std::endl;
  std::cout << FORMAT_RELATIONAL_CMD("GET_COLUMN")
            << "-t <table> -b <branch> -a <column>" << std::endl;
  std::cout << FORMAT_RELATIONAL_CMD("DELETE_COLUMN")
            << "-t <table> -b <branch> -a <column>" << std::endl;
}

ErrorCode Command::ExecCommand(const std::string& cmd) {
  auto it_cmd_exec = cmd_exec_.find(cmd);
  if (it_cmd_exec == cmd_exec_.end()) {
    auto it_alias_exec = alias_exec_.find(cmd);
    if (it_alias_exec == alias_exec_.end()) {
      std::cerr << BOLD_RED("[FAILURE] ")
                << "Unknown command: " << cmd << std::endl;
      return ErrorCode::kUnknownCommand;
    }
    return (*(it_alias_exec->second))();
  }
  return it_cmd_exec->second();
}

ErrorCode Command::ExecGet() {
  const auto& key = Config::key;
  const auto& branch = Config::branch;
  const auto& ver = Config::version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: GET] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\", "
              << "Version: \"" << ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const VMeta & meta) {
    auto type = meta.cell().type();
    std::cout << BOLD_GREEN("[SUCCESS: GET] ") << "Value"
              << "<" << Utils::ToString(type) << ">: ";
    switch (type) {
      case UType::kString:
      case UType::kBlob:
        std::cout << "\"" << meta << "\"";
        break;
      case UType::kList:
        Utils::PrintList(meta.List(), true);
        break;
      case UType::kMap:
        Utils::PrintMap(meta.Map(), true);
        break;
      default:
        std::cout << meta;
        break;
    }
    std::cout << std::endl;
  };
  const auto f_rpt_fail_by_branch = [&key, &branch](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: GET] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  const auto f_rpt_fail_by_ver = [&key, &ver](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: GET] ")
              << "Key: \"" << key << "\", "
              << "Version: \"" << ver << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (key.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (!branch.empty() && ver.empty()) {
    auto rst = odb_.Get(Slice(key), Slice(branch));
    auto& ec = rst.stat;
    auto& meta = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(meta) : f_rpt_fail_by_branch(ec);
    return ec;
  }
  if (branch.empty() && !ver.empty()) {
    auto rst = odb_.Get(Slice(key), Hash::FromBase32(ver));
    auto& ec = rst.stat;
    auto& meta = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(meta) : f_rpt_fail_by_ver(ec);
    return ec;
  }
  // illegal: branch and version are neither set or both set
  f_rpt_invalid_args();
  return ErrorCode::kInvalidCommandArgument;
}

ErrorCode Command::ExecPut() {
  const auto& key = Config::key;
  const auto& val = Config::value;
  const auto& branch = Config::branch;
  const auto& ref_ver = Config::ref_version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: PUT] ")
              << "Key: \"" << key << "\", "
              << "Value: \"" << val << "\", "
              << "Branch: \"" << branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const Hash & ver) {
    std::cout << BOLD_GREEN("[SUCCESS: PUT] ")
              << "Version: \"" << ver << '\"' << std::endl;
  };
  const auto f_rpt_fail_by_branch = [&key, &branch](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: PUT] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  const auto f_rpt_fail_by_ver = [&key, &ref_ver](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: PUT] ")
              << "Key: \"" << key << "\", "
              << "Ref. Version: \"" << ref_ver << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (key.empty() || val.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (!branch.empty() && ref_ver.empty()) {
    auto rst = odb_.Put(Slice(key), VString(Slice(val)), Slice(branch));
    auto& ec = rst.stat;
    auto& ver = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail_by_branch(ec);
    return ec;
  }
  if (branch.empty() && !ref_ver.empty()) {
    auto rst =
      odb_.Put(Slice(key), VString(Slice(val)), Hash::FromBase32(ref_ver));
    auto& ec = rst.stat;
    auto& ver = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail_by_ver(ec);
    return ec;
  }
  if (branch.empty() && ref_ver.empty()) {
    auto rst = odb_.Put(Slice(key), VString(Slice(val)), Hash::kNull);
    auto& ec = rst.stat;
    auto& ver = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail_by_ver(ec);
    return ec;
  }
  // illegal: branch and reffering version are neither set or both set
  f_rpt_invalid_args();
  return ErrorCode::kInvalidCommandArgument;
}

ErrorCode Command::ExecMerge() {
  const auto& key = Config::key;
  const auto& val = Config::value;
  const auto& tgt_branch = Config::branch;
  const auto& ref_branch = Config::ref_branch;
  const auto& ref_ver = Config::ref_version;
  const auto& ref_ver2 = Config::version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: MERGE] ")
              << "Key: \"" << key << "\", "
              << "Value: \"" << val << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Ref. Branch: \"" << ref_branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\", "
              << "Ref. Version (2nd): \"" << ref_ver2 << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const Hash & ver) {
    std::cout << BOLD_GREEN("[SUCCESS: MERGE] ")
              << "Version: \"" << ver << '\"' << std::endl;
  };
  const auto f_rpt_fail_by_branch = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: MERGE] ")
              << "Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Ref. Branch: \"" << ref_branch << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  const auto f_rpt_fail_by_branch_ver = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: MERGE] ")
              << "Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  const auto f_rpt_fail_by_ver = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: MERGE] ")
              << "Key: \"" << key << "\", "
              << "Ref. Version: \"" << ref_ver << "\", "
              << "Ref. Version (2nd): \"" << ref_ver2 << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (key.empty() || val.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (!tgt_branch.empty() && !ref_branch.empty() && ref_ver.empty()) {
    auto rst = odb_.Merge(Slice(key), VString(Slice(val)), Slice(tgt_branch),
                          Slice(ref_branch));
    auto& ec = rst.stat;
    auto& ver = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail_by_branch(ec);
    return ec;
  }
  if (!tgt_branch.empty() && ref_branch.empty() && !ref_ver.empty()) {
    auto rst = odb_.Merge(Slice(key), VString(Slice(val)), Slice(tgt_branch),
                          Hash::FromBase32(ref_ver));
    auto& ec = rst.stat;
    auto& ver = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail_by_branch_ver(ec);
    return ec;
  }
  if (tgt_branch.empty() && !ref_ver.empty() && !ref_ver2.empty()) {
    auto rst = odb_.Merge(Slice(key), VString(Slice(val)),
                          Hash::FromBase32(ref_ver),
                          Hash::FromBase32(ref_ver2));
    auto& ec = rst.stat;
    auto& ver = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail_by_ver(ec);
    return ec;
  }
  // illegal settings of arguments
  f_rpt_invalid_args();
  return ErrorCode::kInvalidCommandArgument;
}

ErrorCode Command::ExecBranch() {
  const auto& key = Config::key;
  const auto& tgt_branch = Config::branch;
  const auto& ref_branch = Config::ref_branch;
  const auto& ref_ver = Config::ref_version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: BRANCH] ")
              << "Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Referring Branch: \"" << ref_branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [&key, &tgt_branch]() {
    std::cout << BOLD_GREEN("[SUCCESS: BRANCH] ")
              << "Branch \"" << tgt_branch
              << "\" has been created for Key \"" << key << "\""
              << std::endl;
  };
  const auto f_rpt_fail_by_branch = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: BRANCH] ")
              << "Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Referring Branch: \"" << ref_branch << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  const auto f_rpt_fail_by_ver = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: BRANCH] ")
              << "Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (key.empty() || tgt_branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (!ref_branch.empty() && ref_ver.empty()) {
    auto ec = odb_.Branch(Slice(key), Slice(ref_branch), Slice(tgt_branch));
    ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail_by_branch(ec);
    return ec;
  }
  if (ref_branch.empty() && !ref_ver.empty()) {
    auto ec = odb_.Branch(Slice(key), Hash::FromBase32(ref_ver),
                          Slice(tgt_branch));
    ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail_by_ver(ec);
    return ec;
  }
  // illegal: ref_branch and ref_ver are neither set or both set
  f_rpt_invalid_args();
  return ErrorCode::kInvalidCommandArgument;
}

ErrorCode Command::ExecRename() {
  const auto& key = Config::key;
  const auto& old_branch = Config::ref_branch;
  const auto& new_branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: RENAME] ")
              << "Key: \"" << key << "\", "
              << "Referring Branch: \"" << old_branch << "\", "
              << "Target Branch: \"" << new_branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << BOLD_GREEN("[SUCCESS: RENAME] ")
              << "Branch \"" << old_branch
              << "\" has been renamed to \"" << new_branch
              << "\" for Key \"" << key << "\"" << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: RENAME] ")
              << "Key: \"" << key << "\", "
              << "Referring Branch: \"" << old_branch << "\", "
              << "Target Branch: \"" << new_branch << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (key.empty() || old_branch.empty() || new_branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = odb_.Rename(Slice(key), Slice(old_branch), Slice(new_branch));
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecDelete() {
  const auto& key = Config::key;
  const auto& branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: DELETE] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&key, &branch]() {
    std::cout << BOLD_GREEN("[SUCCESS: DELETE] ")
              << "Branch \"" << branch
              << "\" has been deleted for Key \"" << key << "\"" << std::endl;
  };
  const auto f_rpt_fail = [&key, &branch](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: DELETE] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (key.empty() || branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = odb_.Delete(Slice(key), Slice(branch));
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecListKey() {
  // screen printing
  const auto f_rpt_success = [](const std::vector<std::string>& keys) {
    std::cout << BOLD_GREEN("[SUCCESS: LIST_KEY] ")
              << "Keys: " << Utils::ToStringWithQuote(keys) << std::endl;
  };
  const auto f_rpt_fail = [](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: LIST_KEY] ")
              << "--> ErrorCode: " << ec << std::endl;
  };
  auto rst = odb_.ListKeys();
  auto& ec = rst.stat;
  auto& keys = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(keys) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecListBranch() {
  const auto& key = Config::key;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: LIST_BRANCH] ")
              << "Key: \"" << key << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const std::vector<std::string>& branches) {
    std::cout << BOLD_GREEN("[SUCCESS: LIST_BRANCH] ")
              << "Branches: " << Utils::ToStringWithQuote(branches)
              << std::endl;
  };
  const auto f_rpt_fail = [&key](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: LIST_BRANCH] ")
              << "Key: \"" << key << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (key.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto rst = odb_.ListBranches(Slice(key));
  auto& ec = rst.stat;
  auto& branches = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(branches) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecHead() {
  const auto& key = Config::key;
  const auto& branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: HEAD] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const Hash & ver) {
    std::cout << BOLD_GREEN("[SUCCESS: HEAD] ")
              << "Version: \"" << ver << '\"' << std::endl;
  };
  const auto f_rpt_fail = [&key, &branch](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: HEAD] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (key.empty() || branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto rst = odb_.GetBranchHead(Slice(key), Slice(branch));
  auto& ec = rst.stat;
  auto& ver = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecLatest() {
  const auto& key = Config::key;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: LATEST] ")
              << "Key: \"" << key << "\""
              << std::endl;
  };
  const auto f_rpt_success = [](const std::vector<Hash>& vers) {
    std::cout << BOLD_GREEN("[SUCCESS: LATEST] Versions: ")
              << Utils::ToStringWithQuote(vers) << std::endl;
  };
  const auto f_rpt_fail = [&key](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: LATEST] ")
              << "Key: \"" << key << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (key.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto rst = odb_.GetLatestVersions(Slice(key));
  auto& ec = rst.stat;
  auto& vers = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(vers) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecExists() {
  const auto& key = Config::key;
  const auto& branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: EXISTS] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success_by_key = [](const bool exist) {
    std::cout << BOLD_GREEN("[SUCCESS: EXISTS KEY] ")
              << (exist ? "True" : "False") << std::endl;
  };
  const auto f_rpt_success_by_branch = [](const bool exist) {
    std::cout << BOLD_GREEN("[SUCCESS: EXISTS BRANCH] ")
              << (exist ? "True" : "False") << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: EXISTS] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (key.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (branch.empty()) {
    auto rst = odb_.Exists(Slice(key));
    auto& ec = rst.stat;
    auto exist = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success_by_key(exist) : f_rpt_fail(ec);
    return ec;
  } else {
    auto rst = odb_.Exists(Slice(key), Slice(branch));
    auto& ec = rst.stat;
    auto exist = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success_by_branch(exist) : f_rpt_fail(ec);
    return ec;
  }
}

ErrorCode Command::ExecIsHead() {
  const auto& key = Config::key;
  const auto& branch = Config::branch;
  const auto& ver = Config::version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: IS_HEAD] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\", "
              << "Version: \"" << ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const bool is_head) {
    std::cout << BOLD_GREEN("[SUCCESS: IS_HEAD] ")
              << (is_head ? "True" : "False") << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: IS_HEAD] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\", "
              << "Version: \"" << ver << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (key.empty() || branch.empty() || ver.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto rst =
    odb_.IsBranchHead(Slice(key), Slice(branch), Hash::FromBase32(ver));
  auto& ec = rst.stat;
  auto& is_head = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(is_head) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecIsLatest() {
  const auto& key = Config::key;
  const auto& ver = Config::version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: IS_LATEST] ")
              << "Key: \"" << key << "\", "
              << "Version: \"" << ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const bool is_latest) {
    std::cout << BOLD_GREEN("[SUCCESS: IS_LATEST] ")
              << (is_latest ? "True" : "False") << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: IS_LATEST] ")
              << "Key: \"" << key << "\", "
              << "Version: \"" << ver << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (key.empty() || ver.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto rst = odb_.IsLatestVersion(Slice(key), Hash::FromBase32(ver));
  auto& ec = rst.stat;
  auto& is_latest = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(is_latest) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecCreateTable() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: CREATE_TABLE] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << BOLD_GREEN("[SUCCESS: CREATE_TABLE] ")
              << "Table \"" << tab << "\" has been created for Branch \""
              << branch << "\"" << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: CREATE_TABLE] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (tab.empty() || branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = cs_.CreateTable(tab, branch);
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecBranchTable() {
  const auto& tab = Config::table;
  const auto& tgt_branch = Config::branch;
  const auto& ref_branch = Config::ref_branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: BRANCH_TABLE] ")
              << "Table: \"" << tab << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Referring Branch: \"" << ref_branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << BOLD_GREEN("[SUCCESS: BRANCH_TABLE] ")
              << "Branch \"" << tgt_branch
              << "\" has been created for Table \"" << tab << "\""
              << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: BRANCH_TABLE] ")
              << "Table: \"" << tab << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Referring Branch: \"" << ref_branch << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (tab.empty() || tgt_branch.empty() || ref_branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = cs_.BranchTable(tab, ref_branch, tgt_branch);
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecGetColumn() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  const auto& col_name = Config::column;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: GET_COLUMN] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Column: \"" << col_name << "\"" << std::endl;
  };
  const auto f_rpt_success = [&](const Column & col) {
    std::cout << BOLD_GREEN("[SUCCESS: GET_COLUMN] ") << "Column: ";
    Utils::PrintList(col, true);
    std::cout << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: GET_COLUMN] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Column: \"" << col_name << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (tab.empty() || branch.empty() || col_name.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  Column col;
  auto ec = cs_.GetColumn(tab, branch, col_name, &col);
  ec == ErrorCode::kOK ? f_rpt_success(col) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecDeleteColumn() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  const auto& col_name = Config::column;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: DELETE_COLUMN] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Column: \"" << col_name << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << BOLD_GREEN("[SUCCESS: DELETE_COLUMN] ")
              << "Column \"" << col_name
              << "\" has been deleted in Table \"" << tab
              << "\" of Branch \"" << branch << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: DELETE_COLUMN] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Column: \"" << col_name << "\""
              << RED(" --> Error Code: " << ec) << std::endl;
  };
  // conditional execution
  if (tab.empty() || branch.empty() || col_name.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = cs_.DeleteColumn(tab, branch, col_name);
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

}  // namespace cli
}  // namespace ustore
