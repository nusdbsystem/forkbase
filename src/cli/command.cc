// Copyright (c) 2017 The Ustore Authors.

#include "cli/command.h"

namespace ustore {
namespace cli {

const std::unordered_set<std::string> Command::kSupportedCommands = {
  "GET", "PUT", "MERGE", "BRANCH", "RENAME", "DELETE",
  "LIST_KEYS", "LIST_BRANCHES", "HEAD", "LATEST",
  "EXISTS", "IS_HEAD", "IS_LATEST",
  // alias
  "LIST_KEY", "LIST_BRANCH", "EXIST"
};

ErrorCode Command::ExecCommand(const std::string& cmd) {
  if (cmd == "GET") return ExecGet();
  if (cmd == "PUT") return ExecPut();
  if (cmd == "MERGE") return ExecMerge();
  if (cmd == "BRANCH") return ExecBranch();
  if (cmd == "RENAME") return ExecRename();
  if (cmd == "DELETE") return ExecDelete();
  if (cmd == "LIST_KEYS" || cmd == "LIST_KEY") return ExecListKey();
  if (cmd == "LIST_BRANCHES" || cmd == "LIST_BRANCH") return ExecListBranch();
  if (cmd == "HEAD") return ExecHead();
  if (cmd == "LATEST") return ExecLatest();
  if (cmd == "EXISTS" || cmd == "EXIST") return ExecExists();
  if (cmd == "IS_HEAD") return ExecIsHead();
  if (cmd == "IS_LATEST") return ExecIsLatest();
  std::cerr << "[FAILURE] Unknown command: " << cmd << std::endl;
  return ErrorCode::kUnknownCommand;
}

ErrorCode Command::ExecGet() {
  const auto& key = Config::key;
  const auto& branch = Config::branch;
  const auto& ver = Config::version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << "[INVALID ARGS: GET] Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\", "
              << "Version: \"" << ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const VString & val) {
    std::cout << "[SUCCESS: GET] Value: \"" << val << '\"' << std::endl;
  };
  const auto f_rpt_fail_by_branch = [&key, &branch](const ErrorCode & ec) {
    std::cerr << "[FAILED: GET] Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\" --> Error Code: " << ec
              << std::endl;
  };
  const auto f_rpt_fail_by_ver = [&key, &ver](const ErrorCode & ec) {
    std::cerr << "[FAILED: GET] Key: \"" << key << "\", "
              << "Version: \"" << ver << "\" --> Error Code: " << ec
              << std::endl;
  };
  // conditional execution
  if (key.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (!branch.empty() && ver.empty()) {
    auto rst = odb_.Get(Slice(key), Slice(branch));
    auto& ec = rst.stat;
    auto val = rst.value.String();
    ec == ErrorCode::kOK ? f_rpt_success(val) : f_rpt_fail_by_branch(ec);
    return ec;
  }
  if (branch.empty() && !ver.empty()) {
    auto rst = odb_.Get(Slice(key), Slice(ver));
    auto& ec = rst.stat;
    auto val = rst.value.String();
    ec == ErrorCode::kOK ? f_rpt_success(val) : f_rpt_fail_by_ver(ec);
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
    std::cerr << "[INVALID ARGS: PUT] Key: \"" << key << "\", "
              << "Value: \"" << val << "\", "
              << "Branch: \"" << branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const Hash & ver) {
    std::cout << "[SUCCESS: PUT] Version: \"" << ver << '\"' << std::endl;
  };
  const auto f_rpt_fail_by_branch = [&key, &branch](const ErrorCode & ec) {
    std::cerr << "[FAILED: PUT] Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\" --> Error Code: " << ec
              << std::endl;
  };
  const auto f_rpt_fail_by_ver = [&key, &ref_ver](const ErrorCode & ec) {
    std::cerr << "[FAILED: PUT] Key: \"" << key << "\", "
              << "Ref. Version: \"" << ref_ver << "\" --> Error Code: "
              << ec << std::endl;
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
  const auto& ref_ver2 = Config::ref_version2;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << "[INVALID ARGS: MERGE] Key: \"" << key << "\", "
              << "Value: \"" << val << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Ref. Branch: \"" << ref_branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\", "
              << "Ref. Version (2nd): \"" << ref_ver2 << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const Hash & ver) {
    std::cout << "[SUCCESS: MERGE] Version: \"" << ver << '\"' << std::endl;
  };
  const auto f_rpt_fail_by_branch = [&](const ErrorCode & ec) {
    std::cerr << "[FAILED: MERGE] Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Ref. Branch: \"" << ref_branch << "\" --> Error Code: "
              << ec << std::endl;
  };
  const auto f_rpt_fail_by_branch_ver = [&](const ErrorCode & ec) {
    std::cerr << "[FAILED: MERGE] Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\" --> Error Code: "
              << ec << std::endl;
  };
  const auto f_rpt_fail_by_ver = [&](const ErrorCode & ec) {
    std::cerr << "[FAILED: PUT] Key: \"" << key << "\", "
              << "Ref. Version: \"" << ref_ver << "\", "
              << "Ref. Version (2nd): \"" << ref_ver2 << "\" --> Error Code: "
              << ec << std::endl;
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
    std::cerr << "[INVALID ARGS: BRANCH] Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Referring Branch: \"" << ref_branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [&key, &tgt_branch]() {
    std::cout << "[SUCCESS: BRANCH] Branch \"" << tgt_branch
              << "\" has been created for Key \"" << key << "\""
              << std::endl;
  };
  const auto f_rpt_fail_by_branch = [&](const ErrorCode & ec) {
    std::cerr << "[FAILED: BRANCH] Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Referring Branch: \"" << ref_branch
              << "\" --> Error Code: " << ec << std::endl;
  };
  const auto f_rpt_fail_by_ver = [&](const ErrorCode & ec) {
    std::cerr << "[FAILED: BRANCH] Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Ref. Version: \"" << ref_ver
              << "\" --> Error Code: " << ec << std::endl;
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
};

ErrorCode Command::ExecRename() {
  const auto& key = Config::key;
  const auto& old_branch = Config::ref_branch;
  const auto& new_branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << "[INVALID ARGS: RENAME] Key: \"" << key << "\", "
              << "Referring Branch: \"" << old_branch << "\", "
              << "Target Branch: \"" << new_branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << "[SUCCESS: RENAME] Branch \"" << old_branch
              << "\" has been renamed to \"" << new_branch
              << "\" for Key \"" << key << "\"" << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << "[FAILED: RENAME] Key: \"" << key << "\", "
              << "Referring Branch: \"" << old_branch << "\", "
              << "Target Branch: \"" << new_branch
              << "\" --> Error Code: " << ec << std::endl;
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
    std::cerr << "[INVALID ARGS: DELETE] Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&key, &branch]() {
    std::cout << "[SUCCESS: DELETE] Branch \"" << branch
              << "\" has been deleted for Key \"" << key << "\""
              << std::endl;
  };
  const auto f_rpt_fail = [&key, &branch](const ErrorCode & ec) {
    std::cerr << "[FAILED: DELETE] Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\" --> Error Code: " << ec
              << std::endl;
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
    std::cout << "[SUCCESS: LIST_KEYS] Keys: "
              << Utils::ToStringWithQuote(keys) << std::endl;
  };
  const auto f_rpt_fail = [](const ErrorCode & ec) {
    std::cerr << "[FAILED: LIST_KEYS] --> ErrorCode: " << ec << std::endl;
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
    std::cerr << "[INVALID ARGS: LIST_BRANCHES] Key: \"" << key << "\""
              << std::endl;
  };
  const auto f_rpt_success = [](const std::vector<std::string>& branches) {
    std::cout << "[SUCCESS: LIST_BRANCHES] Branches: "
              << Utils::ToStringWithQuote(branches) << std::endl;
  };
  const auto f_rpt_fail = [&key](const ErrorCode & ec) {
    std::cerr << "[FAILED: LIST_BRANCHES] Key: \"" << key
              << "\" --> Error Code: " << ec << std::endl;
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
    std::cerr << "[INVALID ARGS: HEAD] Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const Hash & ver) {
    std::cout << "[SUCCESS: HEAD] Version: \"" << ver << '\"' << std::endl;
  };
  const auto f_rpt_fail = [&key, &branch](const ErrorCode & ec) {
    std::cerr << "[FAILED: HEAD] Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\" --> Error Code: " << ec
              << std::endl;
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
    std::cerr << "[INVALID ARGS: LATEST] Key: \"" << key << "\""
              << std::endl;
  };
  const auto f_rpt_success = [](const std::vector<Hash>& vers) {
    std::cout << "[SUCCESS: LATEST] Versions: "
              << Utils::ToStringWithQuote(vers) << std::endl;
  };
  const auto f_rpt_fail = [&key](const ErrorCode & ec) {
    std::cerr << "[FAILED: LATEST] Key: \"" << key
              << "\" --> Error Code: " << ec << std::endl;
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
    std::cerr << "[INVALID ARGS: EXISTS] Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success_by_key = [](const bool exist) {
    std::cout << "[SUCCESS: EXISTS KEY] " << (exist ? "True" : "False")
              << std::endl;
  };
  const auto f_rpt_success_by_branch = [](const bool exist) {
    std::cout << "[SUCCESS: EXISTS BRANCH] " << (exist ? "True" : "False")
              << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << "[FAILED: EXISTS] Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\" --> Error Code: " << ec
              << std::endl;
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
    std::cerr << "[INVALID ARGS: IS_HEAD] Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\", "
              << "Version: \"" << ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const bool is_head) {
    std::cout << "[SUCCESS: IS_HEAD] " << (is_head ? "True" : "False")
              << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << "[FAILED: IS_HEAD] Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\", "
              << "Version: \"" << ver << "\" --> Error Code: " << ec
              << std::endl;
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
    std::cerr << "[INVALID ARGS: IS_LATEST] Key: \"" << key << "\", "
              << "Version: \"" << ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const bool is_latest) {
    std::cout << "[SUCCESS: IS_LATEST] " << (is_latest ? "True" : "False")
              << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << "[FAILED: IS_LATEST] Key: \"" << key << "\", "
              << "Version: \"" << ver << "\" --> Error Code: " << ec
              << std::endl;
  };
  // conditional execution
  if (key.empty() || ver.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto rst =
    odb_.IsLatestVersion(Slice(key), Hash::FromBase32(ver));
  auto& ec = rst.stat;
  auto& is_latest = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(is_latest) : f_rpt_fail(ec);
  return ec;
}

}  // namespace cli
}  // namespace ustore
