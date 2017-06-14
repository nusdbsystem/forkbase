// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLI_CONFIG_H_
#define USTORE_CLI_CONFIG_H_

#include <boost/program_options.hpp>
#include <iostream>
#include <string>
#include <vector>
#include "utils/logging.h"
#include "cli/utils.h"

namespace ustore {
namespace cli {

namespace po = boost::program_options;

class Config {
 public:
  static bool is_help;
  static std::string command_options_help;
  static std::string command;
  static std::string script;
  static std::string file;
  static std::string key;
  static std::string value;
  static std::string branch;
  static std::string ref_branch;
  static std::string version;
  static std::string ref_version;
  static std::string table;
  static std::string ref_table;
  static std::string column;
  static std::string ref_column;

  static bool ParseCmdArgs(int argc, char* argv[]);

  static bool ParseCmdArgs(const std::vector<std::string>& args);

 private:
  static void Reset();

  static bool ParseCmdArgs(int argc, char* argv[], po::variables_map* vm);

  template<typename T>
  static bool CheckArg(const T& var, const bool expr,
                       const std::string& title, const std::string& expect);

  template<typename T1, typename T2>
  static inline bool CheckArgEQ(const T1& var, const T2& expect,
                                const std::string& title) {
    return CheckArg(var, var == expect, title, "=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgNE(const T1& var, const T2& expect,
                                const std::string& title) {
    return CheckArg(var, var != expect, title, "!=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgLE(const T1& var, const T2& expect,
                                const std::string& title) {
    return CheckArg(var, var <= expect, title, "<=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgLT(const T1& var, const T2& expect,
                                const std::string& title) {
    return CheckArg(var, var < expect, title, "<" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgGE(const T1& var, const T2& expect,
                                const std::string& title) {
    return CheckArg(var, var >= expect, title, ">=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgGT(const T1& var, const T2& expect,
                                const std::string& title) {
    return CheckArg(var, var > expect, title, ">" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgInRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return CheckArg(var, lbound <= var && var <= ubound, title, "range of" +
                    Utils::ToStringPair(lbound, ubound, "[", "]", ","));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgInLeftOpenRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return CheckArg(var, lbound < var && var <= ubound, title, "range of" +
                    Utils::ToStringPair(lbound, ubound, "(", "]", ","));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgInRightOpenInRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return CheckArg(var, lbound <= var && var < ubound, title, "range of" +
                    Utils::ToStringPair(lbound, ubound, "[", ")", ","));
  }

  template<typename T1, typename T2>
  static inline bool CheckArgInOpenRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    const std::string expect = "range of (" + Utils::ToString(lbound) +
                               "," + Utils::ToString(ubound) + ")";
    return CheckArg(var, lbound < var && var < ubound, title, "range of" +
                    Utils::ToStringPair(lbound, ubound, "(", ")", ","));
  }
};

template<typename T>
bool Config::CheckArg(const T& var, const bool expr,
                      const std::string& title, const std::string& expect) {
  if (expr) {
    LOG(INFO) << "[ARG] " << title << ": " << var;
  } else {
    std::cerr << BOLD_RED("[ERROR ARG] ") << title << ": "
              << "[Actual] " << var << ", [Expected] " << expect << std::endl;
  }
  return expr;
}

}  // namespace cli
}  // namespace ustore

#endif  // USTORE_CLI_CONFIG_H_
