// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_BENCHMARK_BENCHMARK_CONFIG_H_
#define USTORE_BENCHMARK_BENCHMARK_CONFIG_H_

#include <boost/program_options.hpp>
#include <iostream>
#include <string>
#include "utils/logging.h"
#include "utils/utils.h"

namespace ustore {

namespace po = boost::program_options;

class BenchmarkConfig {
 public:
  static bool is_help;
  static int num_clients;
  static int num_validations;
  static int num_strings;
  static int num_blobs;
  static int string_len;
  static int blob_size;

  static bool ParseCmdArgs(int argc, char* argv[]);

 private:
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
bool BenchmarkConfig::CheckArg(const T& var, const bool expr,
                               const std::string& title,
                               const std::string& expect) {
  if (expr) {
    std::cout << "[ARG] " << title << ": " << var << std::endl;
  } else {
    std::cerr << BOLD_RED("[ERROR ARG] ") << title << ": "
              << "[Actual] " << var << ", [Expected] " << expect << std::endl;
  }
  return expr;
}

}  // namespace ustore

#endif  // USTORE_BENCHMARK_BENCHMARK_CONFIG_H_
