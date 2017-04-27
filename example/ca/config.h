// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_CONFIG_H_
#define USTORE_EXAMPLE_CA_CONFIG_H_

#include <boost/program_options.hpp>
#include <iostream>
#include <list>
#include <map>
#include <sstream>
#include <string>
#include "worker/worker.h"

#include "utils.h"

namespace ustore {
namespace example {
namespace ca {

#define MAP_TYPE std::map
#define LIST_TYPE std::list

using KeyType = std::string;
using DataType = std::string;
using ColumnType = LIST_TYPE<DataType>;
using TableType = MAP_TYPE<KeyType, ColumnType>;

namespace po = boost::program_options;

class Config {
 public:
  static const WorkerID kWorkID;
  static const size_t kDefaultNumColumns;
  static const size_t kDefaultNumRecords;
  static const size_t kDefaultNumIterations;
  static const double kDefaultProbability;

  static bool is_help;
  static size_t n_columns;
  static size_t n_records;
  static double p;
  static size_t iters;

  static bool ParseCmdArgs(const int& argc, char* argv[]);

 private:
  static bool ParseCmdArgs(const int& argc, char* argv[],
                           po::variables_map& vm);

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
    std::cout << "[ARG] " << title << ": " << var << std::endl;
  } else {
    std::cerr << "[ERROR ARG] " << title << ": " << "[Actual] " << var
              << ", [Expected] " << expect << std::endl;
  }
  return expr;
}

} // namespace ca
} // namespace example
} // namespace ustore

#endif // USTORE_EXAMPLE_CA_CONFIG_H_
