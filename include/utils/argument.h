// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_ARGUMENT_H_
#define USTORE_UTILS_ARGUMENT_H_

#include <boost/program_options.hpp>
#include <iostream>
#include <string>
#include <vector>
#include "utils/logging.h"
#include "utils/utils.h"

namespace ustore {

namespace po = boost::program_options;

class Argument {
 public:
  bool is_help;

  Argument() noexcept;
  ~Argument() = default;

  bool ParseCmdArgs(int argc, char* argv[]);

  bool ParseCmdArgs(const std::vector<std::string>& args);

 protected:
  virtual bool CheckArgs() { return true; }

  template<typename T>
  bool Check(const T& var, const bool expr, const std::string& title,
             const std::string& expect);

  template<typename T1, typename T2>
  inline bool CheckEQ(const T1& var, const T2& expect,
                      const std::string& title) {
    return Check(var, var == expect, title, "=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  inline bool CheckNE(const T1& var, const T2& expect,
                      const std::string& title) {
    return Check(var, var != expect, title, "!=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  inline bool CheckLE(const T1& var, const T2& expect,
                      const std::string& title) {
    return Check(var, var <= expect, title, "<=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  inline bool CheckLT(const T1& var, const T2& expect,
                      const std::string& title) {
    return Check(var, var < expect, title, "<" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  inline bool CheckGE(const T1& var, const T2& expect,
                      const std::string& title) {
    return Check(var, var >= expect, title, ">=" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  inline bool CheckGT(const T1& var, const T2& expect,
                      const std::string& title) {
    return Check(var, var > expect, title, ">" + Utils::ToString(expect));
  }

  template<typename T1, typename T2>
  inline bool CheckInRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return Check(var, lbound <= var && var <= ubound, title, "range of" +
                 Utils::ToStringPair(lbound, ubound, "[", "]", ","));
  }

  template<typename T1, typename T2>
  inline bool CheckInLeftOpenRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return Check(var, lbound < var && var <= ubound, title, "range of" +
                 Utils::ToStringPair(lbound, ubound, "(", "]", ","));
  }

  template<typename T1, typename T2>
  inline bool CheckInRightOpenInRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    return Check(var, lbound <= var && var < ubound, title, "range of" +
                 Utils::ToStringPair(lbound, ubound, "[", ")", ","));
  }

  template<typename T1, typename T2>
  inline bool CheckInOpenRange(
    const T1& var, const T2& lbound, const T2& ubound,
    const std::string& title) {
    const std::string expect = "range of (" + Utils::ToString(lbound) +
                               "," + Utils::ToString(ubound) + ")";
    return Check(var, lbound < var && var < ubound, title, "range of" +
                 Utils::ToStringPair(lbound, ubound, "(", ")", ","));
  }

  inline void Add(std::string* param_ptr, const std::string& name_long,
                  const std::string& name_short, const std::string& desc,
                  const std::string& deft_val = "") {
    args_.emplace_back(
      Meta<std::string>({param_ptr, name_long, name_short, desc, deft_val}));
  }
  inline void Add(bool* param_ptr, const std::string& name_long,
                  const std::string& name_short, const std::string& desc) {
    bool_args_.emplace_back(
      Meta<bool>({param_ptr, name_long, name_short, desc, false}));
  }
  inline void Add(int* param_ptr, const std::string& name_long,
                  const std::string& name_short, const std::string& desc,
                  const int& deft_val = 0) {
    int_args_.emplace_back(
      Meta<int>({param_ptr, name_long, name_short, desc, deft_val}));
  }
  inline void Add(int64_t* param_ptr, const std::string& name_long,
                  const std::string& name_short, const std::string& desc,
                  const int64_t& deft_val = 0) {
    int64_args_.emplace_back(
      Meta<int64_t>({param_ptr, name_long, name_short, desc, deft_val}));
  }
  inline void Add(size_t* param_ptr, const std::string& name_long,
                  const std::string& name_short, const std::string& desc,
                  const size_t& deft_val = 0) {
    size_args_.emplace_back(
      Meta<size_t>({param_ptr, name_long, name_short, desc, deft_val}));
  }
  inline void Add(double* param_ptr, const std::string& name_long,
                  const std::string& name_short, const std::string& desc,
                  const double& deft_val = 0.0) {
    double_args_.emplace_back(
      Meta<double>({param_ptr, name_long, name_short, desc, deft_val}));
  }
  inline void Positional(const std::string& name_long) {
    pos_arg_names_.emplace_back(name_long);
  }

 private:
  template<typename T>
  struct Meta {
    T* param_ptr;
    const std::string name_long;
    const std::string name_short;
    const std::string desc;
    const T deft_val;
  };

  std::vector<Meta<std::string>> args_;
  std::vector<Meta<bool>> bool_args_;
  std::vector<Meta<int>> int_args_;
  std::vector<Meta<int64_t>> int64_args_;
  std::vector<Meta<size_t>> size_args_;
  std::vector<Meta<double>> double_args_;

  std::vector<std::string> pos_arg_names_;

  inline void AssignArgs(const std::vector<Meta<std::string>>& args,
                         const po::variables_map& vm) {
    for (auto& meta : args) {
      *(meta.param_ptr) = vm[meta.name_long].as<std::string>();
    }
  }
  inline void AssignArgs(const std::vector<Meta<bool>>& args,
                         const po::variables_map& vm) {
    for (auto& meta : args) {
      *(meta.param_ptr) = vm[meta.name_long].as<bool>();
    }
  }
  inline void AssignArgs(const std::vector<Meta<int>>& args,
                         const po::variables_map& vm) {
    for (auto& meta : args) {
      *(meta.param_ptr) = vm[meta.name_long].as<int>();
    }
  }
  inline void AssignArgs(const std::vector<Meta<int64_t>>& args,
                         const po::variables_map& vm) {
    for (auto& meta : args) {
      *(meta.param_ptr) = vm[meta.name_long].as<int64_t>();
    }
  }
  inline void AssignArgs(const std::vector<Meta<size_t>>& args,
                         const po::variables_map& vm) {
    for (auto& meta : args) {
      *(meta.param_ptr) = vm[meta.name_long].as<size_t>();
    }
  }
  inline void AssignArgs(const std::vector<Meta<double>>& args,
                         const po::variables_map& vm) {
    for (auto& meta : args) {
      *(meta.param_ptr) = vm[meta.name_long].as<double>();
    }
  }

  template<typename T>
  void AddArgs(const std::vector<Meta<T>> args, po::options_description* od);

  bool ParseCmdArgs(int argc, char* argv[], po::variables_map* vm);
};

template<typename T>
bool Argument::Check(const T& var, const bool expr,
                     const std::string& title, const std::string& expect) {
  if (expr) {
    LOG(INFO) << "[ARG] " << title << ": " << var;
  } else {
    std::cerr << BOLD_RED("[ERROR ARG] ") << title << ": "
              << "[Actual] " << var << ", [Expected] " << expect << std::endl;
  }
  return expr;
}

template<typename T>
void Argument::AddArgs(const std::vector<Meta<T>> args,
                       po::options_description* od) {
  for (auto& meta : args) {
    auto& name_long = meta.name_long;
    auto& name_short = meta.name_short;
    auto cfg = name_long + (name_short.empty() ? "" : "," + name_short);
    auto& desc = meta.desc;
    auto& deft_val = meta.deft_val;

    od->add_options()
    (cfg.c_str(), po::value<T>()->default_value(deft_val), desc.c_str());
  }
}

}  // namespace ustore

#endif  // USTORE_UTILS_ARGUMENT_H_
