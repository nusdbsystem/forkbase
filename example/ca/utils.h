// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_UTILS_H_
#define USTORE_EXAMPLE_CA_UTILS_H_

#include <boost/tokenizer.hpp>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <sstream>
#include <unordered_set>
#include <utility>
#include <vector>
#include "spec/value.h"
#include "types/type.h"
#include "utils/logging.h"
#include "worker/worker.h"

namespace ustore {
namespace example {
namespace ca {

#define EQUAL_OR_ELSE_RETURN(expr, expected) do { \
  auto v = expr; \
  if (v != expected) return v; } while(0)

#define EQUAL_OR_ELSE_RETURN_CAST(expr, expected, cast_t) do { \
  auto v = expr; \
  if (v != expected) return static_cast<cast_t>(v); } while(0)

#define GUARD(op) EQUAL_OR_ELSE_RETURN(op, true)

#define GUARD_INT(op) EQUAL_OR_ELSE_RETURN(op, 0)

#define USTORE_GUARD(op) EQUAL_OR_ELSE_RETURN(op, ::ustore::ErrorCode::kOK)

#define USTORE_GUARD_INT(op) \
  EQUAL_OR_ELSE_RETURN_CAST(op, ::ustore::ErrorCode::kOK, int)

class Utils {
 public:
  template<typename T>
  static inline const std::string ToString(const T& obj);

  template<typename T>
  static inline const std::string ToStringPair(
    const T& a, const T& b, const std::string& lsymbol = "(",
    const std::string& rsymbol = ")", const std::string& sep = ", ");

  template<typename T>
  static inline const std::string ToStringSeq(
    const T& begin, const T& end, const std::string& lsymbol = "[",
    const std::string& rsymbol = "]", const std::string& sep = ", ");

  template<class T>
  static inline const std::string ToString(const std::list<T>& list) {
    return ToStringSeq(list.cbegin(), list.cend());
  }

  template<class T>
  static inline const std::string ToString(const std::queue<T>& queue) {
    return ToStringSeq(queue.cbegin(), queue.cend());
  }

  template<class T>
  static inline const std::string ToString(const std::vector<T>& vec) {
    return ToStringSeq(vec.cbegin(), vec.cend());
  }

  template<class T>
  static inline const std::string ToString(const std::unordered_set<T>& set) {
    return ToStringSeq(set.cbegin(), set.cend(), "{", "}");
  }

  template<class T>
  static inline const std::string ToString(const std::set<T>& set) {
    return ToStringSeq(set.cbegin(), set.cend(), "{", "}");
  }

  template<class K, class T>
  static inline const std::map<K, std::string> ToStringMap(
    const std::map<K, std::list<T>>& map);

  static const std::vector<std::string> Tokenize(
    const std::string& str, const char* sep_chars = " [],");

  template<typename T>
  static const std::vector<T> ToVector(
    const std::string& str,
    const std::function<T(const std::string&)>& f_str_to_val,
    const char* sep_chars = " [],");

  static const std::vector<int> ToIntVector(
    const std::string& str, const char* sep_chars = " [],");

  static const std::vector<double> ToDoubleVector(
    const std::string& str, const char* sep_chars = " [],");

  static const std::vector<long> ToLongVector(
    const std::string& str, const char* sep_chars = " [],");

  template<class T1, class T2>
  static ErrorCode Print(const T1& key, const T2& branch, Worker& worker);

  template<class T1, class T2>
  static inline ErrorCode Print(const T1& key, const T2& begin,
                                const T2& end, Worker& worker) {
    for (auto it = begin; it != end; ++it) {
      WORKER_GUARD(Print(key, *it, worker));
    }
    return ErrorCode::kOK;
  }

  template<class T1, class T2>
  static inline ErrorCode Print(const T1& key,
                                const std::list<T2>& branches,
                                Worker& worker) {
    return Print(key, branches.cbegin(), branches.cend(), worker);
  }

  template<class T1, class T2>
  static inline ErrorCode Print(const T1& key,
                                const std::queue<T2>& branches,
                                Worker& worker) {
    return Print(key, branches.cbegin(), branches.cend(), worker);
  }

  template<class T1, class T2>
  static inline ErrorCode Print(const T1& key,
                                const std::vector<T2>& branches,
                                Worker& worker) {
    return Print(key, branches.cbegin(), branches.cend(), worker);
  }

  template<class T1, class T2>
  static inline ErrorCode Print(const T1& key,
                                const std::set<T2>& branches,
                                Worker& worker) {
    return Print(key, branches.cbegin(), branches.cend(), worker);
  }

  template<class T1, class T2>
  static inline ErrorCode Print(const T1& key,
                                const std::unordered_set<T2>& branches,
                                Worker& worker) {
    return Print(key, branches.cbegin(), branches.cend(), worker);
  }

 private:
  static const int kKeyPrintWidth;
  static const int kBrnachPrintWidth;
};

template<typename T>
const std::string Utils::ToString(const T& obj) {
  std::stringstream ss;
  ss << obj;
  return ss.str();
}

template<typename T>
const std::string Utils::ToStringPair(
  const T& a, const T& b, const std::string& lsymbol,
  const std::string& rsymbol, const std::string& sep) {
  std::stringstream ss;
  ss << lsymbol << a << sep << b << rsymbol;
  return ss.str();
}

template<typename T>
const std::string Utils::ToStringSeq(
  const T& begin, const T& end, const std::string& lsymbol,
  const std::string& rsymbol, const std::string& sep) {
  std::stringstream ss;
  ss << lsymbol;
  auto it = begin;
  if (it != end) {
    ss << *it++;
    for (; it != end; ++it) {
      ss << sep << *it;
    }
  }
  ss << rsymbol;
  return ss.str();
}

template<class K, class T>
const std::map<K, std::string> Utils::ToStringMap(
  const std::map<K, std::list<T>>& map) {
  std::map<K, std::string> str_map;
  for (const auto& k_vl : map) {
    str_map[k_vl.first] = ToString(k_vl.second);
  }
  return str_map;
}

template<typename T>
const std::vector<T> Utils::ToVector(
  const std::string& str,
  const std::function<T(const std::string&)>& f_str_to_val,
  const char* sep_chars) {
  std::vector<T> vec;
  for (const auto& t : Tokenize(str, sep_chars)) {
    vec.emplace_back(f_str_to_val(t));
  }
  return vec;
}

template<class T1, class T2>
ErrorCode Utils::Print(const T1& key, const T2& branch,
                       Worker& worker) {
  const Slice key_slice(key);
  const Slice branch_slice(branch);
  std::cout << std::left << std::setw(kKeyPrintWidth) << key
            << " @" << std::setw(kBrnachPrintWidth) << branch << ": ";
  if (worker.Exists(key_slice, branch_slice)) {
    Value val;
    const auto ec = worker.Get(key_slice, branch_slice, &val);
    if (ec == ErrorCode::kOK) {
      std::cout << val;
    } else {
      std::cout << "<failed>" << std::endl;
      return ec;
    }
  } else {
    std::cout << "<none>";
  }
  std::cout << std::endl;
  return ErrorCode::kOK;
}

} // namespace ca
} // namespace example
} // namespace ustore

#endif // USTORE_EXAMPLE_CA_UTILS_H_
