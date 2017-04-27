// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_UTILS_H_
#define USTORE_EXAMPLE_CA_UTILS_H_

#include <boost/tokenizer.hpp>
#include <iostream>
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

namespace ustore {
namespace example {
namespace ca {

class Utils {
 public:
  template<typename T>
  static inline const std::string ToString(const T& obj) {
    std::stringstream ss;
    ss << obj;
    return ss.str();
  }

  template<typename T>
  static inline const std::string ToStringPair(const T& a, const T& b,
      const std::string& lsymbol = "(", const std::string& rsymbol = ")",
      const std::string& sep = ", ") {
    std::stringstream ss;
    ss << lsymbol << a << sep << b << rsymbol;
    return ss.str();
  }

  template<typename T>
  static inline const std::string ToStringSeq(const T& begin, const T& end,
      const std::string& lsymbol = "[", const std::string& rsymbol = "]",
      const std::string& sep = ", ") {
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
    const std::map<K, std::list<T>>& map) {
    std::map<K, std::string> str_map;
    for (const auto& k_vl : map) {
      str_map[k_vl.first] = std::move(ToString(k_vl.second));
    }
    return str_map;
  }

  static inline const std::vector<std::string> Tokenize(
    const std::string& str, const char* sep_chars = " [],") {
    using Tokenizer = boost::tokenizer<boost::char_separator<char>>;
    using CharSep = boost::char_separator<char>;
    std::vector<std::string> vec;
    for (const auto& t : Tokenizer(str, CharSep(sep_chars))) {
      vec.push_back(std::move(t));
    }
    return vec;
  }

  template<typename T>
  static const std::vector<T> ToVector(
    const std::string& str,
    const std::function<T(const std::string&)>& f_str_to_val,
    const char* sep_chars = " [],") {
    std::vector<T> vec;
    for (const auto& t : Tokenize(str, sep_chars)) {
      vec.emplace_back(f_str_to_val(t));
    }
    return vec;
  }

  static const std::vector<int> ToIntVector(
    const std::string& str, const char* sep_chars = " [],") {
    static auto f = [](const std::string & str) { return std::stoi(str); };
    return ToVector<int>(str, f, sep_chars);
  }

  static const std::vector<double> ToDoubleVector(
    const std::string& str, const char* sep_chars = " [],") {
    static auto f = [](const std::string & str) { return std::stod(str); };
    return ToVector<double>(str, f, sep_chars);
  }

  static const std::vector<long> ToLongVector(
    const std::string& str, const char* sep_chars = " [],") {
    static auto f = [](const std::string & str) { return std::stol(str); };
    return ToVector<long>(str, f, sep_chars);
  }

  template<class T1, class T2>
  static inline void Print(const T1& key, const T2& branch, Worker& worker) {
    const Slice key_slice(key);
    const Slice branch_slice(branch);
    std::cout << key << " @" << branch << ": ";
    if (worker.Exists(key_slice, branch_slice)) {
      Value val;
      const auto ec = worker.Get(key_slice, branch_slice, &val);
      CHECK(ec == ErrorCode::kOK);
      std::cout << val;
    } else {
      std::cout << "<none>";
    }
    std::cout << std::endl;
  }

  template<class T1, class T2>
  static inline void Print(const T1& key, const T2& begin, const T2& end,
                           Worker& worker) {
    for (auto it = begin; it != end; ++it) Print(key, *it, worker);
  }

  template<class T1, class T2>
  static inline void Print(const T1& key, const std::list<T2>& branches,
                           Worker& worker) {
    Print(key, branches.cbegin(), branches.cend(), worker);
  }

  template<class T1, class T2>
  static inline void Print(const T1& key, const std::queue<T2>& branches,
                           Worker& worker) {
    Print(key, branches.cbegin(), branches.cend(), worker);
  }

  template<class T1, class T2>
  static inline void Print(const T1& key, const std::vector<T2>& branches,
                           Worker& worker) {
    Print(key, branches.cbegin(), branches.cend(), worker);
  }

  template<class T1, class T2>
  static inline void Print(const T1& key,
                           const std::unordered_set<T2>& branches,
                           Worker& worker) {
    Print(key, branches.cbegin(), branches.cend(), worker);
  }

  template<class T1, class T2>
  static inline void Print(const T1& key, const std::set<T2>& branches,
                           Worker& worker) {
    Print(key, branches.cbegin(), branches.cend(), worker);
  }
};

} // namespace ca
} // namespace example
} // namespace ustore

#endif // USTORE_EXAMPLE_CA_UTILS_H_
