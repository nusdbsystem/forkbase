// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_UTILS_H_
#define USTORE_EXAMPLE_CA_UTILS_H_

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

  template<class T1, class T2>
  static inline void Print(const T1& key, const T2& branch, Worker& worker) {
    const Slice key_slice(key);
    const Slice branch_slice(branch);
    Value val;
    const auto ec = worker.Get(key_slice, branch_slice, &val);
    CHECK(ec == ErrorCode::kOK);
    std::cout << key << " @" << branch << ": " << val << std::endl;
  }
};

} // namespace ca
} // namespace example
} // namespace ustore

#endif // USTORE_EXAMPLE_CA_UTILS_H_
