// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_UTILS_H_
#define USTORE_EXAMPLE_CA_UTILS_H_

#include <string>
#include <sstream>
#include <utility>

#include "config.h"

namespace ustore {
namespace example {
namespace ca {

class Utils {
 public:
  template<class T>
  static inline const std::string ToString(const LIST_TYPE<T>& list) {
    static std::stringstream ss;
    ss.str(std::string());
    ss << "[";
    auto it = list.cbegin();
    if (it != list.cend()) {
      ss << *it;
      for (; it != list.cend(); ++it) {
        ss << ", " << *it;
      }
    }
    ss << "]";
    return ss.str();
  }

  template<class K, class T>
  static inline const MAP_TYPE<K, std::string> ToStringMap(
    const MAP_TYPE<K, LIST_TYPE<T>>& map) {
    MAP_TYPE<K, std::string> str_map;
    for (const auto& k_vl : map) {
      str_map[k_vl.first] = std::move(ToString(k_vl.second));
    }
    return str_map;
  }
};

} // namespace ca
} // namespace example
} // namespace ustore

#endif // USTORE_EXAMPLE_CA_UTILS_H_
