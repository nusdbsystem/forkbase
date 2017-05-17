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
#include "utils/utils.h"
#include "worker/worker_ext.h"

namespace ustore {
namespace example {
namespace ca {

class Utils : public ::ustore::Utils {
 public:
  template<class K, class T>
  static inline const std::map<K, std::string> ToStringMap(
    const std::map<K, std::list<T>>& map);

  template<class T1, class T2>
  static ErrorCode Print(const T1& key, const T2& branch, WorkerExt& worker);

  template<class T1, class T2>
  static inline ErrorCode Print(const T1& key, const T2& begin,
                                const T2& end, WorkerExt& worker) {
    for (auto it = begin; it != end; ++it) {
      WORKER_GUARD(Print(key, *it, worker));
    }
    return ErrorCode::kOK;
  }

  template<class T1, class T2>
  static inline ErrorCode Print(const T1& key,
                                const std::list<T2>& branches,
                                WorkerExt& worker) {
    return Print(key, branches.cbegin(), branches.cend(), worker);
  }

  template<class T1, class T2>
  static inline ErrorCode Print(const T1& key,
                                const std::queue<T2>& branches,
                                WorkerExt& worker) {
    return Print(key, branches.cbegin(), branches.cend(), worker);
  }

  template<class T1, class T2>
  static inline ErrorCode Print(const T1& key,
                                const std::vector<T2>& branches,
                                WorkerExt& worker) {
    return Print(key, branches.cbegin(), branches.cend(), worker);
  }

  template<class T1, class T2>
  static inline ErrorCode Print(const T1& key,
                                const std::set<T2>& branches,
                                WorkerExt& worker) {
    return Print(key, branches.cbegin(), branches.cend(), worker);
  }

  template<class T1, class T2>
  static inline ErrorCode Print(const T1& key,
                                const std::unordered_set<T2>& branches,
                                WorkerExt& worker) {
    return Print(key, branches.cbegin(), branches.cend(), worker);
  }

 private:
  static const int kKeyPrintWidth;
  static const int kBrnachPrintWidth;
};

template<class K, class T>
const std::map<K, std::string> Utils::ToStringMap(
  const std::map<K, std::list<T>>& map) {
  std::map<K, std::string> str_map;
  for (const auto& k_vl : map) {
    str_map[k_vl.first] = ToString(k_vl.second);
  }
  return str_map;
}

template<class T1, class T2>
ErrorCode Utils::Print(const T1& key, const T2& branch,
                       WorkerExt& worker) {
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

}  // namespace ca
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_CA_UTILS_H_
