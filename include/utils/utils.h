// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_UTILS_H_
#define USTORE_UTILS_UTILS_H_

#include <algorithm>
#include <list>
#include <numeric>
#include <queue>
#include <set>
#include <string>
#include <sstream>
#include <unordered_set>
#include <utility>
#include <vector>
#include "types/type.h"
#include "types/server/slist.h"
#include "types/server/smap.h"
#include "spec/value.h"

namespace ustore {

#define EQUAL_OR_ELSE_RETURN(expr, expected) do { \
  auto v = expr; \
  if (v != expected) return v; \
  } while (0)

#define EQUAL_OR_ELSE_RETURN_CAST(expr, expected, cast_t) do { \
  auto v = expr; \
  if (v != expected) return static_cast<cast_t>(v); \
  } while (0)

#define GUARD(op) EQUAL_OR_ELSE_RETURN(op, true)

#define GUARD_INT(op) EQUAL_OR_ELSE_RETURN(op, 0)

#define USTORE_GUARD(op) EQUAL_OR_ELSE_RETURN(op, ::ustore::ErrorCode::kOK)

#define USTORE_GUARD_INT(op) \
  EQUAL_OR_ELSE_RETURN_CAST(op, ::ustore::ErrorCode::kOK, int)

class Utils {
 public:
  template<typename T>
  static inline std::string ToString(const T& obj);

  template<typename T>
  static inline std::string ToStringPair(
    const T& a, const T& b, const std::string& lsymbol = "(",
    const std::string& rsymbol = ")", const std::string& sep = ", ");

  template<typename T>
  static inline std::string ToStringSeq(
    const T& begin, const T& end, const std::string& lsymbol = "[",
    const std::string& rsymbol = "]", const std::string& sep = ", ");

  template<class T>
  static inline std::string ToString(const std::list<T>& list) {
    return ToStringSeq(list.cbegin(), list.cend());
  }

  template<class T>
  static inline std::string ToString(const std::queue<T>& queue) {
    return ToStringSeq(queue.cbegin(), queue.cend());
  }

  template<class T>
  static inline std::string ToString(const std::vector<T>& vec) {
    return ToStringSeq(vec.cbegin(), vec.cend());
  }

  template<class T>
  static inline std::string ToString(const std::unordered_set<T>& set) {
    return ToStringSeq(set.cbegin(), set.cend(), "{", "}");
  }

  template<class T>
  static inline std::string ToString(const std::set<T>& set) {
    return ToStringSeq(set.cbegin(), set.cend(), "{", "}");
  }

  static std::string ToString(const UType& type);

  static std::vector<std::string> Tokenize(
    const std::string& str, const char* sep_chars = " [],");

  template<typename T>
  static std::vector<T> ToVector(
    const std::string& str,
    const std::function<T(const std::string&)>& f_str_to_val,
    const char* sep_chars = " [],");

  static std::vector<int> ToIntVector(
    const std::string& str, const char* sep_chars = " [],");

  static std::vector<double> ToDoubleVector(
    const std::string& str, const char* sep_chars = " [],");

  static std::vector<long> ToLongVector(
    const std::string& str, const char* sep_chars = " [],");

  static UType ToUType(const std::string& str);

  static ErrorCode CheckIndex(const size_t idx, const SList& list);

  template <typename T>
  static std::vector<size_t> SortIndexes(const std::vector<T>& v);
};

template<typename T>
std::string Utils::ToString(const T& obj) {
  std::stringstream ss;
  ss << obj;
  return ss.str();
}

template<typename T>
std::string Utils::ToStringPair(
  const T& a, const T& b, const std::string& lsymbol,
  const std::string& rsymbol, const std::string& sep) {
  std::stringstream ss;
  ss << lsymbol << a << sep << b << rsymbol;
  return ss.str();
}

template<typename T>
std::string Utils::ToStringSeq(
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

template<typename T>
std::vector<T> Utils::ToVector(
  const std::string& str,
  const std::function<T(const std::string&)>& f_str_to_val,
  const char* sep_chars) {
  std::vector<T> vec;
  for (const auto& t : Tokenize(str, sep_chars)) {
    vec.emplace_back(f_str_to_val(t));
  }
  return vec;
}

template <typename T>
std::vector<size_t> Utils::SortIndexes(const std::vector<T>& v) {
  std::vector<size_t> idx(v.size());
  std::iota(idx.begin(), idx.end(), 0);

  // sort indexes based on comparing values in v
  std::sort(idx.begin(), idx.end(),
  [&v](size_t i1, size_t i2) {return v[i1] < v[i2];});

  return idx;
}

inline std::ostream& operator<<(std::ostream& os, const UType& obj) {
  os << Utils::ToString(obj);
  return os;
}

}  // namespace ustore

#endif  // USTORE_UTILS_UTILS_H_
