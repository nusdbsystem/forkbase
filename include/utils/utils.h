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

namespace ustore {

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x)   __builtin_expect(!!(x), 0)

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

#define FONT_RESET          "\033[0m"
#define FONT_BLACK          "\033[30m"          /* Black */
#define FONT_RED            "\033[31m"          /* Red */
#define FONT_GREEN          "\033[32m"          /* Green */
#define FONT_YELLOW         "\033[33m"          /* Yellow */
#define FONT_BLUE           "\033[34m"          /* Blue */
#define FONT_MAGENTA        "\033[35m"          /* Magenta */
#define FONT_CYAN           "\033[36m"          /* Cyan */
#define FONT_WHITE          "\033[37m"          /* White */
#define FONT_BOLD_BLACK     "\033[1m\033[30m"   /* Bold Black */
#define FONT_BOLD_RED       "\033[1m\033[31m"   /* Bold Red */
#define FONT_BOLD_GREEN     "\033[1m\033[32m"   /* Bold Green */
#define FONT_BOLD_YELLOW    "\033[1m\033[33m"   /* Bold Yellow */
#define FONT_BOLD_BLUE      "\033[1m\033[34m"   /* Bold Blue */
#define FONT_BOLD_MAGENTA   "\033[1m\033[35m"   /* Bold Magenta */
#define FONT_BOLD_CYAN      "\033[1m\033[36m"   /* Bold Cyan */
#define FONT_BOLD_WHITE     "\033[1m\033[37m"   /* Bold White */

#define COLOR(font_color, msg) \
  font_color << msg << FONT_RESET

#define BLACK(msg)          COLOR(FONT_BLACK, msg)
#define RED(msg)            COLOR(FONT_RED, msg)
#define GREEN(msg)          COLOR(FONT_GREEN, msg)
#define YELLOW(msg)         COLOR(FONT_YELLOW, msg)
#define BLUE(msg)           COLOR(FONT_BLUE, msg)
#define MAGENTA(msg)        COLOR(FONT_MAGENTA, msg)
#define CYAN(msg)           COLOR(FONT_CYAN, msg)
#define WHITE(msg)          COLOR(FONT_WHITE, msg)
#define BOLD_BLACK(msg)     COLOR(FONT_BOLD_BLACK, msg)
#define BOLD_RED(msg)       COLOR(FONT_BOLD_RED, msg)
#define BOLD_GREEN(msg)     COLOR(FONT_BOLD_GREEN, msg)
#define BOLD_YELLOW(msg)    COLOR(FONT_BOLD_YELLOW, msg)
#define BOLD_BLUE(msg)      COLOR(FONT_BOLD_BLUE, msg)
#define BOLD_MAGENTA(msg)   COLOR(FONT_BOLD_MAGENTA, msg)
#define BOLD_CYAN(msg)      COLOR(FONT_BOLD_CYAN, msg)
#define BOLD_WHITE(msg)     COLOR(FONT_BOLD_WHITE, msg)

class Utils {
 public:
  template<typename T>
  static inline std::string ToString(const T& obj);

  static inline const std::string& ToString(const std::string& str) {
    return str;
  }

  template<typename T>
  static inline std::string ToStringPair(
    const T& a, const T& b, const std::string& lsymbol = "(",
    const std::string& rsymbol = ")", const std::string& sep = ", ");

  template<typename T>
  static inline std::string ToStringSeq(
    const T& begin, const T& end, const std::string& lsymbol = "[",
    const std::string& rsymbol = "]", const std::string& sep = ", ",
    const bool elem_in_quote = false);

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
    const std::string& str, const char* sep_chars = " \t[],");

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

  static void PrintList(const UList& list, const bool elem_in_quote = false,
                        std::ostream& os = std::cout);

  static void PrintMap(const UMap& map, const bool elem_in_quote = false,
                       std::ostream& os = std::cout);

  static void PrintMapKeys(const UMap& map, const bool elem_in_quote = false,
                           std::ostream& os = std::cout);

  static void PrintListDiff(DuallyDiffIndexIterator& it_diff,
                            const bool show_diff = true,
                            const bool elem_in_quote = false,
                            std::ostream& os = std::cout);

  template<class T>
  static void PrintDiff(T& it_diff, const bool show_diff = true,
                        const bool elem_in_quote = false,
                        std::ostream& os = std::cout);
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
  const std::string& rsymbol, const std::string& sep,
  const bool elem_in_quote) {
  const auto quote = elem_in_quote ? "\"" : "";
  std::stringstream ss;
  ss << lsymbol;
  auto it = begin;
  if (it != end) {
    ss << quote << *it++ << quote;
    while (it != end) {
      ss << sep << quote << *it++ << quote;
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

template<typename T>
std::vector<size_t> Utils::SortIndexes(const std::vector<T>& v) {
  std::vector<size_t> idx(v.size());
  std::iota(idx.begin(), idx.end(), 0);

  // sort indexes based on comparing values in v
  std::sort(idx.begin(), idx.end(),
  [&v](size_t i1, size_t i2) {return v[i1] < v[i2];});

  return idx;
}

template<class T>
void Utils::PrintDiff(T& it_diff, const bool show_diff,
                      const bool elem_in_quote, std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto f_print_diff_key = [&os, &it_diff, &quote]() {
    os << quote << it_diff.key() << quote;
  };
  auto f_print_diff = [&os, &it_diff, &quote]() {
    os << quote << it_diff.key() << quote << ":(";
    auto lhs = it_diff.lhs_value();
    if (lhs.empty()) { os << "_"; } else { os << quote << lhs << quote; }
    os << ',';
    auto rhs = it_diff.rhs_value();
    if (rhs.empty()) { os << "_"; } else { os << quote << rhs << quote; }
    os << ')';
  };

  os << "[";
  if (!it_diff.end()) {
    show_diff ? f_print_diff() : f_print_diff_key();
    for (it_diff.next(); !it_diff.end(); it_diff.next()) {
      os << ", ";
      show_diff ? f_print_diff() : f_print_diff_key();
    }
  }
  os << "]";
}

inline std::ostream& operator<<(std::ostream& os, const UType& obj) {
  os << Utils::ToString(obj);
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const ErrorCode& obj) {
  os << static_cast<int>(obj);
  return os;
}

}  // namespace ustore

#endif  // USTORE_UTILS_UTILS_H_
