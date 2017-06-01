// Copyright (c) 2017 The Ustore Authors.

#include <boost/tokenizer.hpp>
#include "utils/utils.h"

namespace ustore {

UType Utils::ToUType(const std::string& str) {
  if (str == "Bool") return UType::kBool;
  if (str == "Num") return UType::kNum;
  if (str == "String") return UType::kString;
  if (str == "Blob") return UType::kBlob;
  if (str == "List") return UType::kList;
  if (str == "Set") return UType::kSet;
  if (str == "Map") return UType::kMap;
  return UType::kUnknown;
}

std::string Utils::ToString(const UType& type) {
  switch (type) {
    case UType::kBool: return "Bool";
    case UType::kNum: return "Num";
    case UType::kString: return "String";
    case UType::kBlob: return "Blob";
    case UType::kList: return "List";
    case UType::kSet: return "Set";
    case UType::kMap: return "Map";
    default: return "unknown";
  }
}

std::vector<std::string> Utils::Tokenize(
  const std::string& str, const char* sep_chars) {
  using Tokenizer = boost::tokenizer<boost::char_separator<char>>;
  using CharSep = boost::char_separator<char>;
  std::vector<std::string> vec;
  for (const auto& t : Tokenizer(str, CharSep(sep_chars))) {
    vec.push_back(std::move(t));
  }
  return vec;
}

std::vector<int> Utils::ToIntVector(
  const std::string& str, const char* sep_chars) {
  static auto f = [](const std::string & str) { return std::stoi(str); };
  return ToVector<int>(str, f, sep_chars);
}

std::vector<double> Utils::ToDoubleVector(
  const std::string& str, const char* sep_chars) {
  static auto f = [](const std::string & str) { return std::stod(str); };
  return ToVector<double>(str, f, sep_chars);
}

std::vector<long> Utils::ToLongVector(
  const std::string& str, const char* sep_chars) {
  static auto f = [](const std::string & str) { return std::stol(str); };
  return ToVector<long>(str, f, sep_chars);
}

ErrorCode Utils::CheckIndex(const size_t idx, const SList& list) {
  if (idx >= list.numElements()) {
    LOG(WARNING) << "Index out of range: [Actual] " << idx
                 << ", [Expected] <" << list.numElements();
    return ErrorCode::kIndexOutOfRange;
  }
  return ErrorCode::kOK;
}

void Utils::PrintList(const UList& list, const bool elem_in_quote,
                      std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto it = list.Scan();
  os << "[";
  if (!it.end()) {
    os << quote << it.value() << quote;
    for (it.next(); !it.end(); it.next()) {
      os << ", " << quote << it.value() << quote;
    }
  }
  os << "]";
}

void Utils::PrintMap(const UMap& map, const bool elem_in_quote,
                     std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto it = map.Scan();
  auto f_print_it = [&os, &quote, &it]() {
    std::cout << "(" << quote << it.key() << quote << "->" << quote
              << it.value() << quote << ")";
  };
  std::cout << "[";
  if (!it.end()) {
    f_print_it();
    for (it.next(); !it.end(); it.next()) {
      std::cout << ", ";
      f_print_it();
    }
  }
  std::cout << "]";
}

}  // namespace ustore
