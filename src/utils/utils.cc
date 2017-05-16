// Copyright (c) 2017 The Ustore Authors.

#include <boost/tokenizer.hpp>

#include "utils/utils.h"

namespace ustore {

UType Utils::ToUType(const std::string& str) {
  if (str == "bool") return UType::kBool;
  if (str == "num") return UType::kNum;
  if (str == "string") return UType::kString;
  if (str == "blob") return UType::kBlob;
  if (str == "list") return UType::kList;
  if (str == "set") return UType::kSet;
  if (str == "map") return UType::kMap;
  return UType::kUnknown;
}

std::string Utils::ToString(const UType& type) {
  switch (type) {
    case UType::kBool: return "bool";
    case UType::kNum: return "num";
    case UType::kString: return "string";
    case UType::kBlob: return "blob";
    case UType::kList: return "list";
    case UType::kSet: return "set";
    case UType::kMap: return "map";
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

}  // namespace ustore
