// Copyright (c) 2017 The Ustore Authors.

#include "utils.h"

namespace ustore {
namespace example {
namespace ca {

const int Utils::kKeyPrintWidth = 8;
const int Utils::kBrnachPrintWidth = 10;

const std::vector<std::string> Utils::Tokenize(
  const std::string& str, const char* sep_chars) {
  using Tokenizer = boost::tokenizer<boost::char_separator<char>>;
  using CharSep = boost::char_separator<char>;
  std::vector<std::string> vec;
  for (const auto& t : Tokenizer(str, CharSep(sep_chars))) {
    vec.push_back(std::move(t));
  }
  return vec;
}

const std::vector<int> Utils::ToIntVector(
  const std::string& str, const char* sep_chars) {
  static auto f = [](const std::string & str) { return std::stoi(str); };
  return ToVector<int>(str, f, sep_chars);
}

const std::vector<double> Utils::ToDoubleVector(
  const std::string& str, const char* sep_chars) {
  static auto f = [](const std::string & str) { return std::stod(str); };
  return ToVector<double>(str, f, sep_chars);
}

const std::vector<long> Utils::ToLongVector(
  const std::string& str, const char* sep_chars) {
  static auto f = [](const std::string & str) { return std::stol(str); };
  return ToVector<long>(str, f, sep_chars);
}

}  // namespace ca
}  // namespace example
}  // namespace ustore
