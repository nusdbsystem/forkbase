// Copyright (c) 2017 The Ustore Authors.

#include <algorithm>
#include <sstream>
#include <iostream>

#include "utils.h"

namespace ustore {
namespace example {
namespace huawei {

using ConverterT = std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t>;

constexpr char delimiter = ',';

static const char alphabet[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

static std::string zh_dict = "中华人民共和国与世界大团结万岁";

std::wstring s2ws(const std::string& str) {
  ConverterT converter;
  return converter.from_bytes(str);
}

std::string ws2s(const std::wstring& wstr) {
  ConverterT converter;
  return converter.to_bytes(wstr);
}

std::string CreateIntColumn(int max) {
  int r = rand() % max;
  std::stringstream ss;
  ss << r;
  return ss.str();
}

std::string CreateStrColumn(int length) {
  std::string str;
  std::generate_n(std::back_inserter(str), length, [&]() {
    int idx = rand() % 62;
    return alphabet[idx];
  });
  return str;
}

std::string CreateZhStrColumn(int length) {
  auto ws = s2ws(zh_dict);
  auto len = ws.length();
  std::wstring str;
  std::generate_n(std::back_inserter(str), length, [&]() {
    int idx = rand() % len;
    return ws[idx];
  });
  return ws2s(str);
}

std::string CreateLocationColumn() {
  std::stringstream ss;
  ss << "(" << rand() % 90 << "." << rand() % 100 << "#" << rand() % 180 << "."
     << rand() % 100 << ")";
  return ss.str();
}

std::string CreateFloatColumn() {
  std::stringstream ss;
  ss << "" << rand() % 100 << "." << rand() % 100 << "";
  return ss.str();
}

std::string CreateTimeColumn() {
  std::stringstream ss;
  ss << "[" << rand() % 24 << ":" << rand() % 60 << "]";
  return ss.str();
}

std::string GenerateHeader(
    const std::vector<ustore::example::huawei::ColSchema>& schema) {
  std::string ret;
  for (const auto& col : schema) {
    ret += col.col_name;
    ret += delimiter;
  }
  ret.erase(ret.end() - 1);
  return ret;
}

std::string GenerateRow(
    const std::vector<ustore::example::huawei::ColSchema>& schema) {
  std::string ret;
  for (const auto& col : schema) {
    switch (col.type) {
      case ustore::example::huawei::kStr:
        ret += CreateStrColumn(col.width);
        break;
      case ustore::example::huawei::kZhStr:
        ret += CreateZhStrColumn(col.width);
        break;
      case ustore::example::huawei::kInt:
        ret += CreateIntColumn(col.width);
        break;
      case ustore::example::huawei::kLocation:
        ret += CreateLocationColumn();
        break;
      case ustore::example::huawei::kFloat:
        ret += CreateFloatColumn();
        break;
      case ustore::example::huawei::kTime:
        ret += CreateTimeColumn();
        break;
    }
    ret += delimiter;
  }
  ret.erase(ret.end() - 1);
  return ret;
}

}  // namespace huawei
}  // namespace example
}  // namespace ustore
