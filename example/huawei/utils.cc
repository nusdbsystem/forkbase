// Copyright (c) 2017 The Ustore Authors.

#include <algorithm>
#include <memory>
#include <sstream>

#include "utils.h"

namespace ustore {
namespace example {
namespace huawei {

constexpr char delimiter = '|';

static const char alphabet[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

static std::wstring zh_dict = L"鼠牛虎兔龙蛇马羊猴鸡狗猪";

std::string ws2s(const std::wstring& w_str) {
  if (w_str.empty()) {
    return "";
  }
  unsigned len = w_str.size() * 4 + 1;
  setlocale(LC_CTYPE, "en_US.UTF-8");
  std::unique_ptr<char[]> p(new char[len]);
  wcstombs(p.get(), w_str.c_str(), len);
  std::string str(p.get());
  return str;
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
  auto len = zh_dict.length();
  std::wstring str;
  std::generate_n(std::back_inserter(str), length, [&]() {
    int idx = rand() % len;
    return zh_dict[idx];
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
