// Copyright (c) 2017 The Ustore Authors.
#include <cstdlib>

#include <codecvt>
#include <locale>
#include <string>
#include <vector>
#include <iostream>

namespace ustore {
namespace example {
namespace huawei {

constexpr int kDefaultMaximumInt = 1000000000;

std::string CreateIntColumn(int);
std::string CreateStrColumn(int);
std::string CreateZhStrColumn(int);
std::string CreateLocationColumn();
std::string CreateFloatColumn();
std::string CreateTimeColumn();

enum DataType {
  kStr,    // normal string
  kZhStr,  // string with Chinese characters
  kInt,
  kLocation,
  kFloat,
  kTime
};

struct ColSchema {
  std::string col_name;
  size_t width;
  DataType type;

  ColSchema(const std::string& str, size_t len, DataType t)
      : col_name(str), width(len), type(t) {}
  ColSchema(const char* str, size_t len, DataType t)
      : col_name(str), width(len), type(t) {}
};

std::string GenerateRow(const std::vector<ColSchema>&);
std::string GenerateHeader(const std::vector<ColSchema>&);

}  // namespace huawei
}  // namespace example
}  // namespace ustore
