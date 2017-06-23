// Copyright (c) 2017 The Ustore Authors.

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "huawei/utils.h"
#include "utils/logging.h"

using namespace ustore;

constexpr size_t kInterval = 1000;
constexpr size_t k1GBSize = 4100000;
constexpr size_t k1TBSize = k1GBSize * 1024;

const std::string kFileName = "deduplication.csv";

int main(int argc, char* argv[]) {
  std::vector<ustore::example::huawei::ColSchema> schema;
  schema.emplace_back("MSISDN", 15, ::ustore::example::huawei::kStr);
  schema.emplace_back("IMSI", 15, ::ustore::example::huawei::kStr);
  schema.emplace_back("IMEI", 15, ::ustore::example::huawei::kStr);
  schema.emplace_back("HOMEAREA", 64 / 2, ::ustore::example::huawei::kZhStr);
  schema.emplace_back("CURAREA", 64 / 2, ::ustore::example::huawei::kZhStr);
  schema.emplace_back("LOCATION", 0, ::ustore::example::huawei::kLocation);
  schema.emplace_back("CAPTURETIME",
                      ::ustore::example::huawei::kDefaultMaximumInt,
                      ::ustore::example::huawei::kInt);

  std::vector<std::string> pre_generated;
  for (size_t i = 0; i < kInterval; ++i)
    pre_generated.push_back(ustore::example::huawei::GenerateRow(schema));

  std::ofstream os(kFileName);
  os << ustore::example::huawei::GenerateHeader(schema) << std::endl;

  for (size_t k = 0; k < k1GBSize / kInterval; ++k) {
    size_t replaced_idx = rand() % kInterval;
    for (size_t i = 0; i < replaced_idx; ++i)
      os << pre_generated[i] << std::endl;
    os << ustore::example::huawei::GenerateRow(schema) << std::endl;
    for (size_t i = replaced_idx + 1; i < kInterval; ++i)
      os << pre_generated[i] <<std::endl;
  }
  std::cout << kFileName << " generated" << std::endl;

  return 0;
}
