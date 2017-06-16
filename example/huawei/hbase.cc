// Copyright (c) 2017 The Ustore Authors.

#include <fstream>
#include <iostream>
#include <vector>

#include "huawei/utils.h"
#include "utils/logging.h"

using namespace ustore;

constexpr long long k1GBSize = 4100000;
constexpr long long k1TBSize = k1GBSize * 1024;

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

  std::ofstream os("hbase.csv");
  os << ustore::example::huawei::GenerateHeader(schema) << std::endl;
  int old_percentage = -1;
  for (long long i = 0; i < k1GBSize; ++i) {
    os << ustore::example::huawei::GenerateRow(schema) << std::endl;
    int percentage = i * 100 / k1GBSize;
    if (old_percentage != percentage) {
      old_percentage = percentage;
      if (percentage % 5 == 0)
        std::cout << percentage << "% genereated..." << std::endl;
    }
  }
  std::cout << "hbase.csv generated" << std::endl;

  return 0;
}
