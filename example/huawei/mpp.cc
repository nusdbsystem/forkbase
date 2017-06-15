// Copyright (c) 2017 The Ustore Authors.

#include <fstream>
#include <iostream>
#include <vector>

#include "utils.h"
#include "utils/logging.h"

using namespace ustore;

constexpr long long k1GBSize = 8000000;
constexpr long long k1TBSize = k1GBSize * 1024;

int main(int argc, char* argv[]) {
  std::vector<ustore::example::huawei::ColSchema> schema;
  schema.emplace_back("BEGINDAY", ::ustore::example::huawei::kDefaultMaximumInt,
                      ::ustore::example::huawei::kInt);
  schema.emplace_back("ENDDAY", ::ustore::example::huawei::kDefaultMaximumInt,
                      ::ustore::example::huawei::kInt);
  schema.emplace_back("MSISDN", 32, ::ustore::example::huawei::kStr);
  schema.emplace_back("IMSI", 32, ::ustore::example::huawei::kStr);
  schema.emplace_back("LOCATION", 0, ::ustore::example::huawei::kLocation);
  schema.emplace_back("OCCUR_TIMES", 1000, ::ustore::example::huawei::kInt);
  schema.emplace_back("OCCUR_DAYS", 1000, ::ustore::example::huawei::kInt);
  schema.emplace_back("TOTAL_DAYS", 1000, ::ustore::example::huawei::kInt);
  schema.emplace_back("OCCUR_PCT", 0, ::ustore::example::huawei::kFloat);
  schema.emplace_back("BD", ::ustore::example::huawei::kDefaultMaximumInt,
                      ::ustore::example::huawei::kInt);
  schema.emplace_back("AD", ::ustore::example::huawei::kDefaultMaximumInt,
                      ::ustore::example::huawei::kInt);

  std::ofstream os("mpp.csv");
  os << ustore::example::huawei::GenerateHeader(schema) << std::endl;
  int old_percentage = -1;
  for (long long i = 0; i < k1GBSize; ++i) {
    os << ustore::example::huawei::GenerateRow(schema) << std::endl;
    int percentage = i * 100 / k1GBSize;
    if (old_percentage != percentage) {
      old_percentage = percentage;
      if (percentage % 5 == 0)
        std::cout << percentage << "\% genereated..." << std::endl;
    }
  }
  std::cout << "mpp.csv generated" << std::endl;

  return 0;
}
