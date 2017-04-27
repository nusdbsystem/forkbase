// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_SIMPLE_DATASET_H_
#define USTORE_EXAMPLE_CA_SIMPLE_DATASET_H_

#include <map>
#include <string>
#include "config.h"

namespace ustore {
namespace example {
namespace ca {

class SimpleDataset {
 public:
  static const MAP<std::string, StringList> GenerateTable(
    const size_t n_columns, const size_t n_records);

 private:
  static const StringList GenerateColumn(
    const std::string& col_name, const size_t n_records);
};

} // namespace ca
} // namespace example
} // namespace ustore

#endif // USTORE_EXAMPLE_CA_SIMPLE_DATASET_H_
