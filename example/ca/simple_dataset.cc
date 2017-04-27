// Copyright (c) 2017 The Ustore Authors.

#include "worker/worker.h"

#include "simple_dataset.h"

namespace ustore {
namespace example {
namespace ca {

const StringList SimpleDataset::GenerateColumn(
  const std::string& col_name, const size_t n_records) {
  StringList col;
  for (size_t i = 0; i < n_records; ++i) {
    col.emplace_back(col_name + "-" + std::to_string(i));
  }
  return col;
}

const MAP<std::string, StringList> SimpleDataset::GenerateTable(
  const size_t n_columns, const size_t n_records) {
  MAP<std::string, StringList> table;
  table["Key"] = GenerateColumn("K", n_records);
  for (size_t i = 0; i < n_columns; ++i) {
    const std::string col_name = "C" + std::to_string(i);
    table[col_name] = GenerateColumn(col_name, n_records);
  }
  return table;
}

} // namespace ca
} // namespace example
} // namespace ustore
