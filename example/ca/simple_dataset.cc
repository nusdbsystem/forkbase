// Copyright (c) 2017 The Ustore Authors.

#include <utility>
#include "worker/worker.h"

#include "simple_dataset.h"

namespace ustore {
namespace example {
namespace ca {

const ColumnType SimpleDataset::GenerateColumn(
  const std::string& col_name, const size_t n_records) {
  ColumnType column;
  for (size_t i = 0; i < n_records; ++i) {
    column.emplace_back(col_name + "-" + std::to_string(i));
  }
  return column;
}

const TableType SimpleDataset::GenerateTable(
  const size_t n_columns, const size_t n_records) {
  TableType table;
  for (size_t i = 0; i < n_columns; ++i) {
    const std::string col_name = "C" + std::to_string(i);
    table[col_name] = std::move(GenerateColumn(col_name, n_records));
  }
  return table;
}

} // namespace ca
} // namespace example
} // namespace ustore
