// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"

#include "ca/relational.h"
#include "worker/worker.h"

using ustore::example::ca::ColumnStore;
using ustore::example::ca::Column;

inline void ASSERT_SUCCEEDED(ustore::ErrorCode code) {
  ASSERT_EQ(code, ustore::ErrorCode::kOK);
}

void ASSERT_FAILED(ustore::ErrorCode code) {
  ASSERT_NE(code, ustore::ErrorCode::kOK);
}

TEST(Relational, Basic) {
  ustore::Worker worker(17);
  ColumnStore col_store(&worker);

  ustore::ErrorCode code;
  const std::string table_name("tname");
  ASSERT_SUCCEEDED(col_store.CreateTable(table_name));

  const std::string col1_name("col1");
  const std::vector<std::string> expected_col1{"col1_v1", "col1_v2"};

  ASSERT_SUCCEEDED(col_store.PutColumn(table_name,
    ColumnStore::kDefaultBranch, col1_name, expected_col1));

  Column actual_col1;
  ASSERT_SUCCEEDED(col_store.GetColumn(table_name,
    ColumnStore::kDefaultBranch, col1_name, &actual_col1));

  ASSERT_EQ(2, actual_col1.numElements());

  ASSERT_SUCCEEDED(col_store.RemoveColumn(table_name,
    ColumnStore::kDefaultBranch, col1_name));

  ASSERT_FAILED(col_store.GetColumn(table_name,
    ColumnStore::kDefaultBranch, col1_name, &actual_col1));

  std::string new_branch("branch");
  ASSERT_SUCCEEDED(col_store.BranchTable(table_name, new_branch,
                                         ColumnStore::kDefaultBranch));

  const std::string col2_name("col2");
  const std::vector<std::string> expected_col2{"col2_v1", "col2_v2", "col2_v3"};

  ASSERT_SUCCEEDED(col_store.PutColumn(table_name,
    new_branch, col2_name, expected_col2));

  Column actual_col2;
  ASSERT_SUCCEEDED(col_store.GetColumn(table_name,
    new_branch, col2_name, &actual_col2));

  ASSERT_EQ(3, actual_col2.numElements());

  const std::string col3_name("col3");
  const std::vector<std::string> expected_col3{"col3_v1"};

  ASSERT_SUCCEEDED(col_store.MergeTable(table_name, new_branch,
    ColumnStore::kDefaultBranch, col3_name, expected_col3));

  Column actual_col3;
  ASSERT_SUCCEEDED(col_store.GetColumn(table_name,
    new_branch, col3_name, &actual_col3));

  ASSERT_EQ(1, actual_col3.numElements());
}
