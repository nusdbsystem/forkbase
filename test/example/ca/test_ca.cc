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
  ColumnStore cs(&worker);

  ustore::ErrorCode code;
  const std::string table_name("Test");
  ASSERT_SUCCEEDED(cs.CreateTable(table_name, "master"));

  const std::string col1_name("col1");
  const std::vector<std::string> expected_col1{"col1_v1", "col1_v2"};

  ASSERT_SUCCEEDED(
    cs.PutColumn(table_name, "master", col1_name, expected_col1));

  Column actual_col1;
  ASSERT_SUCCEEDED(
    cs.GetColumn(table_name, "master", col1_name, &actual_col1));

  ASSERT_EQ(2, actual_col1.numElements());

  ASSERT_SUCCEEDED(
    cs.RemoveColumn(table_name, "master", col1_name));

  ASSERT_FAILED(
    cs.GetColumn(table_name, "master", col1_name, &actual_col1));

  std::string new_branch("branch");
  ASSERT_SUCCEEDED(
    cs.BranchTable(table_name, "master", new_branch));

  const std::string col2_name("col2");
  const std::vector<std::string> expected_col2{"col2_v1", "col2_v2", "col2_v3"};

  ASSERT_SUCCEEDED(
    cs.PutColumn(table_name, new_branch, col2_name, expected_col2));

  Column actual_col2;
  ASSERT_SUCCEEDED(
    cs.GetColumn(table_name, new_branch, col2_name, &actual_col2));

  ASSERT_EQ(3, actual_col2.numElements());

  const std::string col3_name("col3");
  const std::vector<std::string> expected_col3{"col3_v1"};

  ASSERT_SUCCEEDED(
    cs.MergeTable(table_name, new_branch, "master", col3_name, expected_col3));

  Column actual_col3;
  ASSERT_SUCCEEDED(
    cs.GetColumn(table_name, new_branch, col3_name, &actual_col3));

  ASSERT_EQ(1, actual_col3.numElements());
}
