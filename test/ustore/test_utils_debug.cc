// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>
#include "gtest/gtest.h"
#include "utils/debug.h"

TEST(UtilsDebug, SpliceBytes) {
  const ustore::byte_t data[] = "abcde";
  const ustore::byte_t append[] = "xyz";

  const ustore::byte_t* result1 = ustore::SpliceBytes(data, 5, 0, 1, append, 3);
  const ustore::byte_t expected1[] = "xyzbcde";

  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(*(result1 + i), *(expected1 + i));
  }

  const ustore::byte_t* result2 = ustore::SpliceBytes(data, 5, 2, 0, append, 3);
  const ustore::byte_t expected2[] = "abxyzcde";

  for (size_t i = 0; i < 8; i++) {
    ASSERT_EQ(*(result2 + i), *(expected2 + i));
  }

  const ustore::byte_t* result3 = ustore::SpliceBytes(data, 5, 3, 3, append, 3);
  const ustore::byte_t expected3[] = "abcxyz";

  for (size_t i = 0; i < 6; i++) {
    ASSERT_EQ(*(result3 + i), *(expected3 + i));
  }

  const ustore::byte_t* result4 = ustore::SpliceBytes(data, 5, 3, 1, append, 3);
  const ustore::byte_t expected4[] = "abcxyze";

  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(*(result4 + i), *(expected4 + i));
  }

  const ustore::byte_t* result5 = ustore::SpliceBytes(data, 5, 3, 1, append, 0);
  const ustore::byte_t expected5[] = "abce";

  for (size_t i = 0; i < 4; i++) {
    ASSERT_EQ(*(result5 + i), *(expected5 + i));
  }

  const ustore::byte_t* result6 =
      ustore::SpliceBytes(data, 5, 10, 1, append, 3);
  const ustore::byte_t expected6[] = "abcdexyz";

  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(*(result6 + i), *(expected6 + i));
  }

  const ustore::byte_t* result7 =
      ustore::SpliceBytes(data, 5, 10, 0, append, 3);
  const ustore::byte_t expected7[] = "abcdexyz";

  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(*(result7 + i), *(expected7 + i));
  }
}
