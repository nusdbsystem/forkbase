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
    ASSERT_EQ(*(expected1 + i), *(result1 + i));
  }
  delete[] result1;

  const ustore::byte_t* result2 = ustore::SpliceBytes(data, 5, 2, 0, append, 3);
  const ustore::byte_t expected2[] = "abxyzcde";

  for (size_t i = 0; i < 8; i++) {
    ASSERT_EQ(*(expected2 + i), *(result2 + i));
  }
  delete[] result2;

  const ustore::byte_t* result3 = ustore::SpliceBytes(data, 5, 3, 3, append, 3);
  const ustore::byte_t expected3[] = "abcxyz";

  for (size_t i = 0; i < 6; i++) {
    ASSERT_EQ(*(expected3 + i), *(result3 + i));
  }
  delete[] result3;

  const ustore::byte_t* result4 = ustore::SpliceBytes(data, 5, 3, 1, append, 3);
  const ustore::byte_t expected4[] = "abcxyze";

  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(*(expected4 + i), *(result4 + i));
  }
  delete[] result4;

  const ustore::byte_t* result5 = ustore::SpliceBytes(data, 5, 3, 1, append, 0);
  const ustore::byte_t expected5[] = "abce";

  for (size_t i = 0; i < 4; i++) {
    ASSERT_EQ(*(expected5 + i), *(result5 + i));
  }
  delete[] result5;

  const ustore::byte_t* result6 =
      ustore::SpliceBytes(data, 5, 10, 1, append, 3);
  const ustore::byte_t expected6[] = "abcdexyz";

  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(*(expected6 + i), *(result6 + i));
  }
  delete[] result6;

  const ustore::byte_t* result7 =
      ustore::SpliceBytes(data, 5, 10, 0, append, 3);
  const ustore::byte_t expected7[] = "abcdexyz";

  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(*(expected7 + i), *(result7 + i));
  }
  delete[] result7;
}
