// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"

#include "types/ulist.h"

// Check elements scannbed by iterator are all the same to that in vector
inline void CheckIdenticalElements(
  const std::vector<ustore::Slice>& elements,
  ustore::ListIterator* it) {
  for (size_t i = 0; i < elements.size(); i++) {
    auto expected_element = elements[i];

    auto actual_element = it->entry();

    ASSERT_EQ(expected_element.len(), actual_element.len());

    ASSERT_EQ(0, memcmp(expected_element.data(),
                        actual_element.data(),
                        actual_element.len()));
    it->Advance();
  }
  ASSERT_TRUE(it->end());
}


TEST(SList, Small) {
  const ustore::Slice e1("e1", 2);
  const ustore::Slice e2("e22", 3);
  const ustore::Slice e3("e333", 4);
  const ustore::Slice e4("e4444", 5);
  const ustore::Slice e5("e55555", 6);

  // e3 and e4 to be spliced later
  ustore::SList slist({e1, e2, e5});

  size_t val_num_bytes = 0;

  // Get Value by Index
  const ustore::Slice actual_e2 = slist.Get(1);

  EXPECT_EQ(e2.len(), actual_e2.len());
  EXPECT_EQ(0, memcmp(e2.data(), actual_e2.data(), e2.len()));

  // Get Value By index that exceeds total number of elements
  const ustore::Slice nonexist_v5 = slist.Get(5);
  EXPECT_TRUE(nonexist_v5.empty());

  // Test on Iterator
  CheckIdenticalElements({e1, e2, e5}, slist.iterator().get());

  // Splice in middle
  ustore::SList new_slist1(slist.Splice(1, 1, {e3, e4}));
  CheckIdenticalElements({e1, e3, e4, e5},
                         new_slist1.iterator().get());

  // // Splice to the end
  ustore::SList new_slist2(new_slist1.Splice(3, 2, {e2}));
  CheckIdenticalElements({e1, e3, e4, e2},
                         new_slist2.iterator().get());
}

TEST(SList, Huge) {
  std::vector<ustore::Slice> elements;
  size_t element_size = sizeof(uint32_t);

  for (uint32_t i = 0; i < 1 << 10; i++) {
    char* element = new char[element_size];

    std::memcpy(element, &i, element_size);

    elements.push_back(ustore::Slice(element, element_size));
  }

  ustore::SList slist(elements);
  CheckIdenticalElements(elements, slist.iterator().get());

  // Get
  auto actual_element23 = slist.Get(23);
  EXPECT_EQ(element_size, actual_element23.len());
  EXPECT_EQ(0, std::memcmp(elements[23].data(),
                           actual_element23.data(),
                           element_size));

  // Slice 3 elements after 35th one
  //   Insert element 40 at 35 place
  ustore::SList slist1(slist.Splice(35, 3, {elements[40]}));

  auto new_element35 = slist1.Get(35);
  EXPECT_EQ(element_size, new_element35.len());
  EXPECT_EQ(0, std::memcmp(elements[40].data(),
                           new_element35.data(),
                           element_size));

  auto new_element36 = slist1.Get(36);
  EXPECT_EQ(element_size, new_element35.len());
  EXPECT_EQ(0, std::memcmp(elements[38].data(),
                           new_element36.data(),
                           element_size));

  for (const auto& element : elements) {delete[] element.data(); }
}
