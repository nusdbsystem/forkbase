// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"

#include "types/server/factory.h"
#include "types/server/slist.h"

// Check elements scannbed by iterator are all the same to that in vector
inline void CheckIdenticalElements(
  const std::vector<uint64_t>& expected_idxs,
  const std::vector<ustore::Slice>& expected_elements,
  ustore::CursorIterator* it) {
  for (size_t i = 0; i < expected_elements.size(); i++) {
    auto expected_element = expected_elements[i];
    auto actual_element = it->value();

    ASSERT_EQ(expected_idxs[i], it->index());

    ASSERT_EQ(expected_element.len(), actual_element.len());

    ASSERT_EQ(0, memcmp(expected_element.data(),
                        actual_element.data(),
                        actual_element.len()));
    it->next();
  }
  ASSERT_TRUE(it->end());
}

TEST(SList, Empty) {
  ustore::ChunkableTypeFactory factory;
  std::vector<ustore::Slice> e;
  ustore::SList slist = factory.Create<ustore::SList>(e);

  ASSERT_EQ(size_t(0), slist.numElements());
  ASSERT_TRUE(slist.Get(0).empty());

  const ustore::Slice expected_e1("e1", 2);

  // Insert a new element
  ustore::SList new_slist1 = factory.Load<ustore::SList>(
      slist.Splice(0 , 0, {expected_e1}));
  ASSERT_EQ(size_t(1), new_slist1.numElements());
  ustore::Slice actual_e1 = new_slist1.Get(0);
  EXPECT_TRUE(expected_e1 == actual_e1);

  // Remove the only element
  ustore::SList new_slist2 =
    factory.Load<ustore::SList>(new_slist1.Delete(0, 1));
  ASSERT_EQ(size_t(0), new_slist2.numElements());

  auto it = new_slist2.Scan();
  ASSERT_TRUE(it.end());

  // empty list DIFF non-empty list
  auto diff_it1 = new_slist2.Diff(new_slist1);
  ASSERT_TRUE(diff_it1.end());

  // non-empty list DIFF empty list
  auto diff_it2 = new_slist1.Diff(new_slist2);
  ASSERT_EQ(size_t(0), diff_it2.index());
  ASSERT_TRUE(diff_it2.value() == expected_e1);

  ASSERT_FALSE(diff_it2.next());
  ASSERT_TRUE(diff_it2.end());

  ASSERT_TRUE(diff_it2.previous());
  ASSERT_EQ(size_t(0), diff_it2.index());
  ASSERT_TRUE(diff_it2.value() == expected_e1);

  ASSERT_FALSE(diff_it2.previous());
  ASSERT_TRUE(diff_it2.head());

  // non-empty list INTERSECT empty list
  auto intersect_it = new_slist2.Intersect(new_slist1);
  ASSERT_TRUE(intersect_it.end());

  // non-empty list DUALLYDIFF empty list
  auto ddiff_it = ustore::UList::DuallyDiff(new_slist1, new_slist2);
  ASSERT_EQ(size_t(0), ddiff_it.index());
  ASSERT_TRUE(ddiff_it.lhs_value() == expected_e1);
  ASSERT_TRUE(ddiff_it.rhs_value().empty());

  ASSERT_FALSE(ddiff_it.next());
  ASSERT_TRUE(ddiff_it.end());

  ASSERT_TRUE(ddiff_it.previous());
  ASSERT_EQ(size_t(0), ddiff_it.index());
  ASSERT_TRUE(ddiff_it.lhs_value() == expected_e1);
  ASSERT_TRUE(ddiff_it.rhs_value().empty());

  ASSERT_FALSE(ddiff_it.previous());
  ASSERT_TRUE(ddiff_it.head());
}


TEST(SList, Small) {
  ustore::ChunkableTypeFactory factory;
  const ustore::Slice e1("e1", 2);
  const ustore::Slice e2("e22", 3);
  const ustore::Slice e3("e333", 4);
  const ustore::Slice e4("e4444", 5);
  const ustore::Slice e5("e55555", 6);

  // e3 and e4 to be spliced later
  ustore::SList slist =
    factory.Create<ustore::SList>(std::vector<ustore::Slice>{e1, e2, e5});

  // Get Value by Index
  const ustore::Slice actual_e2 = slist.Get(1);

  EXPECT_EQ(e2.len(), actual_e2.len());
  EXPECT_EQ(0, memcmp(e2.data(), actual_e2.data(), e2.len()));

  // Get Value By index that exceeds total number of elements
  const ustore::Slice nonexist_v5 = slist.Get(5);
  EXPECT_TRUE(nonexist_v5.empty());

  // Test on Iterator
  auto it = slist.Scan();
  CheckIdenticalElements({0, 1, 2}, {e1, e2, e5}, &it);

  // Splice in middle
  ustore::SList new_slist1 =
    factory.Load<ustore::SList>(slist.Splice(1, 1, {e3, e4}));
  auto it1 = new_slist1.Scan();
  CheckIdenticalElements({0, 1, 2, 3},
                         {e1, e3, e4, e5}, &it1);

  // Splice to the end
  ustore::SList new_slist2 =
    factory.Load<ustore::SList>(new_slist1.Splice(3, 2, {e2}));
  auto it2 = new_slist2.Scan();
  CheckIdenticalElements({0, 1, 2, 3},
                         {e1, e3, e4, e2}, &it2);

  auto diff_it = ustore::UList::DuallyDiff(slist, new_slist2);

  ASSERT_EQ(size_t(1), diff_it.index());
  ASSERT_EQ(e2, diff_it.lhs_value());
  ASSERT_EQ(e3, diff_it.rhs_value());

  ASSERT_TRUE(diff_it.next());

  ASSERT_EQ(size_t(2), diff_it.index());
  ASSERT_EQ(e5, diff_it.lhs_value());
  ASSERT_EQ(e4, diff_it.rhs_value());

  ASSERT_TRUE(diff_it.next());

  ASSERT_EQ(size_t(3), diff_it.index());
  ASSERT_TRUE(diff_it.lhs_value().empty());
  ASSERT_EQ(e2, diff_it.rhs_value());

  ASSERT_FALSE(diff_it.next());
  ASSERT_TRUE(diff_it.end());

  // try to advance one more steps
  ASSERT_FALSE(diff_it.next());
  ASSERT_TRUE(diff_it.end());

  // start to retreat
  ASSERT_TRUE(diff_it.previous());

  ASSERT_EQ(size_t(3), diff_it.index());
  ASSERT_TRUE(diff_it.lhs_value().empty());
  ASSERT_EQ(e2, diff_it.rhs_value());

  ASSERT_TRUE(diff_it.previous());

  ASSERT_EQ(size_t(2), diff_it.index());
  ASSERT_EQ(e5, diff_it.lhs_value());
  ASSERT_EQ(e4, diff_it.rhs_value());

  ASSERT_TRUE(diff_it.previous());

  ASSERT_EQ(size_t(1), diff_it.index());
  ASSERT_EQ(e2, diff_it.lhs_value());
  ASSERT_EQ(e3, diff_it.rhs_value());

  ASSERT_FALSE(diff_it.previous());
  ASSERT_TRUE(diff_it.head());

  // try to retreat one more steps
  ASSERT_FALSE(diff_it.previous());
  ASSERT_TRUE(diff_it.head());

  ASSERT_TRUE(diff_it.next());

  ASSERT_EQ(size_t(1), diff_it.index());
  ASSERT_EQ(e2, diff_it.lhs_value());
  ASSERT_EQ(e3, diff_it.rhs_value());

  // test on alternative next() and previous()
  ASSERT_TRUE(diff_it.next());
  ASSERT_TRUE(diff_it.previous());

  ASSERT_EQ(size_t(1), diff_it.index());
  ASSERT_EQ(e2, diff_it.lhs_value());
  ASSERT_EQ(e3, diff_it.rhs_value());

  // test for move ctor
  ustore::SList slist1(std::move(slist));

  EXPECT_EQ(e2.len(), slist1.Get(1).len());
  EXPECT_EQ(0, memcmp(e2.data(), slist1.Get(1).data(), e2.len()));

  // test for move assignment
  ustore::SList slist2 = std::move(slist1);

  EXPECT_EQ(e2.len(), slist2.Get(1).len());
  EXPECT_EQ(0, memcmp(e2.data(), slist2.Get(1).data(), e2.len()));
}

class SListHugeEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    element_size_ = sizeof(uint32_t);

    for (uint32_t i = 0; i < 1 << 10; i++) {
      char* element = new char[element_size_];
      std::memcpy(element, &i, element_size_);
      elements_.push_back(ustore::Slice(element, element_size_));
    }  // end for
  }

  // a utility method to generate a vector of [0, 1 ,2 ... upper - 1]
  std::vector<uint64_t> idxs(uint64_t upper) {
    std::vector<uint64_t> v;
    for (uint64_t i = 0; i < upper; ++i) {
      v.push_back(i);
    }
    return v;
  }

  virtual void TearDown() {
    for (const auto& element : elements_) {
      delete[] element.data();
    }
  }

  std::vector<ustore::Slice> elements_;
  size_t element_size_;
};


TEST_F(SListHugeEnv, Access) {
  ustore::ChunkableTypeFactory factory;
  ustore::SList slist = factory.Create<ustore::SList>(elements_);
  auto it = slist.Scan();
  CheckIdenticalElements(idxs(elements_.size()),
                         elements_, &it);

  // Get
  auto actual_element23 = slist.Get(23);
  EXPECT_EQ(element_size_, actual_element23.len());
  EXPECT_EQ(0, std::memcmp(elements_[23].data(),
                           actual_element23.data(),
                           element_size_));
}

TEST_F(SListHugeEnv, Splice) {
  ustore::ChunkableTypeFactory factory;
  ustore::SList slist = factory.Create<ustore::SList>(elements_);
  // Slice 3 elements after 35th one on slist
  //   Insert element 40 at 35 place
  ustore::SList slist1 =
    factory.Load<ustore::SList>(slist.Splice(35, 3, {elements_[40]}));

  auto new_element35 = slist1.Get(35);
  EXPECT_EQ(element_size_, new_element35.len());
  EXPECT_EQ(0, std::memcmp(elements_[40].data(),
                           new_element35.data(),
                           element_size_));

  auto new_element36 = slist1.Get(36);
  EXPECT_EQ(element_size_, new_element35.len());
  EXPECT_EQ(0, std::memcmp(elements_[38].data(),
                           new_element36.data(),
                           element_size_));

  // Construct the expected slist1
  std::vector<ustore::Slice> expected_slist1;
  expected_slist1.insert(expected_slist1.end(),
                         elements_.begin(),
                         elements_.begin() + 35);

  expected_slist1.push_back(elements_[40]);
  expected_slist1.insert(expected_slist1.end(),
                         elements_.begin() + 38,
                         elements_.end());

  auto it1 = slist1.Scan();
  CheckIdenticalElements(idxs(expected_slist1.size()),
                         expected_slist1, &it1);
}

TEST_F(SListHugeEnv, Remove) {
  ustore::ChunkableTypeFactory factory;
  ustore::SList slist = factory.Create<ustore::SList>(elements_);
  // Remove 10 elements after 50th element on slist
  ustore::SList slist2 = factory.Load<ustore::SList>(slist.Delete(50, 10));

  std::vector<ustore::Slice> expected_slist2;
  expected_slist2.insert(expected_slist2.end(),
                         elements_.begin(),
                         elements_.begin() + 50);

  expected_slist2.insert(expected_slist2.end(),
                         elements_.begin() + 60,
                         elements_.end());

  auto it2 = slist2.Scan();
  CheckIdenticalElements(idxs(expected_slist2.size()),
                         expected_slist2, &it2);
}

TEST_F(SListHugeEnv, Insert) {
  ustore::ChunkableTypeFactory factory;
  ustore::SList slist = factory.Create<ustore::SList>(elements_);

  // Insert e[100] to e[119] (20 elements) at 500th position
  std::vector<ustore::Slice> inserted_elements;
  inserted_elements.insert(inserted_elements.end(),
                           elements_.begin() + 100,
                           elements_.begin() + 120);

  ustore::SList slist3 =
    factory.Load<ustore::SList>(slist.Insert(500, inserted_elements));

  std::vector<ustore::Slice> expected_slist3;
  expected_slist3.insert(expected_slist3.end(),
                         elements_.begin(),
                         elements_.begin() + 500);

  expected_slist3.insert(expected_slist3.end(),
                         inserted_elements.begin(),
                         inserted_elements.end());

  expected_slist3.insert(expected_slist3.end(),
                         elements_.begin() + 500,
                         elements_.end());

  auto it3 = slist3.Scan();
  CheckIdenticalElements(idxs(expected_slist3.size()),
                         expected_slist3, &it3);
}

TEST_F(SListHugeEnv, Append) {
  ustore::ChunkableTypeFactory factory;
  ustore::SList slist = factory.Create<ustore::SList>(elements_);

  // Append e[500] to e[599] (20 elements) at list end
  std::vector<ustore::Slice> appended_elements;
  appended_elements.insert(appended_elements.end(),
                           elements_.begin() + 500,
                           elements_.begin() + 600);

  ustore::SList slist4 =
    factory.Load<ustore::SList>(slist.Append(appended_elements));

  std::vector<ustore::Slice> expected_slist4;
  expected_slist4.insert(expected_slist4.end(),
                         elements_.begin(),
                         elements_.end());

  expected_slist4.insert(expected_slist4.end(),
                         appended_elements.begin(),
                         appended_elements.end());

  auto it4 = slist4.Scan();
  CheckIdenticalElements(idxs(expected_slist4.size()),
                         expected_slist4, &it4);
}

TEST_F(SListHugeEnv, Compare) {
  /* rhs elements are constructed as follows:
  index:     0 ---------- 00    100 ------ 199  200 -- (end-100) --- (end-99) -- (end-89)
  element:   e[0] ------e[99] e[200] --- e[299] e[200] -- e[end-100] -- e[0] -- e[10]
  */
  ustore::ChunkableTypeFactory factory;

  std::vector<ustore::Slice> rhs_elements;
  rhs_elements.insert(rhs_elements.end(),
                      elements_.begin(),
                      elements_.begin() + 100);

  rhs_elements.insert(rhs_elements.end(),
                      elements_.begin() + 200,
                      elements_.begin() + 300);

  rhs_elements.insert(rhs_elements.end(),
                      elements_.begin() + 200,
                      elements_.begin() + 300);

  rhs_elements.insert(rhs_elements.end(),
                      elements_.begin() + 300,
                      elements_.end() - 100);

  rhs_elements.insert(rhs_elements.end(),
                      elements_.begin(),
                      elements_.begin() + 10);

  ustore::SList lhs = factory.Create<ustore::SList>(elements_);
  ustore::SList rhs = factory.Create<ustore::SList>(rhs_elements);

// For diff operation
  std::vector<uint64_t> diff_idx;
  for (uint64_t i = 100; i < 200; ++i) {
    diff_idx.push_back(i);
  }

  for (uint64_t i = elements_.size() - 100; i < elements_.size(); ++i) {
    diff_idx.push_back(i);
  }

  std::vector<ustore::Slice> diff_elements;

  diff_elements.insert(diff_elements.end(),
                       elements_.begin() + 100,
                       elements_.begin() + 200);

  diff_elements.insert(diff_elements.end(),
                       elements_.end() - 100,
                       elements_.end());

  auto diff_it = lhs.Diff(rhs);
  CheckIdenticalElements(diff_idx, diff_elements, &diff_it);

// For Intersect operation
  std::vector<uint64_t> intersect_idx;
  for (uint64_t i = 0; i < 100; ++i) {
    intersect_idx.push_back(i);
  }

  for (uint64_t i = 200; i < elements_.size() - 100; ++i) {
    intersect_idx.push_back(i);
  }

  std::vector<ustore::Slice> intersect_elements;

  intersect_elements.insert(intersect_elements.end(),
                            elements_.begin(),
                            elements_.begin() + 100);

  intersect_elements.insert(intersect_elements.end(),
                            elements_.begin() + 200,
                            elements_.end() - 100);

  auto intersect_it = lhs.Intersect(rhs);
  CheckIdenticalElements(intersect_idx, intersect_elements,
                         &intersect_it);
}

TEST_F(SListHugeEnv, Merge) {
 /*
 Node 1 is constructed by
   removing 20 and inserting 30 elements at 120th from base,
   replacing 50 elements at 160th of Node 1 and
   removing 50 elements at 500th of Node 1.

 Node 1:  0 ... 50 ... 100 . 120 . 150 . 160 ... 210 ................. 910   910 . 984
          | ----------------- |     | --- |       | ------------------- |     | --- |
 Base:    0 ... 50 ... 100 . 120 . 140 . 150 ... 200 ... 550     550 . 900 . 950 . 1024
          | --- |       |  -------------  |       |  ---  |       |  -------------  |
 Node 2:  0 ... 50      50 ............. 100 ... 150 ... 500 ... 550 ............. 1024

 Node 2 is constructed by
   removing 50 elements at 50th from base,
   replacing 50 elements at 100th of Node 2 and
   inserting 50 elements at 500th of Node 2.

NOTE: Two replacing contents on both nodes are identical.
 */
  ustore::ChunkableTypeFactory factory;
  ustore::SList base = factory.Create<ustore::SList>(elements_);

  std::vector<ustore::Slice> node1_insertion;
  for (uint32_t i = 1024; i < 1054; i++) {
    char* element = new char[element_size_];
    std::memcpy(element, &i, element_size_);
    node1_insertion.push_back(ustore::Slice(element, element_size_));
  }  // end for

  std::vector<ustore::Slice> node2_insertion;
  for (uint32_t i = 1054; i < 1104; i++) {
    char* element = new char[element_size_];
    std::memcpy(element, &i, element_size_);
    node2_insertion.push_back(ustore::Slice(element, element_size_));
  }  // end for

  std::vector<ustore::Slice> replacement;
  for (uint32_t i = 1104; i < 1154; i++) {
    char* element = new char[element_size_];
    std::memcpy(element, &i, element_size_);
    replacement.push_back(ustore::Slice(element, element_size_));
  }  // end for


  std::vector<ustore::Slice> node1_elements;
  node1_elements.insert(node1_elements.end(),
                        elements_.begin(),
                        elements_.begin() + 120);

  node1_elements.insert(node1_elements.end(),
                        node1_insertion.begin(),
                        node1_insertion.end());

  node1_elements.insert(node1_elements.end(),
                        elements_.begin() + 140,
                        elements_.begin() + 150);

  node1_elements.insert(node1_elements.end(),
                        replacement.begin(),
                        replacement.end());

  node1_elements.insert(node1_elements.end(),
                        elements_.begin() + 200,
                        elements_.begin() + 900);

  node1_elements.insert(node1_elements.end(),
                        elements_.begin() + 950,
                        elements_.end());

  ASSERT_EQ(size_t(984), node1_elements.size());
  ustore::SList node1 = factory.Create<ustore::SList>(node1_elements);


  std::vector<ustore::Slice> node2_elements;
  node2_elements.insert(node2_elements.end(),
                        elements_.begin(),
                        elements_.begin() + 50);

  node2_elements.insert(node2_elements.end(),
                        elements_.begin() + 100,
                        elements_.begin() + 150);

  node2_elements.insert(node2_elements.end(),
                        replacement.begin(),
                        replacement.end());

  node2_elements.insert(node2_elements.end(),
                        elements_.begin() + 200,
                        elements_.begin() + 550);

  node2_elements.insert(node2_elements.end(),
                        node2_insertion.begin(),
                        node2_insertion.end());

  node2_elements.insert(node2_elements.end(),
                        elements_.begin() + 550,
                        elements_.end());

  ASSERT_EQ(size_t(1024), node2_elements.size());
  ustore::SList node2 = factory.Create<ustore::SList>(node2_elements);


  std::vector<ustore::Slice> merged_elements;
  merged_elements.insert(merged_elements.end(),
                         elements_.begin(),
                         elements_.begin() + 50);

  merged_elements.insert(merged_elements.end(),
                         elements_.begin() + 100,
                         elements_.begin() + 120);

  merged_elements.insert(merged_elements.end(),
                         node1_insertion.begin(),
                         node1_insertion.end());

  merged_elements.insert(merged_elements.end(),
                         elements_.begin() + 140,
                         elements_.begin() + 150);

  merged_elements.insert(merged_elements.end(),
                         replacement.begin(),
                         replacement.end());

  merged_elements.insert(merged_elements.end(),
                         elements_.begin() + 200,
                         elements_.begin() + 550);

  merged_elements.insert(merged_elements.end(),
                         node2_insertion.begin(),
                         node2_insertion.end());

  merged_elements.insert(merged_elements.end(),
                         elements_.begin() + 550,
                         elements_.begin() + 900);

  merged_elements.insert(merged_elements.end(),
                         elements_.begin() + 950,
                         elements_.begin() + 1024);

  ustore::SList expected_merge = factory.Create<ustore::SList>(merged_elements);


  ustore::SList actual_merge =
      factory.Load<ustore::SList>(base.Merge(node1, node2));

  ASSERT_TRUE(expected_merge.hash() == actual_merge.hash());

  for (const auto& ele : node1_insertion) delete[] ele.data();
  for (const auto& ele : node2_insertion) delete[] ele.data();
  for (const auto& ele : replacement) delete[] ele.data();
}

TEST(SListMerge, Conflict) {
  ustore::ChunkableTypeFactory factory;
  const ustore::Slice a("a", 1);
  const ustore::Slice b("b", 1);
  const ustore::Slice c("c", 1);
  const ustore::Slice d("d", 1);
  const ustore::Slice e("e", 1);

  const ustore::Slice B("B", 1);
  const ustore::Slice C("C", 1);

  // Base List: a b c d e
  ustore::SList base = factory.Create<ustore::SList>(
      std::vector<ustore::Slice>{a, b, c, d, e});

  // Node 1 is constructed by
  //   removing b,
  //   replacing c with C,
  ustore::SList node1 = factory.Create<ustore::SList>(
      std::vector<ustore::Slice>{a, C, d, e});

  // Node 2 is constructed by
  //   removing a,
  //   replacing b with B
  ustore::SList node2 = factory.Create<ustore::SList>(
      std::vector<ustore::Slice>{B, c, d, e});

  ustore::Hash merge_hash = base.Merge(node1, node2);
  ASSERT_TRUE(merge_hash.empty());
}


TEST(SListMerge, Small) {
  ustore::ChunkableTypeFactory factory;
  const ustore::Slice a("a", 1);
  const ustore::Slice b("b", 1);
  const ustore::Slice c("c", 1);
  const ustore::Slice d("d", 1);
  const ustore::Slice e("e", 1);
  const ustore::Slice f("f", 1);
  const ustore::Slice g("g", 1);
  const ustore::Slice h("h", 1);

  const ustore::Slice C("C", 1);
  const ustore::Slice E("E", 1);
  const ustore::Slice G("G", 1);
  const ustore::Slice H("H", 1);

  // Base List: a b c d e f g h
  ustore::SList base = factory.Create<ustore::SList>(
      std::vector<ustore::Slice>{a, b, c, d, e, f, g, h});

  // Node 1 is constructed by
  //   removing b,
  //   replacing c with C,
  //   inserting G before g
  ustore::SList node1 = factory.Create<ustore::SList>(
      std::vector<ustore::Slice>{a, C, d, e, f, G, g, h});

  // Node 2 is constructed by
  //   removing a,
  //   replacing e with E,
  //   inserting H after h.
  ustore::SList node2 = factory.Create<ustore::SList>(
      std::vector<ustore::Slice>{b, c, d, E, f, g, h, H});

  // Expected Merge Node:
  ustore::SList expected_merge = factory.Create<ustore::SList>(
      std::vector<ustore::Slice>{C, d, E, f, G, g, h, H});

  ustore::SList actual_merge =
      factory.Load<ustore::SList>(base.Merge(node1, node2));

  ASSERT_TRUE(expected_merge.hash() == actual_merge.hash());
}
