// Copyright (c) 2017 The Ustore Authors.

#include <cstring>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "types/server/factory.h"
#include "types/server/sset.h"

// Check KVItems scannbed by iterator are all the same to that in vector
inline void CheckIdenticalItems(
  const std::vector<ustore::Slice>& keys,
  ustore::CursorIterator* it) {
  for (size_t i = 0; i < keys.size(); i++) {
    auto expected_key = keys[i];

    auto actual_key = it->key();

    ASSERT_EQ(expected_key.len(), actual_key.len());

    ASSERT_EQ(0, memcmp(expected_key.data(),
                        actual_key.data(),
                        actual_key.len()));

    it->next();
  }
  ASSERT_TRUE(it->end());
}

TEST(SSet, Empty) {
  ustore::ChunkableTypeFactory factory;
  ustore::SSet sset = factory.Create<ustore::SSet>(
      std::vector<ustore::Slice>{});
  ASSERT_EQ(size_t(0), sset.numElements());

  const ustore::Slice k1("k1", 2);

  // Get an non-existent key from an empty set
  ASSERT_TRUE(sset.Get(k1).empty());

  // Set a kv pair
  ustore::SSet new_sset1 =
    factory.Load<ustore::SSet>(sset.Set(k1));
  ASSERT_EQ(size_t(1), new_sset1.numElements());

  // remove a kv pair to result an empty set
  ustore::SSet new_sset2 = factory.Load<ustore::SSet>(new_sset1.Remove(k1));
  ASSERT_TRUE(new_sset2.Get(k1).empty());
  ASSERT_EQ(size_t(0), new_sset2.numElements());

  auto it = new_sset2.Scan();
  ASSERT_TRUE(it.end());

  // empty set DIFF non-empty set
  auto diff_it1 = new_sset2.Diff(new_sset1);
  ASSERT_TRUE(diff_it1.end());

  // non-empty set DIFF empty set
  auto diff_it2 = new_sset1.Diff(new_sset2);
  ASSERT_TRUE(diff_it2.key() == k1);

  ASSERT_FALSE(diff_it2.next());
  ASSERT_TRUE(diff_it2.end());

  ASSERT_TRUE(diff_it2.previous());
  ASSERT_TRUE(diff_it2.key() == k1);

  ASSERT_FALSE(diff_it2.previous());
  ASSERT_TRUE(diff_it2.head());

  // non-empty set INTERSECT empty set
  auto intersect_it = new_sset2.Intersect(new_sset1);
  ASSERT_TRUE(intersect_it.end());

  // non-empty set DUALLYDIFF empty set
  auto ddiff_it = ustore::USet::DuallyDiff(new_sset1, new_sset2);
  ASSERT_TRUE(ddiff_it.key() == k1);
  // ASSERT_TRUE(ddiff_it.rhs_value().empty());

  ASSERT_FALSE(ddiff_it.next());
  ASSERT_TRUE(ddiff_it.end());

  ASSERT_TRUE(ddiff_it.previous());
  ASSERT_TRUE(ddiff_it.key() == k1);
  // ASSERT_TRUE(ddiff_it.rhs_value().empty());

  ASSERT_FALSE(ddiff_it.previous());
  ASSERT_TRUE(ddiff_it.head());
}

TEST(SSet, Small) {
  ustore::ChunkableTypeFactory factory;
  const ustore::Slice k1("k1", 2);
  const ustore::Slice k2("k22", 3);
  const ustore::Slice k3("k333", 4);

  // A new key to put
  const ustore::Slice k4("k4444", 5);

  // Internally, key slices will be sorted in ascending order
  ustore::SSet sset = factory.Create<ustore::SSet>(
      std::vector<ustore::Slice>{k1, k3, k2});

  // Test on Iterator
  auto it = sset.Scan();
  CheckIdenticalItems({k1, k2, k3}, &it);
  EXPECT_TRUE(it.end());

  // Set with an non-existent key
  ustore::SSet new_sset1 = factory.Load<ustore::SSet>(sset.Set(k4));

  auto it1 = new_sset1.Scan();
  CheckIdenticalItems({k1, k2, k3, k4}, &it1);

  // Set with an existent key
  // Set v3 with v4
  ustore::SSet new_sset2 = factory.Load<ustore::SSet>(new_sset1.Set(k3));

  auto it2 = new_sset2.Scan();
  CheckIdenticalItems({k1, k2, k3, k4}, &it2);

  // Remove an existent key
  ustore::SSet new_sset3 = factory.Load<ustore::SSet>(new_sset2.Remove(k1));
  auto it3 = new_sset3.Scan();
  CheckIdenticalItems({k2, k3, k4}, &it3);

  // Remove an non-existent key
  ustore::SSet new_sset4 = factory.Load<ustore::SSet>(sset.Remove(k4));
  auto it4 = new_sset4.Scan();
  CheckIdenticalItems({k1, k2, k3}, &it4);

  // test for move ctor
  ustore::SSet new_sset4_1(std::move(new_sset4));
  auto it4_1 = new_sset4_1.Scan();
  CheckIdenticalItems({k1, k2, k3}, &it4_1);

  // test for move assignment
  ustore::SSet new_sset4_2 = std::move(new_sset4_1);
  auto it4_2 = new_sset4_2.Scan();
  CheckIdenticalItems({k1, k2, k3}, &it4_2);
}

class SsetHugeEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    entry_size_ = 2 * sizeof(uint32_t);
    for (uint32_t i = 0; i < 1 << 6; i++) {
      for (uint32_t j = 0; j < 1 << 8; j++) {
        char* key = new char[2 * sizeof(uint32_t)];
        std::memcpy(key, &i, sizeof(uint32_t));
        std::memcpy(key + sizeof(uint32_t), &j, sizeof(uint32_t));

        keys_.push_back(ustore::Slice(key, 2 * sizeof(uint32_t)));
      }
    }
  }

  virtual void TearDown() {
    for (const auto& key : keys_) {delete[] key.data(); }
  }

  std::vector<ustore::Slice> keys_;
  size_t entry_size_;
};

TEST_F(SsetHugeEnv, Basic) {
  ustore::ChunkableTypeFactory factory;
  ustore::SSet sset = factory.Create<ustore::SSet>(keys_);
  auto it = sset.Scan();
  CheckIdenticalItems(keys_, &it);

  // Get using key[23]
  auto actual_val23 = sset.Get(keys_[23]);
  EXPECT_EQ(entry_size_, actual_val23.len());

  // Remove key[35]
  ustore::SSet sset1 = factory.Load<ustore::SSet>(sset.Remove(keys_[35]));
  keys_.erase(keys_.begin() + 35);
  auto it1 = sset1.Scan();
  CheckIdenticalItems(keys_, &it1);

  // Set the value of key55 with val56
  ustore::SSet sset2 =
    factory.Load<ustore::SSet>(sset.Set(keys_[55]));

  auto actual_val55 = sset.Get(keys_[55]);
  EXPECT_EQ(entry_size_, actual_val55.len());
}

TEST_F(SsetHugeEnv, Compare) {
  ustore::ChunkableTypeFactory factory;
  ustore::SSet lhs = factory.Create<ustore::SSet>(keys_);

  /* rhs set is constructed from lhs by removing
  * k[100] to k[199]
  * Set value of k[200] with v[201], k[201] with v[202] until k[299] to v[300]
  * Insert new (2<<8) kv items
  */
  ustore::Hash rhs_hash = lhs.hash();
  for (uint32_t i = 100; i < 200; ++i) {
    ustore::SSet rhs = factory.Load<ustore::SSet>(rhs_hash);
    rhs_hash = rhs.Remove(keys_[i]).Clone();
  }

  for (uint32_t i = 200; i < 300; ++i) {
    ustore::SSet rhs = factory.Load<ustore::SSet>(rhs_hash);
    rhs_hash = rhs.Set(keys_[i]).Clone();
  }

  std::vector<ustore::Slice> new_keys;

  uint32_t i = 1 << 6;
  for (uint32_t j = 0; j < 1 << 8; j++) {
    char* key = new char[2 * sizeof(uint32_t)];
    std::memcpy(key, &i, sizeof(uint32_t));
    std::memcpy(key + sizeof(uint32_t), &j, sizeof(uint32_t));

    ustore::Slice k(key, entry_size_);

    ustore::SSet rhs = factory.Load<ustore::SSet>(rhs_hash);
    rhs_hash = rhs.Set(k);

    new_keys.push_back(k);
  }

  ustore::SSet rhs = factory.Load<ustore::SSet>(rhs_hash);

// Check for rhs correctness
  std::vector<ustore::Slice> expected_rhs_keys;

  expected_rhs_keys.insert(expected_rhs_keys.end(),
                           keys_.begin(),
                           keys_.begin() + 100);

  expected_rhs_keys.insert(expected_rhs_keys.end(),
                           keys_.begin() + 200,
                           keys_.end());

  expected_rhs_keys.insert(expected_rhs_keys.end(),
                           new_keys.begin(),
                           new_keys.end());

  auto rhs_it = rhs.Scan();
  CheckIdenticalItems(expected_rhs_keys, &rhs_it);


  // Diff
  std::vector<ustore::Slice> expected_diff_keys;

  expected_diff_keys.insert(expected_diff_keys.end(),
                            keys_.begin() + 100,
                            keys_.begin() + 300);

  auto diff_it = lhs.Diff(rhs);

  // Intersect
  std::vector<ustore::Slice> expected_intersect_keys;

  expected_intersect_keys.insert(expected_intersect_keys.end(),
                                 keys_.begin(),
                                 keys_.begin() + 100);

  expected_intersect_keys.insert(expected_intersect_keys.end(),
                                 keys_.begin() + 300,
                                 keys_.end());


  auto intersect_it = lhs.Intersect(rhs);

  for (const auto& key : new_keys) {delete[] key.data(); }
}

TEST(SSetMerge, SmallNormal) {
  const ustore::Slice k0("k0", 2);

  const ustore::Slice k11("k11", 3);
  const ustore::Slice k1("k1", 2);

  const ustore::Slice k2("k2", 2);

  const ustore::Slice k3("k3", 2);
  const ustore::Slice k33("k33", 3);

  const ustore::Slice k4("k4", 2);

  const ustore::Slice k5("k5", 2);

  const ustore::Slice k6("k6", 2);

  const ustore::Slice k7("k7", 2);

  const ustore::Slice k8("k8", 2);

  ustore::ChunkableTypeFactory factory;
  // Base Set: k0, k1, k2, k3, k5, k7
  ustore::SSet base = factory.Create<ustore::SSet>(
      std::vector<ustore::Slice>{k0, k1, k2, k3, k5, k7});

  // Remove k0, edit k1 add k4, add k6
  // Node 1 Set: k1', k2, k3, k4, k5, k6, k7
  ustore::SSet node1 = factory.Create<ustore::SSet>
      (std::vector<ustore::Slice>{k11, k2, k3, k4, k5, k6, k7});

  // Edit k3, add k6, add k8
  // Node 2 Set: k0, k1, k2, k3', k5, k6, k7, k8
  ustore::SSet node2 = factory.Create<ustore::SSet>
      (std::vector<ustore::Slice>{k0, k1, k2, k33, k5, k6, k7, k8});

  // Result Set: k1', k2, k3' k4, k5, k6, k7, k8
  std::vector<ustore::Slice> merged_keys{k11, k2, k33, k4, k5, k6, k7, k8};

  ustore::SSet expected_merge =
      factory.Create<ustore::SSet>(merged_keys);

  ustore::SSet actual_merge =
      factory.Load<ustore::SSet>(base.Merge(node1, node2));

  auto it = actual_merge.Scan();
  CheckIdenticalItems(merged_keys, &it);

  ASSERT_TRUE(expected_merge.hash() == actual_merge.hash());
}

TEST(SSetMerge, SmallConflict) {
  ustore::ChunkableTypeFactory factory;
  const ustore::Slice k00("k00", 3);
  const ustore::Slice k0("k0", 2);

  const ustore::Slice k11("k11", 3);
  const ustore::Slice k1("k1", 2);

  const ustore::Slice k22("k22", 3);
  const ustore::Slice k2("k2", 2);

  ustore::SSet base = factory.Create<ustore::SSet>
      (std::vector<ustore::Slice>{k0, k1, k2});

  ustore::SSet node1 = factory.Create<ustore::SSet>
      (std::vector<ustore::Slice>{k00, k11, k2});

  ustore::SSet node2 = factory.Create<ustore::SSet>
      (std::vector<ustore::Slice>{k0, k11, k22});

  ustore::Hash result = base.Merge(node1, node2);
  ASSERT_TRUE(result.empty());
}
