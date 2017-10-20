// Copyright (c) 2017 The Ustore Authors.

#include <cstring>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "types/server/factory.h"
#include "types/server/smap.h"

inline void CheckIdenticalItems(
  const std::vector<ustore::Slice>& keys,
  const std::vector<ustore::Slice>& vals,
  ustore::CursorIterator* it) {
  for (size_t i = 0; i < keys.size(); i++) {
    auto expected_key = keys[i];
    auto expected_val = vals[i];

    auto actual_key = it->key();
    auto actual_val = it->value();

    ASSERT_EQ(expected_key.len(), actual_key.len());
    ASSERT_EQ(expected_val.len(), actual_val.len());

    ASSERT_EQ(0, memcmp(expected_key.data(),
                        actual_key.data(),
                        actual_key.len()));

    ASSERT_EQ(0, memcmp(expected_val.data(),
                        actual_val.data(),
                        actual_val.len()))
      << " Expected Val: " << expected_val.ToString()
      << " Actual Val: " << actual_val.ToString();

    it->next();
  }
  ASSERT_TRUE(it->end());
}

TEST(SMap, Empty) {
  ustore::ChunkableTypeFactory factory;
  ustore::SMap smap = factory.Create<ustore::SMap>(
      std::vector<ustore::Slice>{}, std::vector<ustore::Slice>{});
  ASSERT_EQ(size_t(0), smap.numElements());

  const ustore::Slice k1("k1", 2);
  const ustore::Slice expected_v1("v1", 2);

  // Get an non-existent key from an empty map
  ASSERT_TRUE(smap.Get(k1).empty());

  // Set a kv pair
  ustore::SMap new_smap1 =
    factory.Load<ustore::SMap>(smap.Set(k1, expected_v1));
  ASSERT_EQ(size_t(1), new_smap1.numElements());

  ustore::Slice actual_v1 = new_smap1.Get(k1);
  ASSERT_TRUE(expected_v1 == actual_v1);

  // remove a kv pair to result an empty map
  ustore::SMap new_smap2 = factory.Load<ustore::SMap>(new_smap1.Remove(k1));
  ASSERT_TRUE(new_smap2.Get(k1).empty());
  ASSERT_EQ(size_t(0), new_smap2.numElements());

  auto it = new_smap2.Scan();
  ASSERT_TRUE(it.end());

  // empty map DIFF non-empty map
  auto diff_it1 = new_smap2.Diff(new_smap1);
  ASSERT_TRUE(diff_it1.end());

  // non-empty map DIFF empty map
  auto diff_it2 = new_smap1.Diff(new_smap2);
  ASSERT_TRUE(diff_it2.key() == k1);
  ASSERT_TRUE(diff_it2.value() == expected_v1);

  ASSERT_FALSE(diff_it2.next());
  ASSERT_TRUE(diff_it2.end());

  ASSERT_TRUE(diff_it2.previous());
  ASSERT_TRUE(diff_it2.key() == k1);
  ASSERT_TRUE(diff_it2.value() == expected_v1);

  ASSERT_FALSE(diff_it2.previous());
  ASSERT_TRUE(diff_it2.head());

  // non-empty map INTERSECT empty map
  auto intersect_it = new_smap2.Intersect(new_smap1);
  ASSERT_TRUE(intersect_it.end());

  // non-empty map DUALLYDIFF empty map
  auto ddiff_it = ustore::UMap::DuallyDiff(new_smap1, new_smap2);
  ASSERT_TRUE(ddiff_it.key() == k1);
  ASSERT_TRUE(ddiff_it.lhs_value() == expected_v1);
  ASSERT_TRUE(ddiff_it.rhs_value().empty());

  ASSERT_FALSE(ddiff_it.next());
  ASSERT_TRUE(ddiff_it.end());

  ASSERT_TRUE(ddiff_it.previous());
  ASSERT_TRUE(ddiff_it.key() == k1);
  ASSERT_TRUE(ddiff_it.lhs_value() == expected_v1);
  ASSERT_TRUE(ddiff_it.rhs_value().empty());

  ASSERT_FALSE(ddiff_it.previous());
  ASSERT_TRUE(ddiff_it.head());
}

TEST(SMap, Small) {
  ustore::ChunkableTypeFactory factory;
  const ustore::Slice k1("k1", 2);
  const ustore::Slice v1("v1", 2);
  const ustore::Slice k2("k22", 3);
  const ustore::Slice v2("v22", 3);
  const ustore::Slice k3("k333", 4);
  const ustore::Slice v3("v333", 4);

  // A new key to put
  const ustore::Slice k4("k4444", 5);
  const ustore::Slice v4("v4444", 5);

  // Internally, key slices will be sorted in ascending order
  ustore::SMap smap = factory.Create<ustore::SMap>(
      std::vector<ustore::Slice>{k1, k3, k2},
      std::vector<ustore::Slice>{v1, v3, v2});

  // Get Value by Key
  const ustore::Slice actual_v1 = smap.Get(k1);

  EXPECT_EQ(size_t(2), v1.len());
  EXPECT_EQ(0, memcmp(v1.data(), actual_v1.data(), 2));

  // Get Value by Non-existent Key
  const ustore::Slice actual_v4 = smap.Get(k4);
  EXPECT_TRUE(actual_v4.empty());

  // Test on Iterator
  auto it = smap.Scan();
  CheckIdenticalItems({k1, k2, k3}, {v1, v2, v3}, &it);
  EXPECT_TRUE(it.end());

  // Set with an non-existent key
  ustore::SMap new_smap1 = factory.Load<ustore::SMap>(smap.Set(k4, v4));
  EXPECT_EQ(v4.len(), new_smap1.Get(k4).len());
  EXPECT_EQ(0,
            std::memcmp(v4.data(),
                        new_smap1.Get(k4).data(),
                        v4.len()));

  auto it1 = new_smap1.Scan();
  CheckIdenticalItems({k1, k2, k3, k4}, {v1, v2, v3, v4},
                      &it1);

  // Set with an existent key
  // Set v3 with v4
  ustore::SMap new_smap2 = factory.Load<ustore::SMap>(new_smap1.Set(k3, v4));
  EXPECT_EQ(v4.len(), new_smap2.Get(k3).len());
  EXPECT_EQ(0,
            std::memcmp(v4.data(),
                        new_smap2.Get(k3).data(),
                        v4.len()));

  auto it2 = new_smap2.Scan();
  CheckIdenticalItems({k1, k2, k3, k4}, {v1, v2, v4, v4},
                      &it2);

  // Remove an existent key
  ustore::SMap new_smap3 = factory.Load<ustore::SMap>(new_smap2.Remove(k1));
  auto it3 = new_smap3.Scan();
  CheckIdenticalItems({k2, k3, k4}, {v2, v4, v4},
                      &it3);

  // Remove an non-existent key
  ustore::SMap new_smap4 = factory.Load<ustore::SMap>(smap.Remove(k4));
  auto it4 = new_smap4.Scan();
  CheckIdenticalItems({k1, k2, k3}, {v1, v2, v3},
                      &it4);

  // test for move ctor
  ustore::SMap new_smap4_1(std::move(new_smap4));
  auto it4_1 = new_smap4_1.Scan();
  CheckIdenticalItems({k1, k2, k3}, {v1, v2, v3},
                      &it4_1);

  // test for move assignment
  ustore::SMap new_smap4_2 = std::move(new_smap4_1);
  auto it4_2 = new_smap4_2.Scan();
  CheckIdenticalItems({k1, k2, k3}, {v1, v2, v3},
                      &it4_2);


  // Use new_smap3 with smap to perform duallydiff
  // lhs: k1->v1, k2->v2, k3->v3
  // rhs:         k2->v2, k3->v4, k4->v4
  auto dually_diff_it = ustore::UMap::DuallyDiff(smap, new_smap3);

  ASSERT_EQ(k1, dually_diff_it.key());
  ASSERT_EQ(v1, dually_diff_it.lhs_value());
  ASSERT_TRUE(dually_diff_it.rhs_value().empty());

  ASSERT_TRUE(dually_diff_it.next());

  ASSERT_EQ(k3, dually_diff_it.key());
  ASSERT_EQ(v3, dually_diff_it.lhs_value());
  ASSERT_EQ(v4, dually_diff_it.rhs_value());

  ASSERT_TRUE(dually_diff_it.next());

  ASSERT_EQ(k4, dually_diff_it.key());
  ASSERT_TRUE(dually_diff_it.lhs_value().empty());
  ASSERT_EQ(v4, dually_diff_it.rhs_value());

  ASSERT_FALSE(dually_diff_it.next());
  ASSERT_TRUE(dually_diff_it.end());

  // ensure can not advance using next since it is end already
  ASSERT_FALSE(dually_diff_it.next());
  ASSERT_TRUE(dually_diff_it.end());

  // start to retreat
  ASSERT_TRUE(dually_diff_it.previous());

  ASSERT_EQ(k4, dually_diff_it.key());
  ASSERT_TRUE(dually_diff_it.lhs_value().empty());
  ASSERT_EQ(v4, dually_diff_it.rhs_value());

  ASSERT_TRUE(dually_diff_it.previous());

  ASSERT_EQ(k3, dually_diff_it.key());
  ASSERT_EQ(v3, dually_diff_it.lhs_value());
  ASSERT_EQ(v4, dually_diff_it.rhs_value());

  ASSERT_TRUE(dually_diff_it.previous());

  ASSERT_EQ(k1, dually_diff_it.key());
  ASSERT_EQ(v1, dually_diff_it.lhs_value());
  ASSERT_TRUE(dually_diff_it.rhs_value().empty());

  ASSERT_FALSE(dually_diff_it.previous());
  ASSERT_TRUE(dually_diff_it.head());

  // ensure can not retreat using previous since it is at head already
  ASSERT_FALSE(dually_diff_it.previous());
  ASSERT_TRUE(dually_diff_it.head());

  // Test on altenative advance and retreat
  ASSERT_TRUE(dually_diff_it.next());
  ASSERT_FALSE(dually_diff_it.previous());
  ASSERT_TRUE(dually_diff_it.head());

  ASSERT_TRUE(dually_diff_it.next());
  ASSERT_TRUE(dually_diff_it.next());
  ASSERT_TRUE(dually_diff_it.previous());

  ASSERT_EQ(k1, dually_diff_it.key());
  ASSERT_EQ(v1, dually_diff_it.lhs_value());
  ASSERT_TRUE(dually_diff_it.rhs_value().empty());
}

class SMapHugeEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    entry_size_ = 2 * sizeof(uint32_t);
    for (uint32_t i = 0; i < 1 << 6; i++) {
      for (uint32_t j = 0; j < 1 << 8; j++) {
        char* key = new char[2 * sizeof(uint32_t)];
        std::memcpy(key, &i, sizeof(uint32_t));
        std::memcpy(key + sizeof(uint32_t), &j, sizeof(uint32_t));

        char* val = new char[2 * sizeof(uint32_t)];
        std::memcpy(val, &i, sizeof(uint32_t));
        std::memcpy(val + sizeof(uint32_t), &j, sizeof(uint32_t));

        keys_.push_back(ustore::Slice(key, 2 * sizeof(uint32_t)));
        vals_.push_back(ustore::Slice(val, 2 * sizeof(uint32_t)));
      }
    }
  }

  virtual void TearDown() {
    for (const auto& key : keys_) {delete[] key.data(); }
    for (const auto& val : vals_) {delete[] val.data(); }
  }

  std::vector<ustore::Slice> keys_;
  std::vector<ustore::Slice> vals_;
  size_t entry_size_;
};

TEST_F(SMapHugeEnv, Basic) {
  ustore::ChunkableTypeFactory factory;
  ustore::SMap smap = factory.Create<ustore::SMap>(keys_, vals_);
  auto it = smap.Scan();
  CheckIdenticalItems(keys_, vals_, &it);

  // Get using key[23]
  auto actual_val23 = smap.Get(keys_[23]);
  EXPECT_EQ(entry_size_, actual_val23.len());
  EXPECT_EQ(0, std::memcmp(vals_[23].data(), actual_val23.data(), entry_size_));

  // Remove key[35]
  ustore::SMap smap1 = factory.Load<ustore::SMap>(smap.Remove(keys_[35]));
  keys_.erase(keys_.begin() + 35);
  vals_.erase(vals_.begin() + 35);
  auto it1 = smap1.Scan();
  CheckIdenticalItems(keys_, vals_, &it1);

  // Set the value of key55 with val56
  ustore::SMap smap2 =
    factory.Load<ustore::SMap>(smap.Set(keys_[55], vals_[56]));

  auto actual_val55 = smap.Get(keys_[55]);
  EXPECT_EQ(entry_size_, actual_val55.len());
  EXPECT_EQ(0, std::memcmp(vals_[55].data(), actual_val55.data(), entry_size_));
}

TEST_F(SMapHugeEnv, Multiset) {
  // Fist Init with a single kv
  ustore::ChunkableTypeFactory factory;

  std::vector<ustore::Slice> init_keys{keys_[0]};
  std::vector<ustore::Slice> init_vals{vals_[0]};

  ustore::SMap smap = factory.Create<ustore::SMap>(init_keys, init_vals);

  auto it = smap.Scan();
  CheckIdenticalItems(init_keys, init_vals, &it);

  ustore::SMap smap1 = factory.Load<ustore::SMap>(smap.Set(keys_, vals_));
  auto it1 = smap1.Scan();
  CheckIdenticalItems(keys_, vals_, &it1);
}

TEST_F(SMapHugeEnv, Compare) {
  ustore::ChunkableTypeFactory factory;
  ustore::SMap lhs = factory.Create<ustore::SMap>(keys_, vals_);

  /* rhs map is constructed from lhs by removing
  * k[100] to k[199]
  * Set value of k[200] with v[201], k[201] with v[202] until k[299] to v[300]
  * Insert new (2<<8) kv items
  */
  ustore::Hash rhs_hash = lhs.hash();
  for (uint32_t i = 100; i < 200; ++i) {
    ustore::SMap rhs = factory.Load<ustore::SMap>(rhs_hash);
    rhs_hash = rhs.Remove(keys_[i]);
  }

  for (uint32_t i = 200; i < 300; ++i) {
    ustore::SMap rhs = factory.Load<ustore::SMap>(rhs_hash);
    rhs_hash = rhs.Set(keys_[i], vals_[i + 1]);
  }

  std::vector<ustore::Slice> new_keys;
  std::vector<ustore::Slice> new_vals;

  uint32_t i = 1 << 6;
  for (uint32_t j = 0; j < 1 << 8; j++) {
    char* key = new char[2 * sizeof(uint32_t)];
    std::memcpy(key, &i, sizeof(uint32_t));
    std::memcpy(key + sizeof(uint32_t), &j, sizeof(uint32_t));

    char* val = new char[2 * sizeof(uint32_t)];
    std::memcpy(val, &i, sizeof(uint32_t));
    std::memcpy(val + sizeof(uint32_t), &j, sizeof(uint32_t));

    ustore::Slice k(key, entry_size_);
    ustore::Slice v(val, entry_size_);

    ustore::SMap rhs = factory.Load<ustore::SMap>(rhs_hash);
    rhs_hash = rhs.Set(k, v);

    new_keys.push_back(k);
    new_vals.push_back(v);
  }

  ustore::SMap rhs = factory.Load<ustore::SMap>(rhs_hash);

// Check for rhs correctness
  std::vector<ustore::Slice> expected_rhs_keys;
  std::vector<ustore::Slice> expected_rhs_vals;

  expected_rhs_keys.insert(expected_rhs_keys.end(),
                           keys_.begin(),
                           keys_.begin() + 100);

  expected_rhs_vals.insert(expected_rhs_vals.end(),
                           vals_.begin(),
                           vals_.begin() + 100);

  expected_rhs_keys.insert(expected_rhs_keys.end(),
                           keys_.begin() + 200,
                           keys_.end());

  expected_rhs_vals.insert(expected_rhs_vals.end(),
                           vals_.begin() + 201,
                           vals_.begin() + 301);

  expected_rhs_vals.insert(expected_rhs_vals.end(),
                           vals_.begin() + 300,
                           vals_.end());

  expected_rhs_keys.insert(expected_rhs_keys.end(),
                           new_keys.begin(),
                           new_keys.end());

  expected_rhs_vals.insert(expected_rhs_vals.end(),
                           new_vals.begin(),
                           new_vals.end());
  auto rhs_it = rhs.Scan();
  CheckIdenticalItems(expected_rhs_keys, expected_rhs_vals, &rhs_it);


  // Diff
  std::vector<ustore::Slice> expected_diff_keys;
  std::vector<ustore::Slice> expected_diff_vals;

  expected_diff_keys.insert(expected_diff_keys.end(),
                            keys_.begin() + 100,
                            keys_.begin() + 300);

  expected_diff_vals.insert(expected_diff_vals.end(),
                            vals_.begin() + 100,
                            vals_.begin() + 300);

  auto diff_it = lhs.Diff(rhs);
  CheckIdenticalItems(expected_diff_keys, expected_diff_vals, &diff_it);

  // Intersect
  std::vector<ustore::Slice> expected_intersect_keys;
  std::vector<ustore::Slice> expected_intersect_vals;

  expected_intersect_keys.insert(expected_intersect_keys.end(),
                                 keys_.begin(),
                                 keys_.begin() + 100);

  expected_intersect_keys.insert(expected_intersect_keys.end(),
                                 keys_.begin() + 300,
                                 keys_.end());

  expected_intersect_vals.insert(expected_intersect_vals.end(),
                                 vals_.begin(),
                                 vals_.begin() + 100);

  expected_intersect_vals.insert(expected_intersect_vals.end(),
                                 vals_.begin() + 300,
                                 vals_.end());

  auto intersect_it = lhs.Intersect(rhs);
  CheckIdenticalItems(expected_intersect_keys,
                      expected_intersect_vals,
                      &intersect_it);

  for (const auto& key : new_keys) {delete[] key.data(); }
  for (const auto& val : new_vals) {delete[] val.data(); }
}


TEST(SMapMerge, SmallNormal) {
  const ustore::Slice k0("k0", 2);
  const ustore::Slice v0("v0", 2);

  const ustore::Slice k11("k11", 3);
  const ustore::Slice k1("k1", 2);
  const ustore::Slice v1("v1", 2);

  const ustore::Slice k2("k2", 2);
  const ustore::Slice v2("v2", 2);

  const ustore::Slice k3("k3", 2);
  const ustore::Slice v3("v3", 2);
  const ustore::Slice v33("v33", 3);

  const ustore::Slice k4("k4", 2);
  const ustore::Slice v4("v4", 2);

  const ustore::Slice k5("k5", 2);
  const ustore::Slice v5("v5", 2);

  const ustore::Slice k6("k6", 2);
  const ustore::Slice v6("v6", 2);

  const ustore::Slice k7("k7", 2);
  const ustore::Slice v7("v7", 2);

  const ustore::Slice k8("k8", 2);
  const ustore::Slice v8("v8", 2);

  ustore::ChunkableTypeFactory factory;
  // Base Map: kv0, kv1, kv2, kv3, kv5, kv7
  ustore::SMap base = factory.Create<ustore::SMap>(
      std::vector<ustore::Slice>{k0, k1, k2, k3, k5, k7},
      std::vector<ustore::Slice>{v0, v1, v2, v3, v5, v7});


  // Remove kv0, edit kv1 add kv4, add kv6
  // Node 1 Map: kv1', kv2, kv3, kv4, kv5, kv6, kv7
  ustore::SMap node1 = factory.Create<ustore::SMap>
      (std::vector<ustore::Slice>{k11, k2, k3, k4, k5, k6, k7},
       std::vector<ustore::Slice>{v1, v2, v3, v4, v5, v6, v7});

  // edit kv3, add kv6, add kv8
  // Node 2 Map: kv0, kv1, kv2, kv3', kv5, kv6, kv7, kv8
  ustore::SMap node2 = factory.Create<ustore::SMap>
      (std::vector<ustore::Slice>{k0, k1, k2, k3, k5, k6, k7, k8},
       std::vector<ustore::Slice>{v0, v1, v2, v33, v5, v6, v7, v8});

  // Result Map: kv1', kv2, kv3' kv4, kv5, kv6, kv7, kv8
  std::vector<ustore::Slice> merged_keys{k11, k2, k3, k4, k5, k6, k7, k8};
  std::vector<ustore::Slice> merged_vals{v1, v2, v33, v4, v5, v6, v7, v8};

  ustore::SMap expected_merge =
      factory.Create<ustore::SMap>(merged_keys, merged_vals);

  ustore::SMap actual_merge =
      factory.Load<ustore::SMap>(base.Merge(node1, node2));

  auto it = actual_merge.Scan();
  CheckIdenticalItems(merged_keys, merged_vals, &it);

  ASSERT_TRUE(expected_merge.hash() == actual_merge.hash());
}

TEST(SMapMerge, SmallConflict) {
  ustore::ChunkableTypeFactory factory;
  const ustore::Slice k0("k0", 2);
  const ustore::Slice v0("v0", 2);
  const ustore::Slice v00("v00", 3);

  const ustore::Slice k11("k11", 3);
  const ustore::Slice k1("k1", 2);
  const ustore::Slice v1("v1", 2);

  const ustore::Slice k2("k2", 2);
  const ustore::Slice v2("v2", 2);
  const ustore::Slice v22("v22", 3);

  ustore::SMap base = factory.Create<ustore::SMap>
      (std::vector<ustore::Slice>{k0, k1, k2},
       std::vector<ustore::Slice>{v0, v1, v2});

  ustore::SMap node1 = factory.Create<ustore::SMap>
      (std::vector<ustore::Slice>{k0, k11, k2},
       std::vector<ustore::Slice>{v00, v1, v2});

  ustore::SMap node2 = factory.Create<ustore::SMap>
      (std::vector<ustore::Slice>{k0, k11, k2},
       std::vector<ustore::Slice>{v0, v1, v22});

  ustore::Hash result = base.Merge(node1, node2);
  ASSERT_TRUE(result.empty());
}

TEST(SMapMerge, BigNormal) {
  ustore::ChunkableTypeFactory factory;
  std::vector<ustore::Slice> keys_;
  std::vector<ustore::Slice> vals_;
  for (uint32_t i = 0; i < 1 << 4; i++) {
    for (uint32_t j = 0; j < 1 << 8; j++) {
      char* key = new char[2 * sizeof(uint32_t)];
      std::memcpy(key, &i, sizeof(uint32_t));
      std::memcpy(key + sizeof(uint32_t), &j, sizeof(uint32_t));

      char* val = new char[2 * sizeof(uint32_t)];
      std::memcpy(val, &i, sizeof(uint32_t));
      std::memcpy(val + sizeof(uint32_t), &j, sizeof(uint32_t));

      keys_.push_back(ustore::Slice(key, 2 * sizeof(uint32_t)));
      vals_.push_back(ustore::Slice(val, 2 * sizeof(uint32_t)));
    }
  }

  ustore::SMap base = factory.Create<ustore::SMap>(keys_, vals_);

  // node 1 edits the items from 100 to 400
  ustore::Hash node1_hash = base.hash();
  for (uint32_t j = 100; j < 200; j++) {
    ustore::SMap node1 = factory.Load<ustore::SMap>(node1_hash);
    node1_hash = node1.Set(keys_[j], vals_[j + 100]);
  }

  // node 2 edits the items from 500 to 800
  ustore::Hash node2_hash = base.hash();
  for (uint32_t j = 500; j < 600; j++) {
    ustore::SMap node2 = factory.Load<ustore::SMap>(node2_hash);
    node2_hash = node2.Set(keys_[j], vals_[j + 100]);
  }

  // both nodes edit the items from 900 to 1000
  for (uint32_t j = 900; j < 1000; j++) {
    ustore::SMap node1 = factory.Load<ustore::SMap>(node1_hash);
    ustore::SMap node2 = factory.Load<ustore::SMap>(node2_hash);
    node1_hash = node1.Set(keys_[j], vals_[j + 100]);
    node2_hash = node2.Set(keys_[j], vals_[j + 100]);
  }

  ustore::SMap node1 = factory.Load<ustore::SMap>(node1_hash);
  ustore::SMap node2 = factory.Load<ustore::SMap>(node2_hash);

  ustore::Hash merged_hash = base.Merge(node1, node2);

  std::vector<ustore::Slice> new_vals;

  new_vals.insert(new_vals.end(),
                  vals_.begin(), vals_.begin() + 100);

  new_vals.insert(new_vals.end(),
                  vals_.begin() + 200, vals_.begin() + 300);

  new_vals.insert(new_vals.end(),
                  vals_.begin() + 200, vals_.begin() + 500);

  new_vals.insert(new_vals.end(),
                  vals_.begin() + 600, vals_.begin() + 700);

  new_vals.insert(new_vals.end(),
                  vals_.begin() + 600, vals_.begin() + 900);

  new_vals.insert(new_vals.end(),
                  vals_.begin() + 1000, vals_.begin() + 1100);

  new_vals.insert(new_vals.end(),
                  vals_.begin() + 1000, vals_.end());


  ustore::SMap expected_merge = factory.Create<ustore::SMap>(keys_, new_vals);
  ustore::SMap merge_node = factory.Load<ustore::SMap>(merged_hash);
  auto it = merge_node.Scan();

  CheckIdenticalItems(keys_, new_vals, &it);

  ASSERT_TRUE(merged_hash == expected_merge.hash());

  for (const auto& key : keys_) {delete[] key.data(); }
  for (const auto& val : vals_) {delete[] val.data(); }
}
