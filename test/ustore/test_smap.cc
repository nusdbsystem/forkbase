// Copyright (c) 2017 The Ustore Authors.

#include <cstring>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "types/server/smap.h"

// Check KVItems scannbed by iterator are all the same to that in vector
inline void CheckIdenticalItems(
  const std::vector<ustore::Slice>& keys,
  const std::vector<ustore::Slice>& vals,
  ustore::UIterator* it) {
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
                        actual_val.len()));
    it->next();
  }
  ASSERT_TRUE(it->end());
}

TEST(SMap, Small) {
  const ustore::Slice k1("k1", 2);
  const ustore::Slice v1("v1", 2);
  const ustore::Slice k2("k22", 3);
  const ustore::Slice v2("v22", 3);
  const ustore::Slice k3("k333", 4);
  const ustore::Slice v3("v333", 4);

  // A new key to put
  const ustore::Slice k4("k4444", 5);
  const ustore::Slice v4("v4444", 5);

  ustore::SMap smap({k1, k2, k3}, {v1, v2, v3});

  size_t val_num_bytes = 0;

  // Get Value by Key
  const ustore::Slice actual_v1 = smap.Get(k1);

  EXPECT_EQ(2, v1.len());
  EXPECT_EQ(0, memcmp(v1.data(), actual_v1.data(), 2));

  // Get Value by Non-existent Key
  const ustore::Slice actual_v4 = smap.Get(k4);
  EXPECT_TRUE(actual_v4.empty());

  // Test on Iterator
  auto it = smap.Scan();
  CheckIdenticalItems({k1, k2, k3}, {v1, v2, v3}, it.get());
  EXPECT_TRUE(it->end());

  // Set with an non-existent key
  ustore::SMap new_smap1(smap.Set(k4, v4));
  EXPECT_EQ(v4.len(), new_smap1.Get(k4).len());
  EXPECT_EQ(0,
            std::memcmp(v4.data(),
                        new_smap1.Get(k4).data(),
                        v4.len()));

  CheckIdenticalItems({k1, k2, k3, k4}, {v1, v2, v3, v4},
                      new_smap1.Scan().get());

  // Set with an existent key
  // Set v3 with v4
  ustore::SMap new_smap2(new_smap1.Set(k3, v4));
  EXPECT_EQ(v4.len(), new_smap2.Get(k3).len());
  EXPECT_EQ(0,
            std::memcmp(v4.data(),
                        new_smap2.Get(k3).data(),
                        v4.len()));

  CheckIdenticalItems({k1, k2, k3, k4}, {v1, v2, v4, v4},
                      new_smap2.Scan().get());

  // // Remove an existent key
  ustore::SMap new_smap3(new_smap2.Remove(k1));
  CheckIdenticalItems({k2, k3, k4}, {v2, v4, v4},
                      new_smap3.Scan().get());

  // Remove an non-existent key
  ustore::SMap new_smap4(smap.Remove(k4));
  CheckIdenticalItems({k1, k2, k3}, {v1, v2, v3},
                      new_smap4.Scan().get());


  // Use new_smap3 with smap to perform duallydiff
  // lhs: k1->v1, k2->v2, k3->v3
  // rhs:         k2->v2, k3->v4, k4->v4
  auto dually_diff_it = ustore::UMap::DuallyDiff(smap, new_smap3);

  ASSERT_EQ(k1, dually_diff_it->key());
  ASSERT_EQ(v1, dually_diff_it->lhs_value());
  ASSERT_TRUE(dually_diff_it->rhs_value().empty());

  ASSERT_TRUE(dually_diff_it->next());

  ASSERT_EQ(k3, dually_diff_it->key());
  ASSERT_EQ(v3, dually_diff_it->lhs_value());
  ASSERT_EQ(v4, dually_diff_it->rhs_value());

  ASSERT_TRUE(dually_diff_it->next());

  ASSERT_EQ(k4, dually_diff_it->key());
  ASSERT_TRUE(dually_diff_it->lhs_value().empty());
  ASSERT_EQ(v4, dually_diff_it->rhs_value());

  ASSERT_FALSE(dually_diff_it->next());
  ASSERT_TRUE(dually_diff_it->end());

  // ensure can not advance using next since it is end already
  ASSERT_FALSE(dually_diff_it->next());
  ASSERT_TRUE(dually_diff_it->end());

  // start to retreat
  ASSERT_TRUE(dually_diff_it->previous());

  ASSERT_EQ(k4, dually_diff_it->key());
  ASSERT_TRUE(dually_diff_it->lhs_value().empty());
  ASSERT_EQ(v4, dually_diff_it->rhs_value());

  ASSERT_TRUE(dually_diff_it->previous());

  ASSERT_EQ(k3, dually_diff_it->key());
  ASSERT_EQ(v3, dually_diff_it->lhs_value());
  ASSERT_EQ(v4, dually_diff_it->rhs_value());

  ASSERT_TRUE(dually_diff_it->previous());

  ASSERT_EQ(k1, dually_diff_it->key());
  ASSERT_EQ(v1, dually_diff_it->lhs_value());
  ASSERT_TRUE(dually_diff_it->rhs_value().empty());

  ASSERT_FALSE(dually_diff_it->previous());
  ASSERT_TRUE(dually_diff_it->head());

  // ensure can not retreat using previous since it is at head already
  ASSERT_FALSE(dually_diff_it->previous());
  ASSERT_TRUE(dually_diff_it->head());

  // Test on altenative advance and retreat
  ASSERT_TRUE(dually_diff_it->next());
  ASSERT_FALSE(dually_diff_it->previous());
  ASSERT_TRUE(dually_diff_it->head());

  ASSERT_TRUE(dually_diff_it->next());
  ASSERT_TRUE(dually_diff_it->next());
  ASSERT_TRUE(dually_diff_it->previous());

  ASSERT_EQ(k1, dually_diff_it->key());
  ASSERT_EQ(v1, dually_diff_it->lhs_value());
  ASSERT_TRUE(dually_diff_it->rhs_value().empty());
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
  ustore::SMap smap(keys_, vals_);
  CheckIdenticalItems(keys_, vals_, smap.Scan().get());

  // Get using key[23]
  auto actual_val23 = smap.Get(keys_[23]);
  EXPECT_EQ(entry_size_, actual_val23.len());
  EXPECT_EQ(0, std::memcmp(vals_[23].data(), actual_val23.data(), entry_size_));

  // Remove key[35]
  ustore::SMap smap1(smap.Remove(keys_[35]));
  keys_.erase(keys_.begin() + 35);
  vals_.erase(vals_.begin() + 35);
  CheckIdenticalItems(keys_, vals_, smap1.Scan().get());

  // Set the value of key55 with val56
  ustore::SMap smap2(smap.Set(keys_[55], vals_[56]));

  auto actual_val55 = smap.Get(keys_[55]);
  EXPECT_EQ(entry_size_, actual_val55.len());
  EXPECT_EQ(0, std::memcmp(vals_[55].data(), actual_val55.data(), entry_size_));
}

TEST_F(SMapHugeEnv, Compare) {
  ustore::SMap lhs(keys_, vals_);

  /* rhs map is constructed from lhs by removing
  * k[100] to k[199]
  * Set value of k[200] with v[201], k[201] with v[202] until k[299] to v[300]
  * Insert new (2<<8) kv items
  */
  ustore::Hash rhs_hash = lhs.hash();
  for (uint32_t i = 100; i < 200; ++i) {
    ustore::SMap rhs(rhs_hash);
    rhs_hash = rhs.Remove(keys_[i]);
  }

  for (uint32_t i = 200; i < 300; ++i) {
    ustore::SMap rhs(rhs_hash);
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

    ustore::SMap rhs(rhs_hash);
    rhs_hash = rhs.Set(k, v);

    new_keys.push_back(k);
    new_vals.push_back(v);
  }

  ustore::SMap rhs(rhs_hash);

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

  CheckIdenticalItems(expected_rhs_keys, expected_rhs_vals, rhs.Scan().get());


  // Diff
  std::vector<ustore::Slice> expected_diff_keys;
  std::vector<ustore::Slice> expected_diff_vals;

  expected_diff_keys.insert(expected_diff_keys.end(),
                            keys_.begin() + 100,
                            keys_.begin() + 300);

  expected_diff_vals.insert(expected_diff_vals.end(),
                            vals_.begin() + 100,
                            vals_.begin() + 300);

  CheckIdenticalItems(expected_diff_keys, expected_diff_vals,
                      lhs.Diff(rhs).get());


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


  CheckIdenticalItems(expected_intersect_keys,
                      expected_intersect_vals,
                      lhs.Intersect(rhs).get());


  for (const auto& key : new_keys) {delete[] key.data(); }
  for (const auto& val : new_vals) {delete[] val.data(); }
}
