// Copyright (c) 2017 The Ustore Authors.

#include <cstring>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "types/umap.h"

// Check KVItems scannbed by iterator are all the same to that in vector
inline void CheckIdenticalItems(
  const std::vector<ustore::Slice>& keys,
  const std::vector<ustore::Slice>& vals,
  ustore::KVIterator* it) {

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
    it->Advance();
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
  auto it = smap.iterator();
  CheckIdenticalItems({k1, k2, k3}, {v1, v2, v3}, it.get());
  EXPECT_TRUE(it->end());

  // // Set with an non-existent key
  ustore::SMap new_smap1(smap.Set(k4, v4));
  EXPECT_EQ(v4.len(), new_smap1.Get(k4).len());
  EXPECT_EQ(0,
            std::memcmp(v4.data(),
                        new_smap1.Get(k4).data(),
                        v4.len()));

  CheckIdenticalItems({k1, k2, k3, k4}, {v1, v2, v3, v4},
                      new_smap1.iterator().get());

  // Set with an existent key
  // Set v3 with v4
  ustore::SMap new_smap2(new_smap1.Set(k3, v4));
  EXPECT_EQ(v4.len(), new_smap2.Get(k3).len());
  EXPECT_EQ(0,
            std::memcmp(v4.data(),
                        new_smap2.Get(k3).data(),
                        v4.len()));

  CheckIdenticalItems({k1, k2, k3, k4}, {v1, v2, v4, v4},
                      new_smap2.iterator().get());

  // // Remove an existent key
  ustore::SMap new_smap3(new_smap2.Remove(k1));
  CheckIdenticalItems({k2, k3, k4}, {v2, v4, v4},
                      new_smap3.iterator().get());

  // Remove an non-existent key
  ustore::SMap new_smap4(smap.Remove(k4));
  CheckIdenticalItems({k1, k2, k3}, {v1, v2, v3},
                      new_smap4.iterator().get());
}

TEST(SMap, Huge) {
  std::vector<ustore::Slice> keys;
  std::vector<ustore::Slice> vals;
  size_t entry_size = 2 * sizeof(uint32_t);

  for (uint32_t i = 0; i < 1 << 6; i++) {
    for (uint32_t j = 0; j < 1 << 8; j++) {
      char* key = new char[2 * sizeof(uint32_t)];
      std::memcpy(key, &i, sizeof(uint32_t));
      std::memcpy(key + sizeof(uint32_t), &j, sizeof(uint32_t));

      char* val = new char[2 * sizeof(uint32_t)];
      std::memcpy(val, &i, sizeof(uint32_t));
      std::memcpy(val + sizeof(uint32_t), &j, sizeof(uint32_t));

      keys.push_back(ustore::Slice(key, 2 * sizeof(uint32_t)));
      vals.push_back(ustore::Slice(val, 2 * sizeof(uint32_t)));
    }
  }

  ustore::SMap smap(keys, vals);
  CheckIdenticalItems(keys, vals, smap.iterator().get());

  // Get
  auto actual_val23 = smap.Get(keys[23]);
  EXPECT_EQ(entry_size, actual_val23.len());
  EXPECT_EQ(0, std::memcmp(vals[23].data(), actual_val23.data(), entry_size));

  // Remove
  ustore::SMap smap1(smap.Remove(keys[35]));
  keys.erase(keys.begin() + 35);
  vals.erase(vals.begin() + 35);
  CheckIdenticalItems(keys, vals, smap1.iterator().get());

  // Set the value of key55 with val56
  ustore::SMap smap2(smap.Set(keys[55], vals[56]));

  auto actual_val55 = smap.Get(keys[55]);
  EXPECT_EQ(entry_size, actual_val55.len());
  EXPECT_EQ(0, std::memcmp(vals[55].data(), actual_val55.data(), entry_size));

  for (const auto& key : keys) {delete[] key.data(); }

  for (const auto& val : vals) {delete[] val.data(); }
}
