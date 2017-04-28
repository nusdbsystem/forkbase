// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"

#include "node/blob_node.h"
#include "node/map_node.h"
#include "node/node_comparator.h"
#include "node/node_builder.h"

class IndexComparatorSmallEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    loader_ = std::make_shared<ustore::ChunkLoader>();
    constexpr ustore::byte_t rhs_data[] = "abcededfhijklmnopqrst";  // 20 chars

    ustore::NodeBuilder nb(ustore::BlobChunker::Instance(), true);

    ustore::FixedSegment seg(rhs_data, 20, 1);
    nb.SpliceElements(0, &seg);
    rhs_root_ = nb.Commit();

    rhs_cmptor_ = new ustore::IndexComparator(rhs_root_, loader_);
  }

  virtual void TearDown() {
    delete rhs_cmptor_;
  }

  std::shared_ptr<ustore::ChunkLoader> loader_;
  ustore::Hash rhs_root_;
  const ustore::IndexComparator* rhs_cmptor_;
};


TEST_F(IndexComparatorSmallEnv, Basic) {
  // lhs is constructed by replacing 3 elements starting at 10th with xxx
  //   And removing the last two elements in the end and append y
  ustore::NodeBuilder* nb1 = ustore::NodeBuilder::NewNodeBuilderAtIndex(
      rhs_root_, 10, loader_.get(), ustore::BlobChunker::Instance(), true);

  constexpr ustore::byte_t lhs_data1[] = "xxx";
  ustore::FixedSegment seg1(lhs_data1, 3, 1);
  nb1->SpliceElements(3, &seg1);
  ustore::Hash lhs_temporal = nb1->Commit();
  delete nb1;

  ustore::NodeBuilder* nb2 = ustore::NodeBuilder::NewNodeBuilderAtIndex(
      lhs_temporal, 18, loader_.get(), ustore::BlobChunker::Instance(), true);

  constexpr ustore::byte_t lhs_data2[] = "y";
  ustore::FixedSegment seg2(lhs_data2, 1, 1);
  nb2->SpliceElements(2, &seg2);
  ustore::Hash lhs = nb2->Commit();
  delete nb2;

  //  lhs DIFF rhs
  std::vector<ustore::IndexRange> df_ranges = rhs_cmptor_->Diff(lhs);

  ASSERT_EQ(2, df_ranges.size());

  EXPECT_EQ(10, df_ranges[0].start_idx);
  EXPECT_EQ(3, df_ranges[0].num_subsequent);

  EXPECT_EQ(18, df_ranges[1].start_idx);
  EXPECT_EQ(1, df_ranges[1].num_subsequent);

  // lhs INTERSECT rhs
  std::vector<ustore::IndexRange> intersect_ranges
      = rhs_cmptor_->Intersect(lhs);

  ASSERT_EQ(2, intersect_ranges.size());

  EXPECT_EQ(0, intersect_ranges[0].start_idx);
  EXPECT_EQ(10, intersect_ranges[0].num_subsequent);

  EXPECT_EQ(13, intersect_ranges[1].start_idx);
  EXPECT_EQ(5, intersect_ranges[1].num_subsequent);
}

TEST_F(IndexComparatorSmallEnv, Insertion) {
  // lhs is constructed by inserting 3 elements at 10th of rhs with xxx
  ustore::NodeBuilder* nb = ustore::NodeBuilder::NewNodeBuilderAtIndex(
      rhs_root_, 10, loader_.get(), ustore::BlobChunker::Instance(), true);

  constexpr ustore::byte_t lhs_data[] = "xxx";
  ustore::FixedSegment seg(lhs_data, 3, 1);
  nb->SpliceElements(0, &seg);
  ustore::Hash lhs = nb->Commit();
  delete nb;

  //  lhs DIFF rhs
  std::vector<ustore::IndexRange> df_ranges = rhs_cmptor_->Diff(lhs);

  ASSERT_EQ(1, df_ranges.size());

  EXPECT_EQ(10, df_ranges[0].start_idx);
  EXPECT_EQ(13, df_ranges[0].num_subsequent);

  // lhs INTERSECT rhs
  std::vector<ustore::IndexRange> intersect_ranges
      = rhs_cmptor_->Intersect(lhs);

  ASSERT_EQ(1, intersect_ranges.size());

  EXPECT_EQ(0, intersect_ranges[0].start_idx);
  EXPECT_EQ(10, intersect_ranges[0].num_subsequent);
}

TEST_F(IndexComparatorSmallEnv, Deletion) {
  // lhs is constructed by removing 3 elements at 10th of rhs with xxx
  ustore::NodeBuilder* nb = ustore::NodeBuilder::NewNodeBuilderAtIndex(
      rhs_root_, 10, loader_.get(), ustore::BlobChunker::Instance(), true);

  constexpr ustore::byte_t* lhs_data = nullptr;
  ustore::FixedSegment seg(lhs_data, 0, 1);
  nb->SpliceElements(3, &seg);
  ustore::Hash lhs = nb->Commit();
  delete nb;

  //  lhs DIFF rhs
  std::vector<ustore::IndexRange> df_ranges = rhs_cmptor_->Diff(lhs);

  ASSERT_EQ(1, df_ranges.size());

  EXPECT_EQ(10, df_ranges[0].start_idx);
  EXPECT_EQ(7, df_ranges[0].num_subsequent);

  // lhs INTERSECT rhs
  std::vector<ustore::IndexRange> intersect_ranges
      = rhs_cmptor_->Intersect(lhs);

  ASSERT_EQ(1, intersect_ranges.size());

  EXPECT_EQ(0, intersect_ranges[0].start_idx);
  EXPECT_EQ(10, intersect_ranges[0].num_subsequent);
}

class IndexComparatorBigEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    loader_ = std::make_shared<ustore::ChunkLoader>();
    const ustore::byte_t rhs_data[] = {
        "SCENE I. Rome. A street.  Enter FLAVIUS, MARULLUS, and certain "
        "Commoners FLAVIUS Hence! home, you idle creatures get you home: Is "
        "this "
        "a holiday? what! know you not, Being mechanical, you ought not walk "
        "Upon a labouring day without the sign Of your profession? Speak, what "
        "trade art thou?  First Commoner Why, sir, a carpenter.  MARULLUS "
        "Where "
        "is thy leather apron and thy rule?  What dost thou with thy best "
        "apparel on?  You, sir, what trade are you?  Second Commoner Truly, "
        "sir, "
        "in respect of a fine workman, I am but, as you would say, a cobbler.  "
        "MARULLUS But what trade art thou? answer me directly.  Second "
        "Commoner "
        "I am, indeed, sir, a surgeon to old shoes; when they are in great "
        "danger, I recover them. As proper men as ever trod upon neat's "
        "leather "
        "have gone upon my handiwork.  FLAVIUS But wherefore art not in thy "
        "shop "
        "today?  Why dost thou lead these men about the streets?  Second "
        "Commoner Truly, sir, to wear out their shoes, to get myself into more "
        "work. But, indeed, sir, we make holiday, to see Caesar and to rejoice "
        "in his triumph.  MARULLUS Wherefore rejoice? What conquest brings he "
        "home?  What tributaries follow him to Rome, To grace in captive bonds "
        "his chariot-wheels?  You blocks, you stones, you worse than senseless "
        "things!  O you hard hearts, you cruel men of Rome, Knew you not "
        "Pompey? "
        "Many a time and oft Have you climb'd up to walls and battlements, To "
        "towers and windows, yea, to chimney-tops, Your infants in your arms, "
        "Caesar's trophies. I'll about, And drive away the vulgar from the "
        "streets: So do you too, where you perceive them thick.  These growing "
        "feathers pluck'd from Caesar's wing Will make him fly an ordinary "
        "pitch, Who else would soar above the view of men And keep us all in "
        "servile fearfulness. Exeunt"};

    ustore::NodeBuilder nb(ustore::BlobChunker::Instance(), true);

    rhs_len_ = sizeof(rhs_data) - 1;
    ustore::FixedSegment seg(rhs_data, rhs_len_, 1);
    nb.SpliceElements(0, &seg);
    rhs_root_ = nb.Commit();

    rhs_cmptor_ = new ustore::IndexComparator(rhs_root_, loader_);
  }

  virtual void TearDown() {
    delete rhs_cmptor_;
  }

  std::shared_ptr<ustore::ChunkLoader> loader_;
  ustore::Hash rhs_root_;
  size_t rhs_len_;
  const ustore::IndexComparator* rhs_cmptor_;
};


TEST_F(IndexComparatorBigEnv, Basic) {
  // lhs is constructed by replacing 10 elements starting at 60th
  //   And removing the last 5 elements in the end and append 10
  ustore::NodeBuilder* nb1 = ustore::NodeBuilder::NewNodeBuilderAtIndex(
      rhs_root_, 60, loader_.get(), ustore::BlobChunker::Instance(), true);

  constexpr ustore::byte_t lhs_data1[] = "9999999999";  // 10 9s
  ustore::FixedSegment seg1(lhs_data1, 10, 1);
  nb1->SpliceElements(10, &seg1);
  ustore::Hash lhs_temporal = nb1->Commit();
  delete nb1;

  ustore::NodeBuilder* nb2 = ustore::NodeBuilder::NewNodeBuilderAtIndex(
      lhs_temporal, rhs_len_ - 5, loader_.get(),
      ustore::BlobChunker::Instance(), true);

  constexpr ustore::byte_t lhs_data2[] = "9999999999";  // 10 9s
  ustore::FixedSegment seg2(lhs_data2, 10, 1);
  nb2->SpliceElements(5, &seg2);
  ustore::Hash lhs = nb2->Commit();
  delete nb2;

  //  lhs DIFF rhs
  std::vector<ustore::IndexRange> df_ranges = rhs_cmptor_->Diff(lhs);

  ASSERT_EQ(2, df_ranges.size());

  EXPECT_EQ(60, df_ranges[0].start_idx);
  EXPECT_EQ(10, df_ranges[0].num_subsequent);

  EXPECT_EQ(rhs_len_ - 5, df_ranges[1].start_idx);
  EXPECT_EQ(10, df_ranges[1].num_subsequent);

  // lhs INTERSECT rhs
  std::vector<ustore::IndexRange> intersect_ranges
      = rhs_cmptor_->Intersect(lhs);

  ASSERT_EQ(2, intersect_ranges.size());

  EXPECT_EQ(0, intersect_ranges[0].start_idx);
  EXPECT_EQ(60, intersect_ranges[0].num_subsequent);

  EXPECT_EQ(70, intersect_ranges[1].start_idx);
  EXPECT_EQ(rhs_len_ - 70 - 5, intersect_ranges[1].num_subsequent);
}


class KeyComparatorSmallEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    loader_ = std::make_shared<ustore::ChunkLoader>();

    constexpr const ustore::byte_t k1[] = "k1";
    constexpr const ustore::byte_t v1[] = "v1";
    constexpr const ustore::byte_t k2[] = "k2";
    constexpr const ustore::byte_t v2[] = "v2";
    constexpr const ustore::byte_t k3[] = "k3";
    constexpr const ustore::byte_t v3[] = "v3";
    constexpr const ustore::byte_t k4[] = "k4";
    constexpr const ustore::byte_t v4[] = "v4";
    constexpr const ustore::byte_t k5[] = "k55";
    constexpr const ustore::byte_t v5[] = "v55";
    constexpr const ustore::byte_t k6[] = "k6";
    constexpr const ustore::byte_t v6[] = "v6";
    constexpr const ustore::byte_t k7[] = "k7";
    constexpr const ustore::byte_t v7[] = "v7";

    ustore::KVItem kv1{k1, v1, 2, 2};
    ustore::KVItem kv2{k2, v2, 2, 2};
    ustore::KVItem kv3{k3, v3, 2, 2};
    ustore::KVItem kv4{k4, v4, 2, 2};
    ustore::KVItem kv5{k5, v5, 3, 3};
    ustore::KVItem kv6{k6, v6, 2, 2};
    ustore::KVItem kv7{k7, v7, 2, 2};

    std::unique_ptr<const ustore::Segment> seg =
        ustore::MapNode::Encode({kv1, kv2, kv3, kv4, kv5, kv6, kv7});

    ustore::NodeBuilder nb(ustore::MapChunker::Instance(), false);

    nb.SpliceElements(0, seg.get());
    rhs_root_ = nb.Commit();

    rhs_cmptor_ = new ustore::KeyComparator(rhs_root_, loader_);
  }

  uint64_t numElements(const ustore::Hash& root) {
    const ustore::Chunk* chunk = loader_->Load(root);
    auto node = ustore::SeqNode::CreateFromChunk(chunk);
    return node->numElements();
  }

  virtual void TearDown() {
    delete rhs_cmptor_;
  }

  std::shared_ptr<ustore::ChunkLoader> loader_;
  ustore::Hash rhs_root_;
  const ustore::KeyComparator* rhs_cmptor_;
};


TEST_F(KeyComparatorSmallEnv, Basic) {
  // lhs is constructed by
  //   replacing k2 with new v2, remove kv3
  //   replace kv5 with new kv5
  //   remove k7 and append k8 and k9

  constexpr const ustore::byte_t k2[] = "k2";
  constexpr const ustore::byte_t new_v2[] = "v22";

  constexpr const ustore::byte_t k5[] = "k55";

  constexpr const ustore::byte_t new_k5[] = "k5";
  constexpr const ustore::byte_t new_v5[] = "v5";

  constexpr const ustore::byte_t k7[] = "k7";

  constexpr const ustore::byte_t k8[] = "k8";
  constexpr const ustore::byte_t v8[] = "v8";
  constexpr const ustore::byte_t k9[] = "k9";
  constexpr const ustore::byte_t v9[] = "v9";

  ustore::KVItem new_kv2{k2, new_v2, 3, 3};
  ustore::KVItem new_kv5{new_k5, new_v2, 2, 2};

  ustore::KVItem kv8{k8, v8, 2, 2};
  ustore::KVItem kv9{k9, v9, 2, 2};

  const ustore::OrderedKey key2(false, k2, 2);
  const ustore::OrderedKey key5(false, k5, 3);
  const ustore::OrderedKey key7(false, k7, 2);

  bool foundKey = false;

// replacing k2 with new v2, remove kv3
  ustore::NodeBuilder* nb1 = ustore::NodeBuilder::NewNodeBuilderAtKey(
      rhs_root_, key2, loader_.get(), ustore::MapChunker::Instance(),
      false, &foundKey);

  ASSERT_TRUE(foundKey);

  std::unique_ptr<const ustore::Segment> seg1 =
      ustore::MapNode::Encode({new_kv2});

  nb1->SpliceElements(2, seg1.get());
  ustore::Hash lhs_t1 = nb1->Commit();

  ASSERT_EQ(6, numElements(lhs_t1));
  delete nb1;


  foundKey = false;
// replace kv5 with new_kv5
  ustore::NodeBuilder* nb2 = ustore::NodeBuilder::NewNodeBuilderAtKey(
      lhs_t1, key5, loader_.get(), ustore::MapChunker::Instance(),
      false, &foundKey);

  ASSERT_TRUE(foundKey);

  std::unique_ptr<const ustore::Segment> seg2 =
      ustore::MapNode::Encode({new_kv5});

  nb2->SpliceElements(1, seg2.get());
  ustore::Hash lhs_t2 = nb2->Commit();
  delete nb2;
  ASSERT_EQ(6, numElements(lhs_t2));


  foundKey = false;
// remove k6 and append kv7 and kv8
  ustore::NodeBuilder* nb3 = ustore::NodeBuilder::NewNodeBuilderAtKey(
      lhs_t2, key7, loader_.get(), ustore::MapChunker::Instance(),
      false, &foundKey);

  ASSERT_TRUE(foundKey);

  std::unique_ptr<const ustore::Segment> seg3 =
      ustore::MapNode::Encode({kv8, kv9});

  nb3->SpliceElements(1, seg3.get());
  ustore::Hash lhs = nb3->Commit();
  delete nb3;
  ASSERT_EQ(7, numElements(lhs));


  //  lhs DIFF rhs
  std::vector<ustore::IndexRange> df_ranges = rhs_cmptor_->Diff(lhs);

  ASSERT_EQ(3, df_ranges.size());

  EXPECT_EQ(1, df_ranges[0].start_idx);  // for key2
  EXPECT_EQ(1, df_ranges[0].num_subsequent);

  EXPECT_EQ(3, df_ranges[1].start_idx);  // for key5
  EXPECT_EQ(1, df_ranges[1].num_subsequent);

  EXPECT_EQ(5, df_ranges[2].start_idx);  // for key7 and key8
  EXPECT_EQ(2, df_ranges[2].num_subsequent);

  // lhs INTERSECT rhs
  std::vector<ustore::IndexRange> intersect_ranges
      = rhs_cmptor_->Intersect(lhs);

  ASSERT_EQ(3, intersect_ranges.size());

  EXPECT_EQ(0, intersect_ranges[0].start_idx);
  EXPECT_EQ(1, intersect_ranges[0].num_subsequent);

  EXPECT_EQ(2, intersect_ranges[1].start_idx);
  EXPECT_EQ(1, intersect_ranges[1].num_subsequent);

  EXPECT_EQ(4, intersect_ranges[2].start_idx);
  EXPECT_EQ(1, intersect_ranges[2].num_subsequent);
}


class KeyComparatorBigEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    loader_ = std::make_shared<ustore::ChunkLoader>();

    // the num bytes for both key and value
    entry_size_ = 2 * sizeof(uint32_t);

    for (uint32_t i = 0; i < 1 << 6; i++) {
      for (uint32_t j = 0; j < 1 << 8; j++) {
        ++num_items_;
        ustore::byte_t* key = new ustore::byte_t[2 * sizeof(uint32_t)];
        std::memcpy(key, &i, sizeof(uint32_t));
        std::memcpy(key + sizeof(uint32_t), &j, sizeof(uint32_t));

        ustore::byte_t* val = new ustore::byte_t[2 * sizeof(uint32_t)];
        std::memcpy(val, &i, sizeof(uint32_t));
        std::memcpy(val + sizeof(uint32_t), &j, sizeof(uint32_t));

        keys_.push_back(key);
        vals_.push_back(val);
        kvs_.push_back({key, val, entry_size_, entry_size_});
      }
    }  // end 2 fors

    std::unique_ptr<const ustore::Segment> seg =
        ustore::MapNode::Encode(kvs_);

    ustore::NodeBuilder nb(ustore::MapChunker::Instance(), false);

    nb.SpliceElements(0, seg.get());
    rhs_root_ = nb.Commit();

    rhs_cmptor_ = new ustore::KeyComparator(rhs_root_, loader_);
  }

  uint64_t numElements(const ustore::Hash& root) {
    const ustore::Chunk* chunk = loader_->Load(root);
    auto node = ustore::SeqNode::CreateFromChunk(chunk);
    return node->numElements();
  }

  virtual void TearDown() {
    delete rhs_cmptor_;

    for (auto key : keys_) { delete key; }

    for (auto val : vals_) { delete val; }
  }

  uint64_t num_items_ = 0;
  size_t entry_size_;

  std::vector<const ustore::byte_t*> keys_;
  std::vector<const ustore::byte_t*> vals_;
  std::vector<ustore::KVItem> kvs_;

  std::shared_ptr<ustore::ChunkLoader> loader_;
  ustore::Hash rhs_root_;
  const ustore::KeyComparator* rhs_cmptor_;
};


TEST_F(KeyComparatorBigEnv, Basic) {
  // lhs is constructed by
  //   replacing 100 kvitems from the 400th
  //   removing 200 kvitems from the 100th item

  bool foundKey = false;
  const ustore::OrderedKey key400{false, keys_[400], entry_size_};

  std::vector<ustore::KVItem> new_items;
  for (size_t i = 400; i < 400 + 100; i++) {
    new_items.push_back({keys_[i], vals_[i+1], entry_size_, entry_size_});
  }
  auto seg1 = ustore::MapNode::Encode(new_items);

  foundKey = false;
  ustore::NodeBuilder* nb1 = ustore::NodeBuilder::NewNodeBuilderAtKey(
      rhs_root_, key400, loader_.get(), ustore::MapChunker::Instance(),
      false, &foundKey);

  ASSERT_TRUE(foundKey);
  nb1->SpliceElements(100, seg1.get());
  ustore::Hash lhs_t = nb1->Commit();
  delete nb1;
  ASSERT_EQ(num_items_, numElements(lhs_t));



  const ustore::OrderedKey key100{false, keys_[100], entry_size_};

  ustore::NodeBuilder* nb2 = ustore::NodeBuilder::NewNodeBuilderAtKey(
      lhs_t, key100, loader_.get(), ustore::MapChunker::Instance(),
      false, &foundKey);

  ASSERT_TRUE(foundKey);
  ustore::VarSegment seg2(nullptr, 0, {});

  nb2->SpliceElements(200, &seg2);
  ustore::Hash lhs = nb2->Commit();
  delete nb2;
  ASSERT_EQ(num_items_ - 200, numElements(lhs));


  //  lhs DIFF rhs
  std::vector<ustore::IndexRange> df_ranges = rhs_cmptor_->Diff(lhs);

  for (auto range : df_ranges) {
    LOG(INFO) << range.start_idx << " "
              << range.num_subsequent;
  }

  ASSERT_EQ(1, df_ranges.size());

  EXPECT_EQ(200, df_ranges[0].start_idx);
  EXPECT_EQ(100, df_ranges[0].num_subsequent);


  // // lhs INTERSECT rhs
  std::vector<ustore::IndexRange> intersect_ranges
      = rhs_cmptor_->Intersect(lhs);

  ASSERT_EQ(2, intersect_ranges.size());

  EXPECT_EQ(0, intersect_ranges[0].start_idx);
  EXPECT_EQ(200, intersect_ranges[0].num_subsequent);

  EXPECT_EQ(300, intersect_ranges[1].start_idx);
  EXPECT_EQ(num_items_ - 200 - 300, intersect_ranges[1].num_subsequent);
}
