// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"

#include "node/blob_node.h"
#include "node/map_node.h"
#include "node/list_node.h"
#include "node/node_comparator.h"
#include "node/node_builder.h"

class IndexComparatorSmallEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    loader_ = std::make_shared<ustore::LocalChunkLoader>();
    constexpr ustore::byte_t rhs_data[] = "abcededfhijklmnopqrst";  // 20 chars

    ustore::LocalChunkWriter writer;
    ustore::NodeBuilder nb(&writer, ustore::BlobChunker::Instance(),
                           ustore::MetaChunker::Instance(), true);

    ustore::FixedSegment seg(rhs_data, 20, 1);
    nb.SpliceElements(0, &seg);
    rhs_root_ = nb.Commit();

    rhs_intersector_ = new ustore::IndexIntersector(rhs_root_, loader_.get());
    rhs_differ_ = new ustore::IndexDiffer(rhs_root_, loader_.get());
  }

  virtual void TearDown() {
    delete rhs_intersector_;
    delete rhs_differ_;
  }

  std::shared_ptr<ustore::ChunkLoader> loader_;
  ustore::Hash rhs_root_;

  const ustore::IndexIntersector* rhs_intersector_;
  const ustore::IndexDiffer* rhs_differ_;
};


TEST_F(IndexComparatorSmallEnv, Basic) {
  // lhs is constructed by replacing 3 elements starting at 10th with xxx
  //   And removing the last two elements in the end and append y
  ustore::LocalChunkWriter writer;
  ustore::NodeBuilder nb1(rhs_root_, 10, loader_.get(), &writer,
                          ustore::BlobChunker::Instance(),
                          ustore::MetaChunker::Instance(), true);

  constexpr ustore::byte_t lhs_data1[] = "xxx";
  ustore::FixedSegment seg1(lhs_data1, 3, 1);
  nb1.SpliceElements(3, &seg1);
  ustore::Hash lhs_temporal = nb1.Commit();

  ustore::NodeBuilder nb2(lhs_temporal, 18, loader_.get(), &writer,
                          ustore::BlobChunker::Instance(),
                          ustore::MetaChunker::Instance(), true);

  constexpr ustore::byte_t lhs_data2[] = "y";
  ustore::FixedSegment seg2(lhs_data2, 1, 1);
  nb2.SpliceElements(2, &seg2);
  ustore::Hash lhs = nb2.Commit();

  // lhs DIFF rhs
  std::vector<ustore::IndexRange> df_ranges = rhs_differ_->Compare(lhs);

  ASSERT_EQ(size_t(2), df_ranges.size());

  EXPECT_EQ(size_t(10), df_ranges[0].start_idx);
  EXPECT_EQ(size_t(3), df_ranges[0].num_subsequent);

  EXPECT_EQ(size_t(18), df_ranges[1].start_idx);
  EXPECT_EQ(size_t(1), df_ranges[1].num_subsequent);

  // lhs INTERSECT rhs
  std::vector<ustore::IndexRange> intersect_ranges
      = rhs_intersector_->Compare(lhs);

  ASSERT_EQ(size_t(2), intersect_ranges.size());

  EXPECT_EQ(size_t(0), intersect_ranges[0].start_idx);
  EXPECT_EQ(size_t(10), intersect_ranges[0].num_subsequent);

  EXPECT_EQ(size_t(13), intersect_ranges[1].start_idx);
  EXPECT_EQ(size_t(5), intersect_ranges[1].num_subsequent);
}

TEST_F(IndexComparatorSmallEnv, Insertion) {
  ustore::LocalChunkWriter writer;
  // lhs is constructed by inserting 3 elements at 10th of rhs with xxx
  ustore::NodeBuilder nb(rhs_root_, 10, loader_.get(), &writer,
                         ustore::BlobChunker::Instance(),
                         ustore::MetaChunker::Instance(), true);

  constexpr ustore::byte_t lhs_data[] = "xxx";
  ustore::FixedSegment seg(lhs_data, 3, 1);
  nb.SpliceElements(0, &seg);
  ustore::Hash lhs = nb.Commit();

  //  lhs DIFF rhs
  std::vector<ustore::IndexRange> df_ranges = rhs_differ_->Compare(lhs);

  ASSERT_EQ(size_t(1), df_ranges.size());

  EXPECT_EQ(size_t(10), df_ranges[0].start_idx);
  EXPECT_EQ(size_t(13), df_ranges[0].num_subsequent);

  // lhs INTERSECT rhs
  std::vector<ustore::IndexRange> intersect_ranges
      = rhs_intersector_->Compare(lhs);

  ASSERT_EQ(size_t(1), intersect_ranges.size());

  EXPECT_EQ(size_t(0), intersect_ranges[0].start_idx);
  EXPECT_EQ(size_t(10), intersect_ranges[0].num_subsequent);
}

TEST_F(IndexComparatorSmallEnv, Deletion) {
  ustore::LocalChunkWriter writer;
  // lhs is constructed by removing 3 elements at 10th of rhs with xxx
  ustore::NodeBuilder nb(rhs_root_, 10, loader_.get(), &writer,
                         ustore::BlobChunker::Instance(),
                         ustore::MetaChunker::Instance(), true);

  constexpr ustore::byte_t* lhs_data = nullptr;
  ustore::FixedSegment seg(lhs_data, 0, 1);
  nb.SpliceElements(3, &seg);
  ustore::Hash lhs = nb.Commit();

  //  lhs DIFF rhs
  std::vector<ustore::IndexRange> df_ranges = rhs_differ_->Compare(lhs);

  ASSERT_EQ(size_t(1), df_ranges.size());

  EXPECT_EQ(size_t(10), df_ranges[0].start_idx);
  EXPECT_EQ(size_t(7), df_ranges[0].num_subsequent);

  // lhs INTERSECT rhs
  std::vector<ustore::IndexRange> intersect_ranges
      = rhs_intersector_->Compare(lhs);

  ASSERT_EQ(size_t(1), intersect_ranges.size());

  EXPECT_EQ(size_t(0), intersect_ranges[0].start_idx);
  EXPECT_EQ(size_t(10), intersect_ranges[0].num_subsequent);
}

class IndexComparatorBigEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    loader_ = std::make_shared<ustore::LocalChunkLoader>();
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

    ustore::LocalChunkWriter writer;
    ustore::NodeBuilder nb(&writer, ustore::BlobChunker::Instance(),
                           ustore::MetaChunker::Instance(), true);

    rhs_len_ = sizeof(rhs_data) - 1;
    ustore::FixedSegment seg(rhs_data, rhs_len_, 1);
    nb.SpliceElements(0, &seg);
    rhs_root_ = nb.Commit();

    rhs_intersector_ = new ustore::IndexIntersector(rhs_root_, loader_.get());
    rhs_differ_ = new ustore::IndexDiffer(rhs_root_, loader_.get());
  }

  virtual void TearDown() {
    delete rhs_intersector_;
    delete rhs_differ_;
  }

  std::shared_ptr<ustore::ChunkLoader> loader_;
  ustore::Hash rhs_root_;
  size_t rhs_len_;

  const ustore::IndexDiffer* rhs_differ_;
  const ustore::IndexIntersector* rhs_intersector_;
};


TEST_F(IndexComparatorBigEnv, Basic) {
  ustore::LocalChunkWriter writer;
  // lhs is constructed by replacing 10 elements starting at 60th
  //   And removing the last 5 elements in the end and append 10
  ustore::NodeBuilder nb1(rhs_root_, 60, loader_.get(), &writer,
                          ustore::BlobChunker::Instance(),
                          ustore::MetaChunker::Instance(), true);

  constexpr ustore::byte_t lhs_data1[] = "9999999999";  // 10 9s
  ustore::FixedSegment seg1(lhs_data1, 10, 1);
  nb1.SpliceElements(10, &seg1);
  ustore::Hash lhs_temporal = nb1.Commit();

  ustore::NodeBuilder nb2(lhs_temporal, rhs_len_ - 5,
                          loader_.get(), &writer,
                          ustore::BlobChunker::Instance(),
                          ustore::MetaChunker::Instance(), true);

  constexpr ustore::byte_t lhs_data2[] = "9999999999";  // 10 9s
  ustore::FixedSegment seg2(lhs_data2, 10, 1);
  nb2.SpliceElements(5, &seg2);
  ustore::Hash lhs = nb2.Commit();

  //  lhs DIFF rhs
  std::vector<ustore::IndexRange> df_ranges = rhs_differ_->Compare(lhs);

  ASSERT_EQ(size_t(2), df_ranges.size());

  EXPECT_EQ(size_t(60), df_ranges[0].start_idx);
  EXPECT_EQ(size_t(10), df_ranges[0].num_subsequent);

  EXPECT_EQ(rhs_len_ - 5, df_ranges[1].start_idx);
  EXPECT_EQ(size_t(10), df_ranges[1].num_subsequent);

  // lhs INTERSECT rhs
  std::vector<ustore::IndexRange> intersect_ranges
      = rhs_intersector_->Compare(lhs);

  ASSERT_EQ(size_t(2), intersect_ranges.size());

  EXPECT_EQ(size_t(0), intersect_ranges[0].start_idx);
  EXPECT_EQ(size_t(60), intersect_ranges[0].num_subsequent);

  EXPECT_EQ(size_t(70), intersect_ranges[1].start_idx);
  EXPECT_EQ(rhs_len_ - 70 - 5, intersect_ranges[1].num_subsequent);
}


class KeyComparatorSmallEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    loader_ = std::make_shared<ustore::LocalChunkLoader>();

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

    ustore::KVItem kv1{{k1, 2}, {v1, 2}};
    ustore::KVItem kv2{{k2, 2}, {v2, 2}};
    ustore::KVItem kv3{{k3, 2}, {v3, 2}};
    ustore::KVItem kv4{{k4, 2}, {v4, 2}};
    ustore::KVItem kv5{{k5, 3}, {v5, 3}};
    ustore::KVItem kv6{{k6, 2}, {v6, 2}};
    ustore::KVItem kv7{{k7, 2}, {v7, 2}};

    std::unique_ptr<const ustore::Segment> seg =
        ustore::MapNode::Encode({kv1, kv2, kv3, kv4, kv5, kv6, kv7});

    ustore::LocalChunkWriter writer;
    ustore::NodeBuilder nb(&writer, ustore::MapChunker::Instance(),
                           ustore::MetaChunker::Instance(), false);

    nb.SpliceElements(0, seg.get());
    rhs_root_ = nb.Commit();

    rhs_differ_ = new ustore::KeyDiffer(rhs_root_, loader_.get());
    rhs_intersector_ = new ustore::KeyIntersector(rhs_root_, loader_.get());
    rhs_mapper_ = new ustore::KeyMapper(rhs_root_, loader_.get());
  }

  uint64_t numElements(const ustore::Hash& root) {
    const ustore::Chunk* chunk = loader_->Load(root);
    auto node = ustore::SeqNode::CreateFromChunk(chunk);
    return node->numElements();
  }

  virtual void TearDown() {
    delete rhs_differ_;
    delete rhs_intersector_;
    delete rhs_mapper_;
  }

  std::shared_ptr<ustore::ChunkLoader> loader_;
  ustore::Hash rhs_root_;

  const ustore::KeyDiffer* rhs_differ_;
  const ustore::KeyIntersector* rhs_intersector_;
  const ustore::KeyMapper* rhs_mapper_;
};


TEST_F(KeyComparatorSmallEnv, Basic) {
  ustore::LocalChunkWriter writer;
  // lhs is constructed by
  //   replacing k2 with new v2,
  //   remove kv3
  //   replace kv5 with new kv5
  //   remove k7
  //   append k8 and k9

  // lhs: k1->v1. k2->v22, k4->v4, k5->v22, k6->v6, k8->v8, k9->v9
  // rhs: k1->v1, k2->v2, k3->v3, k4->v4, k55->v55, k6->v6, k7->v7

  constexpr const ustore::byte_t k2[] = "k2";
  constexpr const ustore::byte_t new_v2[] = "v22";

  constexpr const ustore::byte_t k5[] = "k55";

  constexpr const ustore::byte_t new_k5[] = "k5";

  constexpr const ustore::byte_t k7[] = "k7";

  constexpr const ustore::byte_t k8[] = "k8";
  constexpr const ustore::byte_t v8[] = "v8";
  constexpr const ustore::byte_t k9[] = "k9";
  constexpr const ustore::byte_t v9[] = "v9";

  ustore::KVItem new_kv2{{k2, 3}, {new_v2, 3}};
  ustore::KVItem new_kv5{{new_k5, 2}, {new_v2, 2}};

  ustore::KVItem kv8{{k8, 2}, {v8, 2}};
  ustore::KVItem kv9{{k9, 2}, {v9, 2}};

  const ustore::OrderedKey key2(false, k2, 2);
  const ustore::OrderedKey key5(false, k5, 3);
  const ustore::OrderedKey key7(false, k7, 2);


// replacing k2 with new v2, remove kv3
  ustore::NodeBuilder nb1(rhs_root_, key2, loader_.get(), &writer,
                          ustore::MapChunker::Instance(),
                          ustore::MetaChunker::Instance(), false);

  std::unique_ptr<const ustore::Segment> seg1 =
      ustore::MapNode::Encode({new_kv2});

  nb1.SpliceElements(2, seg1.get());
  ustore::Hash lhs_t1 = nb1.Commit();

  ASSERT_EQ(size_t(6), numElements(lhs_t1));

// replace kv5 with new_kv5
  ustore::NodeBuilder nb2(lhs_t1, key5, loader_.get(), &writer,
                          ustore::MapChunker::Instance(),
                          ustore::MetaChunker::Instance(), false);

  std::unique_ptr<const ustore::Segment> seg2 =
      ustore::MapNode::Encode({new_kv5});

  nb2.SpliceElements(1, seg2.get());
  ustore::Hash lhs_t2 = nb2.Commit();
  ASSERT_EQ(size_t(6), numElements(lhs_t2));


  ustore::NodeBuilder nb3(lhs_t2, key7, loader_.get(), &writer,
                          ustore::MapChunker::Instance(),
                          ustore::MetaChunker::Instance(), false);

  std::unique_ptr<const ustore::Segment> seg3 =
      ustore::MapNode::Encode({kv8, kv9});

  nb3.SpliceElements(1, seg3.get());
  ustore::Hash lhs = nb3.Commit();

  ASSERT_EQ(size_t(7), numElements(lhs));

  //  lhs DIFF rhs
  std::vector<ustore::IndexRange> df_ranges = rhs_differ_->Compare(lhs);

  ASSERT_EQ(size_t(3), df_ranges.size());

  EXPECT_EQ(size_t(1), df_ranges[0].start_idx);  // for key2
  EXPECT_EQ(size_t(1), df_ranges[0].num_subsequent);

  EXPECT_EQ(size_t(3), df_ranges[1].start_idx);  // for key5
  EXPECT_EQ(size_t(1), df_ranges[1].num_subsequent);

  EXPECT_EQ(size_t(5), df_ranges[2].start_idx);  // for key8 and key9
  EXPECT_EQ(size_t(2), df_ranges[2].num_subsequent);

  // lhs INTERSECT rhs
  std::vector<ustore::IndexRange> intersect_ranges
      = rhs_intersector_->Compare(lhs);

  ASSERT_EQ(size_t(3), intersect_ranges.size());

  EXPECT_EQ(size_t(0), intersect_ranges[0].start_idx);
  EXPECT_EQ(size_t(1), intersect_ranges[0].num_subsequent);

  EXPECT_EQ(size_t(2), intersect_ranges[1].start_idx);
  EXPECT_EQ(size_t(1), intersect_ranges[1].num_subsequent);

  EXPECT_EQ(size_t(4), intersect_ranges[2].start_idx);
  EXPECT_EQ(size_t(1), intersect_ranges[2].num_subsequent);

  // lhs MAP rhs
  std::vector<std::pair<ustore::IndexRange, ustore::IndexRange>> range_maps
      = rhs_mapper_->Compare(lhs);

  ASSERT_EQ(size_t(3), range_maps.size());

  // lhs: {0, 1} -> rhs: {0, 1}
  EXPECT_EQ(size_t(0), range_maps[0].first.start_idx);
  EXPECT_EQ(size_t(1), range_maps[0].first.num_subsequent);

  EXPECT_EQ(size_t(0), range_maps[0].second.start_idx);
  EXPECT_EQ(size_t(1), range_maps[0].second.num_subsequent);

  // lhs: {2, 1} -> rhs: {3, 1}
  EXPECT_EQ(size_t(2), range_maps[1].first.start_idx);
  EXPECT_EQ(size_t(1), range_maps[1].first.num_subsequent);

  EXPECT_EQ(size_t(3), range_maps[1].second.start_idx);
  EXPECT_EQ(size_t(1), range_maps[1].second.num_subsequent);

  // lhs: {4, 1} -> rhs: {5, 1}
  EXPECT_EQ(size_t(4), range_maps[2].first.start_idx);
  EXPECT_EQ(size_t(1), range_maps[2].first.num_subsequent);

  EXPECT_EQ(size_t(5), range_maps[2].second.start_idx);
  EXPECT_EQ(size_t(1), range_maps[2].second.num_subsequent);
}


class KeyComparatorBigEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    loader_ = std::make_shared<ustore::LocalChunkLoader>();

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
        kvs_.push_back({{key, entry_size_}, {val, entry_size_}});
      }
    }  // end 2 fors

    std::unique_ptr<const ustore::Segment> seg =
        ustore::MapNode::Encode(kvs_);

    ustore::LocalChunkWriter writer;
    ustore::NodeBuilder nb(&writer, ustore::MapChunker::Instance(),
                           ustore::MetaChunker::Instance(), false);

    nb.SpliceElements(0, seg.get());
    rhs_root_ = nb.Commit();

    rhs_differ_ = new ustore::KeyDiffer(rhs_root_, loader_.get());
    rhs_intersector_ = new ustore::KeyIntersector(rhs_root_, loader_.get());
    rhs_mapper_ = new ustore::KeyMapper(rhs_root_, loader_.get());
  }

  uint64_t numElements(const ustore::Hash& root) {
    const ustore::Chunk* chunk = loader_->Load(root);
    auto node = ustore::SeqNode::CreateFromChunk(chunk);
    return node->numElements();
  }

  virtual void TearDown() {
    delete rhs_differ_;
    delete rhs_intersector_;
    delete rhs_mapper_;

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

  const ustore::KeyDiffer* rhs_differ_;
  const ustore::KeyIntersector* rhs_intersector_;
  const ustore::KeyMapper* rhs_mapper_;
};


TEST_F(KeyComparatorBigEnv, Basic) {
  ustore::LocalChunkWriter writer;
  // lhs is constructed by
  //   replacing 100 kvitems from the 400th
  //   removing 200 kvitems from the 100th item

  const ustore::OrderedKey key400{false, keys_[400], entry_size_};

  std::vector<ustore::KVItem> new_items;
  for (size_t i = 400; i < 400 + 100; i++) {
    new_items.push_back({{keys_[i], entry_size_}, {vals_[i+1], entry_size_}});
  }
  auto seg1 = ustore::MapNode::Encode(new_items);

  ustore::NodeBuilder nb1(rhs_root_, key400, loader_.get(), &writer,
                          ustore::MapChunker::Instance(),
                          ustore::MetaChunker::Instance(), false);

  nb1.SpliceElements(100, seg1.get());
  ustore::Hash lhs_t = nb1.Commit();
  ASSERT_EQ(num_items_, numElements(lhs_t));

  const ustore::OrderedKey key100{false, keys_[100], entry_size_};

  ustore::NodeBuilder nb2(lhs_t, key100, loader_.get(), &writer,
                          ustore::MapChunker::Instance(),
                          ustore::MetaChunker::Instance(), false);
  ustore::VarSegment seg2(nullptr, 0, {});

  nb2.SpliceElements(200, &seg2);
  ustore::Hash lhs = nb2.Commit();
  ASSERT_EQ(num_items_ - 200, numElements(lhs));


  //  lhs DIFF rhs
  std::vector<ustore::IndexRange> df_ranges = rhs_differ_->Compare(lhs);

  ASSERT_EQ(size_t(1), df_ranges.size());

  EXPECT_EQ(size_t(200), df_ranges[0].start_idx);
  EXPECT_EQ(size_t(100), df_ranges[0].num_subsequent);


  // lhs INTERSECT rhs
  std::vector<ustore::IndexRange> intersect_ranges
      = rhs_intersector_->Compare(lhs);

  ASSERT_EQ(size_t(2), intersect_ranges.size());

  EXPECT_EQ(size_t(0), intersect_ranges[0].start_idx);
  EXPECT_EQ(size_t(200), intersect_ranges[0].num_subsequent);

  EXPECT_EQ(size_t(300), intersect_ranges[1].start_idx);
  EXPECT_EQ(num_items_ - 200 - 300, intersect_ranges[1].num_subsequent);


  // lhs MAP rhs
  std::vector<std::pair<ustore::IndexRange, ustore::IndexRange>> range_maps
      = rhs_mapper_->Compare(lhs);

  ASSERT_EQ(size_t(3), range_maps.size());

  // lhs: {0, 100} -> rhs: {0, 100}
  EXPECT_EQ(size_t(0), range_maps[0].first.start_idx);
  EXPECT_EQ(size_t(100), range_maps[0].first.num_subsequent);

  EXPECT_EQ(size_t(0), range_maps[0].second.start_idx);
  EXPECT_EQ(size_t(100), range_maps[0].second.num_subsequent);

  // lhs: {100, 100} -> rhs: {300, 100}
  EXPECT_EQ(size_t(100), range_maps[1].first.start_idx);
  EXPECT_EQ(size_t(100), range_maps[1].first.num_subsequent);

  EXPECT_EQ(size_t(300), range_maps[1].second.start_idx);
  EXPECT_EQ(size_t(100), range_maps[1].second.num_subsequent);

  // lhs: {300, end} -> rhs: {500, end}
  EXPECT_EQ(size_t(300), range_maps[2].first.start_idx);
  EXPECT_EQ(num_items_ - 200 - 300, range_maps[2].first.num_subsequent);

  EXPECT_EQ(size_t(500), range_maps[2].second.start_idx);
  EXPECT_EQ(num_items_ - 500, range_maps[2].second.num_subsequent);
}


TEST(LevenshteinMapper, Simple) {
  const ustore::Slice k("k", 1);
  const ustore::Slice s("s", 1);
  const ustore::Slice i("i", 1);
  const ustore::Slice t("t", 1);
  const ustore::Slice e("e", 1);
  const ustore::Slice n("n", 1);
  const ustore::Slice g("g", 1);

  std::unique_ptr<const ustore::Segment> lhs_init_seg =
      ustore::ListNode::Encode({k, i, t, e, n});

  std::unique_ptr<const ustore::Segment> rhs_init_seg =
      ustore::ListNode::Encode({s, i, t, i, n, g});

/* Edi distance matrix shall be as follows:
+>: RHS Insertion   +^: LHS Insertion
++: Subsitution     **: Match

                               R H S
           0       s       i       t        i       n      g
  ---------------------------------------------------------------
    0  | (oo, 0) (+>, 1) (+>, 2) (+>, 3) (+>, 4) (+>, 5) (+>, 6)
L   k  | (+^, 1) (++, 1) (++, 2) (++, 3) (++, 4) (++, 5) (++, 6)
H   i  | (+^, 2) (++, 2) (**, 1) (+>, 2) (**, 3) (+>, 4) (+>, 5)
S   t  | (+^, 3) (++, 3) (+^, 2) (**, 1) (+>, 2) (+>, 3) (+>, 4)
    e  | (+^, 4) (++, 4) (+^, 3) (+^, 2) (++, 2) (++, 3) (++, 4)
    n  | (+^, 5) (++, 5) (+^, 4)(|^,  3) (++, 3) (**, 2) (+>, 3)

Result Map: LHS => RHS
(1, 2) => (1, 2)
(4, 1) => (4, 1)
*/

  ustore::LocalChunkWriter writer;
  auto loader = std::make_shared<ustore::LocalChunkLoader>();

  ustore::NodeBuilder lhs_nb(&writer, ustore::ListChunker::Instance(),
                             ustore::MetaChunker::Instance(), false);
  lhs_nb.SpliceElements(0, lhs_init_seg.get());
  const ustore::Hash lhs_root = lhs_nb.Commit();

  ustore::NodeBuilder rhs_nb(&writer, ustore::ListChunker::Instance(),
                             ustore::MetaChunker::Instance(), false);
  rhs_nb.SpliceElements(0, rhs_init_seg.get());
  const ustore::Hash rhs_root = rhs_nb.Commit();

  ustore::LevenshteinMapper mapper(rhs_root, loader.get());
  ustore::RangeMaps result = mapper.Compare(lhs_root);

  ASSERT_EQ(size_t(2), result.size());
  ustore::IndexRange lhs_range = result[0].first;
  ustore::IndexRange rhs_range = result[0].second;

  EXPECT_EQ(uint64_t(1), lhs_range.start_idx);
  EXPECT_EQ(uint64_t(2), lhs_range.num_subsequent);

  EXPECT_EQ(uint64_t(1), rhs_range.start_idx);
  EXPECT_EQ(uint64_t(2), rhs_range.num_subsequent);

  lhs_range = result[1].first;
  rhs_range = result[1].second;

  EXPECT_EQ(uint64_t(4), lhs_range.start_idx);
  EXPECT_EQ(uint64_t(1), lhs_range.num_subsequent);

  EXPECT_EQ(uint64_t(4), rhs_range.start_idx);
  EXPECT_EQ(uint64_t(1), rhs_range.num_subsequent);
}

TEST(LevenshteinMapper, Complex) {
  std::vector<const ustore::byte_t*> vals;
  std::vector<ustore::Slice> lhs_init_slices;
  for (uint32_t i = 0; i < 1024; i++) {
    ustore::byte_t* val = new ustore::byte_t[sizeof(uint32_t)];
    std::memcpy(val, &i, sizeof(uint32_t));
    vals.push_back(val);
    lhs_init_slices.push_back(ustore::Slice(val, sizeof(uint32_t)));
  }  // end 2 fors

  std::vector<ustore::Slice> rhs_insert_slices;
  for (uint32_t i = 2048; i < 2148; i++) {
    ustore::byte_t* val = new ustore::byte_t[sizeof(uint32_t)];
    std::memcpy(val, &i, sizeof(uint32_t));
    vals.push_back(val);
    rhs_insert_slices.push_back(ustore::Slice(val, sizeof(uint32_t)));
  }  // end for

/* RHS is constructed by removing 100 elements from LHS 100th
     And THEN inserting 100 elements at LHS's 600 idx.

LHS: [0 ... 100 ... 200 ... 600     600 ... 1024]
      |------|       | ----- |       | ----- |
RHS: [0 ... 100     100 ... 500 *** 600 ... 1024]

Final Result Map: LHS => RHS
  (0, 100) => (0, 100)
  (200, 400) => (100, 400)
  (600, 424) => (600, 424)
*/

  std::vector<ustore::Slice> rhs_init_slices;
  rhs_init_slices.insert(rhs_init_slices.end(),
                         lhs_init_slices.begin(),
                         lhs_init_slices.begin() + 100);

  rhs_init_slices.insert(rhs_init_slices.end(),
                         lhs_init_slices.begin() + 200,
                         lhs_init_slices.begin() + 600);

  rhs_init_slices.insert(rhs_init_slices.end(),
                         rhs_insert_slices.begin(),
                         rhs_insert_slices.end());

  rhs_init_slices.insert(rhs_init_slices.end(),
                         lhs_init_slices.begin() + 600,
                         lhs_init_slices.end());

// Construct tree
  std::unique_ptr<const ustore::Segment> lhs_init_seg =
      ustore::ListNode::Encode(lhs_init_slices);

  std::unique_ptr<const ustore::Segment> rhs_init_seg =
      ustore::ListNode::Encode(rhs_init_slices);

  ustore::LocalChunkWriter writer;
  auto loader = std::make_shared<ustore::LocalChunkLoader>();

  ustore::NodeBuilder lhs_nb(&writer, ustore::ListChunker::Instance(),
                             ustore::MetaChunker::Instance(), false);
  lhs_nb.SpliceElements(0, lhs_init_seg.get());
  const ustore::Hash lhs_root = lhs_nb.Commit();

  ustore::NodeBuilder rhs_nb(&writer, ustore::ListChunker::Instance(),
                             ustore::MetaChunker::Instance(), false);
  rhs_nb.SpliceElements(0, rhs_init_seg.get());
  const ustore::Hash rhs_root = rhs_nb.Commit();

  ustore::LevenshteinMapper mapper(rhs_root, loader.get());
  ustore::RangeMaps result = mapper.Compare(lhs_root);

  ASSERT_EQ(size_t(3), result.size());
  ustore::IndexRange lhs_range = result[0].first;
  ustore::IndexRange rhs_range = result[0].second;

  EXPECT_EQ(uint64_t(0), lhs_range.start_idx);
  EXPECT_EQ(uint64_t(100), lhs_range.num_subsequent);

  EXPECT_EQ(uint64_t(0), rhs_range.start_idx);
  EXPECT_EQ(uint64_t(100), rhs_range.num_subsequent);

  lhs_range = result[1].first;
  rhs_range = result[1].second;

  EXPECT_EQ(uint64_t(200), lhs_range.start_idx);
  EXPECT_EQ(uint64_t(400), lhs_range.num_subsequent);

  EXPECT_EQ(uint64_t(100), rhs_range.start_idx);
  EXPECT_EQ(uint64_t(400), rhs_range.num_subsequent);

  lhs_range = result[2].first;
  rhs_range = result[2].second;

  EXPECT_EQ(uint64_t(600), lhs_range.start_idx);
  EXPECT_EQ(uint64_t(424), lhs_range.num_subsequent);

  EXPECT_EQ(uint64_t(600), rhs_range.start_idx);
  EXPECT_EQ(uint64_t(424), rhs_range.num_subsequent);

  for (const ustore::byte_t* val : vals) delete[] val;
}
