// Copyright (c) 2017 The Ustore Authors.

#ifdef TEST_NODEBUILDER

#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "node/blob_node.h"
#include "node/node_builder.h"

#include "utils/debug.h"
// #include "utils/logging.h"

class NodeBuilderEnv : public ::testing::Test {
 protected:
  virtual void SetUp() { loader_ = new ustore::ServerChunkLoader(); }

  virtual void TearDown() {
    delete loader_;
    delete rhasher_;
  }

  void Test_Same_Content(const ustore::Hash& root_hash,
                         ustore::ChunkLoader* loader,
                         const ustore::byte_t* expected_data) {
    // currently the leaf_node is only blob type
    ustore::NodeCursor leaf_cursor(root_hash, 0, loader);
    ASSERT_TRUE(!leaf_cursor.empty());
    ASSERT_EQ(0, leaf_cursor.idx());
    ASSERT_FALSE(leaf_cursor.isEnd());

    size_t offset = 0;
    do {
      ASSERT_EQ(*(expected_data + offset), *leaf_cursor.current())
          << "At offset: " << offset;
      ++offset;
    } while (leaf_cursor.Advance(true));
  }

  // Testing created prolly tree satisfiying desired property
  void Test_Tree_Integrity(const ustore::Hash& root_hash,
                           ustore::ChunkLoader* loader) {
    ustore::NodeCursor* leaf_cursor = new
        ustore::NodeCursor(root_hash, 0, loader);
    // go through created chunks from lowest level until root
    // check rolling hasher boundary pattern is only detected
    //   at end of chunk, except the last chunk.
    ustore::NodeCursor* cursor = leaf_cursor;
    ustore::NodeCursor* parent_cur = nullptr;
    size_t level = 0;
    while (cursor != nullptr) {
      // copy the parent cursor first,
      //   which now places at seq start of its level
      if (cursor->parent() == nullptr) {
        parent_cur = nullptr;
      } else {
        parent_cur = new ustore::NodeCursor(*(cursor->parent()));
      }
      // Init a rolling hasher for each seq
      delete rhasher_;
      rhasher_ = ustore::RollingHasher::TestHasher();

      ASSERT_EQ(cursor->idx(), 0);

      std::vector<size_t> chunks_num_elements;
      std::vector<size_t> chunks_num_bytes;
      std::ostringstream stm;
      bool atSeqEnd = false;

      do {
        bool atBoundary = false;
        size_t chunk_num_elements = 0;
        size_t chunk_num_bytes = 0;

        while (true) {
          ++chunk_num_elements;
          chunk_num_bytes += cursor->numCurrentBytes();

          rhasher_->HashBytes(cursor->current(), cursor->numCurrentBytes());
          atBoundary = rhasher_->CrossedBoundary();

          if (cursor->Advance(false)) {
            ASSERT_FALSE(atBoundary)
                << "Level: " << level
                << " Chunk Id: " << chunks_num_elements.size()
                << " Entry Id: " << chunk_num_elements - 1;
          } else {
            // LOG(INFO) << "Level: " << level
            //           << " Chunk ID: " << chunks_num_elements.size()
            //           << " Chunk # Elements: " << chunk_num_elements;
            break;
          }  // end if
        }    // end while
        chunks_num_elements.push_back(chunk_num_elements);
        chunks_num_bytes.push_back(chunk_num_bytes);

        // now cursor points to the chunk end
        // attemp to advance across the boundary
        atSeqEnd = !cursor->Advance(true);
        if (!atSeqEnd) {
          // Cursor now points the next chunk start
          // rhasher shall detect a boundary pattern
          //   from last chunk end.
          ASSERT_TRUE(atBoundary) << "Level " << level << " Chunk "
                                  << chunks_num_elements.size() - 1;
          rhasher_->ClearLastBoundary();
        }  // end if
      } while (!atSeqEnd);

      stm << "Level " << level;
      if (level == 0) {
        stm << " (Leaf): ";
      } else {
        stm << ": ";
      }

      for (size_t i = 0; i < chunks_num_bytes.size(); i++) {
        stm << chunks_num_elements[i];
        stm << " [" << chunks_num_bytes[i] << "]  ";
      }
      if (verbose) {
        LOG(INFO) << stm.str();
      }

      chunks_num_bytes.clear();
      chunks_num_elements.clear();
      ++level;
      delete cursor;
      cursor = parent_cur;
    }  // end while cursor
  }

  ustore::RollingHasher* rhasher_ = nullptr;
  ustore::ChunkLoader* loader_ = nullptr;
  bool verbose = false;
};

// a prolly tree with single root node
class NodeBuilderSimple : public NodeBuilderEnv {
 protected:
  virtual void SetUp() {
    NodeBuilderEnv::SetUp();
    verbose = false;
    const ustore::byte_t raw_data[] = "abcdefghijklmn";

    num_original_bytes_ = sizeof(raw_data) - 1;
    original_content_ = new ustore::byte_t[num_original_bytes_];
    std::memcpy(original_content_, raw_data, num_original_bytes_);

    const ustore::BlobChunker* chunker = ustore::BlobChunker::Instance();
    ustore::NodeBuilder builder(chunker, true);
    ustore::FixedSegment seg(original_content_, num_original_bytes_, 1);
    builder.SpliceElements(0, &seg);
    root_chunk = loader_->Load(builder.Commit());
  }

  void Test_Splice(size_t splice_idx, ustore::byte_t* insert_bytes,
                   size_t num_insert_bytes, size_t num_delete_bytes,
                   bool isVerbose = false) {
    verbose = isVerbose;
    ustore::NodeBuilder b(root_chunk->hash(), splice_idx, loader_,
                          ustore::BlobChunker::Instance(), true);

    ustore::FixedSegment seg(insert_bytes, num_insert_bytes, 1);

    b.SpliceElements(num_delete_bytes, &seg);
    const ustore::Chunk* c = loader_->Load(b.Commit());

    Test_Tree_Integrity(c->hash(), loader_);
    const ustore::byte_t* expected_data =
        ustore::SpliceBytes(original_content_, num_original_bytes_, splice_idx,
                            num_delete_bytes, insert_bytes, num_insert_bytes);
    Test_Same_Content(c->hash(), loader_, expected_data);

    delete[] expected_data;
  }

  virtual void TearDown() {
    NodeBuilderEnv::TearDown();
    delete[] original_content_;
  }

  const ustore::Chunk* root_chunk;

  size_t num_original_bytes_;
  ustore::byte_t* original_content_;
};

TEST_F(NodeBuilderSimple, InitTree) {
  verbose = false;
  Test_Tree_Integrity(root_chunk->hash(), loader_);
  Test_Same_Content(root_chunk->hash(), loader_, original_content_);
}

TEST_F(NodeBuilderSimple, SpliceStart) {
  // Test on insertion at start point
  ustore::byte_t insert_slice[] = "xyz";
  // insert only
  // "abcdefghijklmn" ==> "xyzabcdefghijklmn";
  Test_Splice(0, insert_slice, 3, 0);
  // delete 1 byte and insert
  // "abcdefghijklmn" ==> "xyzbcdefghijklmn";
  Test_Splice(0, insert_slice, 3, 1);
  // delete 3 bytes
  // "abcdefghijklmn" ==> "defghijklmn";
  Test_Splice(0, nullptr, 0, 3);
}

TEST_F(NodeBuilderSimple, SpliceMiddle) {
  ustore::byte_t insert_slice[] = "mn";
  // insert only
  // "abcdefghijklmn" ==> "abcdemnfghijklmn";
  Test_Splice(5, insert_slice, 3, 0);
  // delete 1 byte and insert
  // "abcdefghijklmn" ==> "abcdemnghijklmn";
  Test_Splice(5, insert_slice, 3, 1);
  // delete 3 bytes
  // "abcdefghijklmn" ==> "abcdeijklmn";
  Test_Splice(5, nullptr, 0, 3);
}

TEST_F(NodeBuilderSimple, SpliceFromEnd) {
  ustore::byte_t insert_slice[] = "xy";
  // insert only
  // "abcdefghijklmn" ==> "abcdefghijklmnxy";
  Test_Splice(14, insert_slice, 3, 0);
  // delete 1 byte and insert
  // "abcdefghijklmn" ==> "abcdefghijklmnxy";
  Test_Splice(14, insert_slice, 3, 1);
  // delete 3 bytes
  // "abcdefghijklmn" ==> "abcdefghijklmn";
  Test_Splice(14, nullptr, 0, 3);
}

TEST_F(NodeBuilderSimple, SpliceToEnd) {
  ustore::byte_t insert_slice[] = "xyz";
  // delete 3 byte and insert
  // "abcdefghijklmn" ==> "abcdefghijklxyz";
  Test_Splice(12, insert_slice, 3, 3);
  // delete 3 bytes
  // "abcdefghijklmn" ==> "abcdefghijkl";
  Test_Splice(12, nullptr, 0, 3);
}

class NodeBuilderComplex : public NodeBuilderEnv {
 protected:
  virtual void SetUp() {
    NodeBuilderEnv::SetUp();
    verbose = false;
    SetupNormalTree();
    SetUpSpecialTree();
  }

  /* Setup a normal Prolley tree as followed:
                      2                       (# of Entry in MetaChunk)
  |---------------------------------------|
    2                   8                     (# of Entry in MetaChunk)
  |--- |  |-------------------------------|
  67  38  193  183  512  320  53  55  74  256 (# of Byte in BlobChunk)
  */
  virtual void SetupNormalTree() {
    const ustore::byte_t raw_data[] = {
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

    original_num_bytes_ = sizeof(raw_data) - 1;
    original_content_ = new ustore::byte_t[original_num_bytes_];
    std::memcpy(original_content_, raw_data, original_num_bytes_);

    const ustore::Chunker* chunker = ustore::BlobChunker::Instance();
    ustore::NodeBuilder builder(chunker, true);

    ustore::FixedSegment seg(original_content_, original_num_bytes_, 1);

    builder.SpliceElements(0, &seg);
    root_chunk_ = loader_->Load(builder.Commit());
  }

  void SetUpSpecialTree() {
    // Construct special tree with both implicit and explicit boundary in the
    // end
    const ustore::Chunker* chunker = ustore::BlobChunker::Instance();
    // length of first three leaf chunks of above tree
    special_num_bytes_ = 67 + 38 + 193;
    special_content_ = new ustore::byte_t[special_num_bytes_];
    std::memcpy(special_content_, original_content_, special_num_bytes_);

    ustore::FixedSegment sseg(special_content_, special_num_bytes_, 1);
    ustore::NodeBuilder abuilder(chunker, true);

    abuilder.SpliceElements(0, &sseg);
    special_root_ = loader_->Load(abuilder.Commit());
  }

  void test_splice_case(const ustore::Hash& root_hash, size_t splice_idx,
                        const ustore::byte_t* append_data,
                        size_t append_num_bytes, size_t num_delete) {
    const ustore::Chunker* chunker = ustore::BlobChunker::Instance();

    ustore::NodeBuilder b(
        root_hash, splice_idx, loader_, chunker, true);

    ustore::FixedSegment seg(append_data, append_num_bytes, 1);

    b.SpliceElements(num_delete, &seg);
    const ustore::Chunk* c = loader_->Load(b.Commit());

    Test_Tree_Integrity(c->hash(), loader_);
    const ustore::byte_t* d =
        ustore::SpliceBytes(original_content_, original_num_bytes_, splice_idx,
                            num_delete, append_data, append_num_bytes);

    Test_Same_Content(c->hash(), loader_, d);

    delete[] d;
  }

  // test on three cases of splice from splice_idx element
  //   1. Normal Splice: First remove and then append elements
  //   2. No removal just appending
  //   3. No appending, just removal
  void Test_Splice(const ustore::Hash& root_hash, size_t splice_idx,
                   bool isVerbose = false) {
    verbose = isVerbose;
    const ustore::byte_t append_data[] = {
        "Commoners FLAVIUS Hence! home, you idle creatures get you home: Is "
        "this "
        "a holiday? what! know you not, Being mechanical, you ought not walk "
        "Upon a labouring day without the sign Of your profession? Speak, what "
        "trade art thou?  First Commoner Why, sir, a carpenter.  MARULLUS "
        "Where "
        "is thy leather apron and thy rule?  What dost thou with thy best "
        "apparel on?  You, sir, what trade are you?  Second Commoner Truly, "
        "sir, "
        "in respect of a fine workman, I am but, as you would say, a cobbler."};

    size_t append_num_bytes = sizeof(append_data) - 1;

    // Delete and Insert
    test_splice_case(root_hash, splice_idx, append_data, append_num_bytes, 500);
    // Insert Only
    test_splice_case(root_hash, splice_idx, append_data, append_num_bytes, 0);
    // // Delete Only
    test_splice_case(root_hash, splice_idx, nullptr, 0, 500);
  }

  virtual void TearDown() {
    NodeBuilderEnv::TearDown();
    delete[] original_content_;
    delete[] special_content_;
  }

  const ustore::Chunk* root_chunk_;
  ustore::byte_t* original_content_;
  size_t original_num_bytes_;

  const ustore::Chunk* special_root_;
  ustore::byte_t* special_content_;
  size_t special_num_bytes_;
};

TEST_F(NodeBuilderComplex, InitTree) {
  // verbose = true;
  Test_Tree_Integrity(root_chunk_->hash(), loader_);
  Test_Same_Content(root_chunk_->hash(), loader_, original_content_);
}

TEST_F(NodeBuilderComplex, SpliceSeqStart) {
  Test_Splice(root_chunk_->hash(), 0);
}

TEST_F(NodeBuilderComplex, SpliceSeqMiddle) {
  Test_Splice(root_chunk_->hash(), 244);
}

TEST_F(NodeBuilderComplex, SpliceSeqEnd) {
  Test_Splice(root_chunk_->hash(), original_num_bytes_);
}

TEST_F(NodeBuilderComplex, SpliceChunkStart) {
  // 298 the index of first element in 3rd chunk
  Test_Splice(root_chunk_->hash(), 298);
}

TEST_F(NodeBuilderComplex, SpliceChunkEnd) {
  // 480th-entry is the first entry 4th leaf chunk
  Test_Splice(root_chunk_->hash(), 480);
}

// Test on special ptree
//   with both implicit and explicit tree at seq end
TEST_F(NodeBuilderComplex, Special) {
  Test_Splice(special_root_->hash(), special_num_bytes_);
}

// a prolly tree with single root node
class AdvancedNodeBuilderEmpty : public NodeBuilderEnv {
 protected:
  virtual void SetUp() {
    NodeBuilderEnv::SetUp();
    verbose = false;

    ustore::AdvancedNodeBuilder builder;
    root_hash_ = builder.Commit(*ustore::BlobChunker::Instance(), true);
  }

  virtual void TearDown() {
    NodeBuilderEnv::TearDown();
  }

  ustore::Hash root_hash_;
};

TEST_F(AdvancedNodeBuilderEmpty, Init) {
  const ustore::Chunk* chunk = loader_->Load(root_hash_);
  auto node = ustore::SeqNode::CreateFromChunk(chunk);

  ASSERT_EQ(size_t(0), node->numEntries());
}

TEST_F(AdvancedNodeBuilderEmpty, InsertSpliceDelete) {
  // First insert into empty tree
  const ustore::byte_t inserted_data[] = "123";
  const size_t num_bytes_inserted = 3;

  ustore::AdvancedNodeBuilder builder1(root_hash_, loader_);

  ustore::FixedSegment inserted_seg(inserted_data,
                                    num_bytes_inserted,
                                    num_bytes_inserted);

  builder1.Insert(0, inserted_seg);

  const ustore::Hash hash1 =
      builder1.Commit(*ustore::BlobChunker::Instance(), true);

  Test_Tree_Integrity(hash1, loader_);
  Test_Same_Content(hash1, loader_, inserted_data);

  // Then remove all 3 characters and append '4567'
  const ustore::byte_t spliced_data[] = "4567";
  const size_t num_bytes_spliced = 4;
  const size_t num_bytes_removed = 3;

  ustore::AdvancedNodeBuilder builder2(hash1, loader_);

  ustore::FixedSegment spliced_seg(spliced_data,
                                   num_bytes_spliced,
                                   num_bytes_spliced);

  builder2.Splice(0, num_bytes_removed, spliced_seg);

  const ustore::Hash hash2 =
      builder2.Commit(*ustore::BlobChunker::Instance(), true);

  Test_Tree_Integrity(hash2, loader_);
  Test_Same_Content(hash2, loader_, spliced_data);

  ustore::AdvancedNodeBuilder builder3(hash2, loader_);

  builder3.Remove(0, 4);

  const ustore::Hash hash3 =
      builder3.Commit(*ustore::BlobChunker::Instance(), true);

  const ustore::Chunk* chunk = loader_->Load(hash3);
  auto node = ustore::SeqNode::CreateFromChunk(chunk);

  ASSERT_EQ(size_t(0), node->numEntries());
}

// a prolly tree with single root node
class AdvancedNodeBuilderSimple : public NodeBuilderEnv {
 protected:
  virtual void SetUp() {
    NodeBuilderEnv::SetUp();
    verbose = false;
    const ustore::byte_t raw_data[] = "abcdefghijklmn";

    num_original_bytes_ = sizeof(raw_data) - 1;
    original_content_ = new ustore::byte_t[num_original_bytes_];
    std::memcpy(original_content_, raw_data, num_original_bytes_);

    ustore::AdvancedNodeBuilder builder;
    ustore::FixedSegment seg(original_content_, num_original_bytes_, 1);
    builder.Splice(0, 0, seg);
    root_hash_ = builder.Commit(*ustore::BlobChunker::Instance(), true);
  }

  virtual void TearDown() {
    delete[] original_content_;
    NodeBuilderEnv::TearDown();
  }

  // test on a single splice operation
  // at the given idx
  void TestSingleSplice(size_t idx, bool isVerbose = false) {
    // idx must be in valid range
    ASSERT_LE(idx, num_original_bytes_);
    verbose = isVerbose;

    const ustore::byte_t inserted_data[] = "123";
    const size_t num_bytes_inserted = 3;
    const size_t num_bytes_removed = 1;

    size_t expected_num_bytes = 0;
    const ustore::byte_t* expected_result =
        ustore::MultiSplice(original_content_,
                            num_original_bytes_,
                            {idx}, {num_bytes_removed},
                            {inserted_data},
                            {num_bytes_inserted},
                            &expected_num_bytes);


    ustore::AdvancedNodeBuilder builder(root_hash_, loader_);

    ustore::FixedSegment seg(inserted_data,
                             num_bytes_inserted,
                             num_bytes_inserted);

    builder.Splice(idx, num_bytes_removed, seg);

    const ustore::Hash result_hash =
        builder.Commit(*ustore::BlobChunker::Instance(), true);

    std::string expected_str(
        reinterpret_cast<const char*>(expected_result),
        expected_num_bytes);
    DLOG(INFO) << "Expected Bytes in string: " <<  expected_str;

    Test_Tree_Integrity(result_hash, loader_);
    Test_Same_Content(result_hash, loader_, expected_result);

    delete[] expected_result;
  }

  void TestDoubleSplice(size_t idx1, size_t idx2,
                        bool isVerbose = false) {


    DLOG(INFO) << "\n\n Test on Double Splice.";
    verbose = isVerbose;

    const ustore::byte_t inserted_data1[] = "123";
    const size_t num_bytes_inserted1 = 3;
    const size_t num_bytes_removed1 = 1;

    const ustore::byte_t inserted_data2[] = "45";
    const size_t num_bytes_inserted2 = 2;
    const size_t num_bytes_removed2 = 2;

    // idx must be in valid range
    ASSERT_LE(size_t(0), idx1);
    ASSERT_LT(idx1 + num_bytes_removed1, idx2);
    ASSERT_LE(idx2, num_original_bytes_);


    size_t expected_num_bytes = 0;
    const ustore::byte_t* expected_result =
        ustore::MultiSplice(original_content_,
                            num_original_bytes_,
                            {idx1, idx2},
                            {num_bytes_removed1, num_bytes_removed2},
                            {inserted_data1, inserted_data2},
                            {num_bytes_inserted1, num_bytes_inserted2},
                            &expected_num_bytes);


    ustore::AdvancedNodeBuilder builder(root_hash_, loader_);

    ustore::FixedSegment seg1(inserted_data1,
                              num_bytes_inserted1,
                              num_bytes_inserted1);

    builder.Splice(idx1, num_bytes_removed1, seg1);

    ustore::FixedSegment seg2(inserted_data2,
                              num_bytes_inserted2,
                              num_bytes_inserted2);

    builder.Splice(idx2, num_bytes_removed2, seg2);

    const ustore::Hash result_hash =
        builder.Commit(*ustore::BlobChunker::Instance(), true);

    std::string expected_str(
        reinterpret_cast<const char*>(expected_result),
        expected_num_bytes);
    DLOG(INFO) << "Expected Bytes in string: " <<  expected_str;

    Test_Tree_Integrity(result_hash, loader_);
    Test_Same_Content(result_hash, loader_, expected_result);

    delete[] expected_result;
  }

  ustore::Hash root_hash_;

  size_t num_original_bytes_;
  ustore::byte_t* original_content_;
};

TEST_F(AdvancedNodeBuilderSimple, Init) {
  Test_Tree_Integrity(root_hash_, loader_);
  Test_Same_Content(root_hash_, loader_, original_content_);
}

TEST_F(AdvancedNodeBuilderSimple, SpliceHead) {
  TestSingleSplice(0);
}

TEST_F(AdvancedNodeBuilderSimple, SpliceMiddle) {
  TestSingleSplice(10);
}

TEST_F(AdvancedNodeBuilderSimple, SpliceEnd) {
  TestSingleSplice(num_original_bytes_);
}


TEST_F(AdvancedNodeBuilderSimple, SpliceHeadMiddle) {
  TestDoubleSplice(0, 10);
}

TEST_F(AdvancedNodeBuilderSimple, SpliceHeadEnd) {
  TestDoubleSplice(0, num_original_bytes_);
}

TEST_F(AdvancedNodeBuilderSimple, SpliceMiddleEnd) {
  TestDoubleSplice(10, num_original_bytes_);
}


class AdvancedNodeBuilderComplex : public NodeBuilderEnv {
 protected:
  virtual void SetUp() {
    NodeBuilderEnv::SetUp();
    verbose = false;
    const ustore::byte_t raw_data[] = {
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

    num_original_bytes_ = sizeof(raw_data) - 1;
    original_content_ = new ustore::byte_t[num_original_bytes_];
    std::memcpy(original_content_, raw_data, num_original_bytes_);

    ustore::AdvancedNodeBuilder builder;
    ustore::FixedSegment seg(original_content_, num_original_bytes_, 1);
    builder.Splice(0, 0, seg);
    root_hash_ = builder.Commit(*ustore::BlobChunker::Instance(), true);
  }

  virtual void TearDown() {
    delete[] original_content_;
    NodeBuilderEnv::TearDown();
  }

  void TestTripleRemove(size_t idx1, size_t idx2, size_t idx3,
                      bool isVerbose = false) {
    verbose = isVerbose;

    const size_t num_bytes_removed1 = 1;
    const size_t num_bytes_removed2 = 2;
    const size_t num_bytes_removed3 = 4;

    // Testing for validity
    ASSERT_LE(size_t(0), idx1);
    ASSERT_LE(idx1 + num_bytes_removed1, idx2);
    ASSERT_LE(idx2 + num_bytes_removed2, idx3);
    ASSERT_LE(idx3, num_original_bytes_);

    size_t expected_num_bytes = 0;
    const ustore::byte_t* expected_result =
        ustore::MultiSplice(original_content_,
                            num_original_bytes_,
                            {idx1, idx2, idx3},

                            {num_bytes_removed1,
                             num_bytes_removed2,
                             num_bytes_removed3},

                            {nullptr, nullptr, nullptr},
                            {0, 0, 0},
                            &expected_num_bytes);

    ustore::AdvancedNodeBuilder builder(root_hash_, loader_);

    const ustore::Hash result_hash =
        builder.Remove(idx1, num_bytes_removed1)
               .Remove(idx2, num_bytes_removed2)
               .Remove(idx3, num_bytes_removed3)
               .Commit(*ustore::BlobChunker::Instance(), true);

    Test_Same_Content(result_hash, loader_, expected_result);
    Test_Tree_Integrity(result_hash, loader_);

    delete[] expected_result;
  }

  // Test on splicing on idx1, idx2, id3
  // requirement on idxs
  // idx1 + 1 < idx2 and idx2 + 2 < idx3
  void TestTripleSplice(size_t idx1, size_t idx2, size_t idx3,
                        bool isVerbose = false) {
    verbose = isVerbose;

    const ustore::byte_t inserted_data1[] =
        ", indeed, sir, a surgeon to old shoes; when they are in";
    const size_t num_bytes_inserted1 = sizeof(inserted_data1) - 1;
    const size_t num_bytes_removed1 = 1;

    const ustore::byte_t inserted_data2[] =
        {"is thy leather apron and thy rule?  What dost thou with thy best"
        "apparel on?  You, sir, what trade are you?  Second Commoner Truly, "};
    const size_t num_bytes_inserted2 = sizeof(inserted_data2) - 1;
    const size_t num_bytes_removed2 = 2;

    const ustore::byte_t inserted_data3[] =
        {"SCENE I. Rome. A street.  Enter FLAVIUS, MARULLUS, and certain "
        "Commoners FLAVIUS Hence! home, you idle creatures get you home: Is "
        "this "};

    const size_t num_bytes_inserted3 = sizeof(inserted_data3) - 1;
    const size_t num_bytes_removed3 = 4;

    // Testing for validity
    ASSERT_LE(size_t(0), idx1);
    ASSERT_LE(idx1 + num_bytes_removed1, idx2);
    ASSERT_LE(idx2 + num_bytes_removed2, idx3);
    ASSERT_LE(idx3, num_original_bytes_);

    size_t expected_num_bytes = 0;
    const ustore::byte_t* expected_result =
        ustore::MultiSplice(original_content_,
                            num_original_bytes_,
                            {idx1, idx2, idx3},

                            {num_bytes_removed1,
                             num_bytes_removed2,
                             num_bytes_removed3},

                            {inserted_data1,
                             inserted_data2,
                             inserted_data3},

                            {num_bytes_inserted1,
                             num_bytes_inserted2,
                             num_bytes_inserted3},

                            &expected_num_bytes);



    ustore::FixedSegment seg1(inserted_data1,
                              num_bytes_inserted1,
                              1);

    ustore::FixedSegment seg2(inserted_data2,
                              num_bytes_inserted2,
                              1);


    ustore::FixedSegment seg3(inserted_data3,
                              num_bytes_inserted3,
                              1);

    ustore::AdvancedNodeBuilder builder(root_hash_, loader_);

    const ustore::Hash result_hash =
        builder.Splice(idx1, num_bytes_removed1, seg1)
               .Splice(idx2, num_bytes_removed2, seg2)
               .Splice(idx3, num_bytes_removed3, seg3)
               .Commit(*ustore::BlobChunker::Instance(), true);

    Test_Same_Content(result_hash, loader_, expected_result);
    Test_Tree_Integrity(result_hash, loader_);

    delete[] expected_result;
  }


  void TestTripleInsert(size_t idx1, size_t idx2, size_t idx3,
                        bool isVerbose = false) {
    verbose = isVerbose;

    const ustore::byte_t inserted_data1[] =
        ", indeed, sir, a surgeon to old shoes; when they are in";
    const size_t num_bytes_inserted1 = sizeof(inserted_data1) - 1;
    const size_t num_bytes_removed1 = 0;

    const ustore::byte_t inserted_data2[] =
        {"is thy leather apron and thy rule?  What dost thou with thy best"
        "apparel on?  You, sir, what trade are you?  Second Commoner Truly, "};
    const size_t num_bytes_inserted2 = sizeof(inserted_data2) - 1;
    const size_t num_bytes_removed2 = 0;

    const ustore::byte_t inserted_data3[] =
        {"SCENE I. Rome. A street.  Enter FLAVIUS, MARULLUS, and certain "
        "Commoners FLAVIUS Hence! home, you idle creatures get you home: Is "
        "this "};

    const size_t num_bytes_inserted3 = sizeof(inserted_data3) - 1;
    const size_t num_bytes_removed3 = 0;

    // Testing for validity
    ASSERT_LE(size_t(0), idx1);
    ASSERT_LE(idx1 + num_bytes_removed1, idx2);
    ASSERT_LE(idx2 + num_bytes_removed2, idx3);
    ASSERT_LE(idx3, num_original_bytes_);

    size_t expected_num_bytes = 0;
    const ustore::byte_t* expected_result =
        ustore::MultiSplice(original_content_,
                            num_original_bytes_,
                            {idx1, idx2, idx3},

                            {num_bytes_removed1,
                             num_bytes_removed2,
                             num_bytes_removed3},

                            {inserted_data1,
                             inserted_data2,
                             inserted_data3},

                            {num_bytes_inserted1,
                             num_bytes_inserted2,
                             num_bytes_inserted3},

                            &expected_num_bytes);

    ustore::FixedSegment seg1(inserted_data1,
                              num_bytes_inserted1,
                              1);

    ustore::FixedSegment seg2(inserted_data2,
                              num_bytes_inserted2,
                              1);


    ustore::FixedSegment seg3(inserted_data3,
                              num_bytes_inserted3,
                              1);

    ustore::AdvancedNodeBuilder builder(root_hash_, loader_);

    const ustore::Hash result_hash =
        builder.Insert(idx1, seg1)
               .Insert(idx2, seg2)
               .Insert(idx3, seg3)
               .Commit(*ustore::BlobChunker::Instance(), true);


    Test_Same_Content(result_hash, loader_, expected_result);
    Test_Tree_Integrity(result_hash, loader_);

    delete[] expected_result;
  }

  ustore::Hash root_hash_;
  size_t num_original_bytes_;
  ustore::byte_t* original_content_;
};

TEST_F(AdvancedNodeBuilderComplex, Init) {
  Test_Tree_Integrity(root_hash_, loader_);
  Test_Same_Content(root_hash_, loader_, original_content_);
}

/* Setup a normal Prolley tree as followed:
                    2                       (# of Entry in MetaChunk)
|---------------------------------------|
  2                   8                     (# of Entry in MetaChunk)
|--- |  |-------------------------------|
67  38  193  183  512  320  53  55  74  256 (# of Byte in BlobChunk) */

TEST_F(AdvancedNodeBuilderComplex, Case1) {
/* Case 1:
   The first two operation on first chunk
   The third on chunk far away
*/
  TestTripleSplice(0, 10, 150);
  TestTripleRemove(0, 10, 150);
  TestTripleInsert(0, 10, 150);
}

TEST_F(AdvancedNodeBuilderComplex, Case2) {
/* Case 2:
  Three operation on the first three chunks
*/
  TestTripleSplice(10, 100, 150);
  TestTripleRemove(10, 100, 150);
  TestTripleInsert(10, 100, 150);
}

TEST_F(AdvancedNodeBuilderComplex, Case3) {
/* Case 3:
  Three operation on the same chunk
*/
  TestTripleSplice(310, 330, 350);
  TestTripleRemove(310, 330, 350);
  TestTripleInsert(310, 330, 350);
}

TEST_F(AdvancedNodeBuilderComplex, Case4) {
/* Case 4:
  First and second operation on the same chunk
  Third operation on seq end
*/
  TestTripleSplice(310, 330, num_original_bytes_);
  TestTripleRemove(310, 330, num_original_bytes_);
  TestTripleInsert(310, 330, num_original_bytes_);
}

TEST_F(AdvancedNodeBuilderComplex, Case5) {
/* Case 5:
  Three operation on the first entry of thre continuous chunk
*/
  TestTripleSplice(67, 105, 293);
  TestTripleRemove(67, 105, 293);
  TestTripleInsert(67, 105, 293);
}

TEST_F(AdvancedNodeBuilderComplex, Case6) {
/* Case 6:
  Three operation on the last entry of thre continuous chunk
*/
  TestTripleSplice(66, 104, 292);
  TestTripleRemove(66, 104, 292);
  TestTripleInsert(66, 104, 292);
}

TEST_F(AdvancedNodeBuilderComplex, Case7) {
/* Case 7:
  Three operation on pure random
*/
  TestTripleSplice(166, 304, 492);
  TestTripleRemove(266, 504, 522);
  TestTripleInsert(626, 704, 892);
}

TEST_F(AdvancedNodeBuilderComplex, Case8) {
/* Case 8:
  Three operation continuously
*/
  TestTripleSplice(166, 167, 169);
  TestTripleRemove(266, 267, 269);
}

#endif  // TEST_NODEBUILDER
