// Copyright (c) 2017 The Ustore Authors.

#ifdef TEST_NODEBUILDER
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "node/blob_node.h"
#include "node/chunker.h"
#include "node/node_builder.h"
#include "node/cursor.h"
#include "store/chunk_loader.h"
#include "utils/debug.h"
#include "utils/logging.h"
#include "utils/singleton.h"

class NodeBuilderEnv : public ::testing::Test {
 protected:
  virtual void SetUp() { loader_ = new ustore::ChunkLoader(); }

  virtual void TearDown() { delete loader_; }

  void Test_Same_Content(const ustore::Hash& root_hash,
                         ustore::ChunkLoader* loader,
                         const ustore::byte_t* expected_data) {
    // currently the leaf_node is only blob type
    ustore::NodeCursor* leaf_cursor =
        ustore::NodeCursor::GetCursorByIndex(root_hash, 0, loader);
    ASSERT_TRUE(leaf_cursor != nullptr);
    ASSERT_EQ(0, leaf_cursor->idx());
    ASSERT_FALSE(leaf_cursor->isEnd());

    size_t offset = 0;
    do {
      ASSERT_EQ(*(expected_data + offset), *leaf_cursor->current())
          << "At offset: " << offset;
      ++offset;
    } while (leaf_cursor->Advance(true));

    delete leaf_cursor;
  }

  // Testing created prolly tree satisfiying desired property
  void Test_Tree_Integrity(const ustore::Hash& root_hash,
                           ustore::ChunkLoader* loader) {
    ustore::NodeCursor* leaf_cursor =
        ustore::NodeCursor::GetCursorByIndex(root_hash, 0, loader);
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

    const ustore::BlobChunker* chunker =
        ustore::Singleton<ustore::BlobChunker>::Instance();
    ustore::NodeBuilder builder = ustore::NodeBuilder(chunker, true);
    ustore::FixedSegment seg(original_content_, num_original_bytes_, 1);
    builder.SpliceElements(0, &seg);
    root_chunk = builder.Commit();
  }

  void Test_Splice(size_t splice_idx, ustore::byte_t* insert_bytes,
                   size_t num_insert_bytes, size_t num_delete_bytes,
                   bool isVerbose = false) {
    verbose = isVerbose;
    const ustore::BlobChunker* chunker =
        ustore::Singleton<ustore::BlobChunker>::Instance();
    ustore::NodeBuilder* b = ustore::NodeBuilder::NewNodeBuilderAtIndex(
        root_chunk->hash(), splice_idx, loader_, chunker, true);

    ustore::FixedSegment seg(insert_bytes, num_insert_bytes, 1);

    b->SpliceElements(num_delete_bytes, &seg);
    const ustore::Chunk* c = b->Commit();

    Test_Tree_Integrity(c->hash(), loader_);
    const ustore::byte_t* expected_data =
        ustore::SpliceBytes(original_content_, num_original_bytes_, splice_idx,
                            num_delete_bytes, insert_bytes, num_insert_bytes);
    Test_Same_Content(c->hash(), loader_, expected_data);

    delete[] expected_data;
    delete b;
    delete c;
  }

  virtual void TearDown() {
    NodeBuilderEnv::TearDown();
    delete[] original_content_;
    delete root_chunk;
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
  ustore::InitLogging("");
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
  Test_Splice(20, insert_slice, 3, 0);
  // delete 1 byte and insert
  // "abcdefghijklmn" ==> "abcdefghijklmnxy";
  Test_Splice(20, insert_slice, 3, 1);
  // delete 3 bytes
  // "abcdefghijklmn" ==> "abcdefghijklmn";
  Test_Splice(20, nullptr, 0, 3);
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
                      1                      (# of Entry in MetaChunk)
  |---------------------------------------|
   1                  8                   1   (# of Entry in MetaChunk)
  |-| | ------------------------------|  |-|
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

    const ustore::Chunker* chunker =
        ustore::Singleton<ustore::BlobChunker>::Instance();
    ustore::NodeBuilder builder = ustore::NodeBuilder(chunker, true);

    ustore::FixedSegment seg(original_content_, original_num_bytes_, 1);

    builder.SpliceElements(0, &seg);
    root_chunk_ = builder.Commit();
  }

  void SetUpSpecialTree() {
    // Construct special tree with both implicit and explicit boundary in the
    // end
    const ustore::Chunker* chunker =
        ustore::Singleton<ustore::BlobChunker>::Instance();
    // length of first three leaf chunks of above tree
    special_num_bytes_ = 67 + 38 + 193;
    special_content_ = new ustore::byte_t[special_num_bytes_];
    std::memcpy(special_content_, original_content_, special_num_bytes_);

    ustore::FixedSegment sseg(special_content_, special_num_bytes_, 1);
    ustore::NodeBuilder abuilder = ustore::NodeBuilder(chunker, true);

    abuilder.SpliceElements(0, &sseg);
    special_root_ = abuilder.Commit();
  }

  void test_splice_case(const ustore::Hash& root_hash, size_t splice_idx,
                        const ustore::byte_t* append_data,
                        size_t append_num_bytes, size_t num_delete) {
    const ustore::Chunker* chunker =
        ustore::Singleton<ustore::BlobChunker>::Instance();

    ustore::NodeBuilder* b = ustore::NodeBuilder::NewNodeBuilderAtIndex(
        root_hash, splice_idx, loader_, chunker, true);

    ustore::FixedSegment seg(append_data, append_num_bytes, 1);

    b->SpliceElements(num_delete, &seg);
    const ustore::Chunk* c = b->Commit();

    Test_Tree_Integrity(c->hash(), loader_);
    const ustore::byte_t* d =
        ustore::SpliceBytes(original_content_, original_num_bytes_, splice_idx,
                            num_delete, append_data, append_num_bytes);

    Test_Same_Content(c->hash(), loader_, d);

    delete b;
    delete c;
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
    delete root_chunk_;

    delete[] special_content_;
    delete special_root_;
  }

  const ustore::Chunk* root_chunk_;
  ustore::byte_t* original_content_;
  size_t original_num_bytes_;

  const ustore::Chunk* special_root_;
  ustore::byte_t* special_content_;
  size_t special_num_bytes_;
};

TEST_F(NodeBuilderComplex, InitTree) {
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
  Test_Splice(root_chunk_->hash(), 5000);
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

#endif  // TEST_NODEBUILDER
