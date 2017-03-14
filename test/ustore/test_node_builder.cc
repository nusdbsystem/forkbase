// Copyright (c) 2017 The Ustore Authors.

#ifdef TEST_NODEBUILDER
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "node/blob_node.h"
#include "node/node_builder.h"
#include "node/chunk_loader.h"
#include "node/cursor.h"

#include "utils/logging.h"
#include "utils/singleton.h"
#include "utils/debug.h"

#ifdef USE_LEVELDB
#include "store/ldb_store.h"

static ustore::Singleton<ustore::LDBStore> ldb;
#endif  // USE_LEVELDB

class NodeBuilderEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    #ifdef USE_LEVELDB
    ustore::ChunkStore* cs = ldb.Instance();
    #endif  // USE_LEVELDB
    loader_ = new ustore::ChunkLoader(cs);
  }

  virtual void TearDown() {
    delete loader_;
  }

  void Test_Same_Content(const ustore::Hash& root_hash,
                         ustore::ChunkLoader* loader,
                         const ustore::byte_t* expected_data) {
    // currently the leaf_node is only blob type
    ustore::NodeCursor* leaf_cursor =
                          ustore::NodeCursor::GetCursorByIndex(
                                                               root_hash,
                                                               0, loader);
    ASSERT_TRUE(leaf_cursor != nullptr);
    ASSERT_EQ(leaf_cursor->idx(), 0);
    ASSERT_FALSE(leaf_cursor->isEnd());

    size_t offset = 0;
    do {
      ASSERT_EQ(*leaf_cursor->current(), *(expected_data + offset))
                                        << "At offset: " << offset;
      ++offset;
    } while (!leaf_cursor->Advance(true));

    delete leaf_cursor;
  }

  // Testing created prolly tree satisfiying desired property
  void Test_Tree_Integrity(const ustore::Hash& root_hash,
                           ustore::ChunkLoader* loader) {
    ustore::NodeCursor* leaf_cursor =
                          ustore::NodeCursor::GetCursorByIndex(
                                                               root_hash,
                                                             0, loader);
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
        parent_cur =  nullptr;
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
            // LOG(INFO) << "Level: " << level
            //           << " Chunk ID: " << chunks_num_elements.size()
            //           << " Chunk # Elements: " << chunk_num_elements;
            break;
          } else {
            ASSERT_FALSE(atBoundary)
                         << "Level: " << level
                         << " Chunk Id: "
                         << chunks_num_elements.size()
                         << " Entry Id: "
                         << chunk_num_elements - 1;
          }  // end if
        }  // end while
        chunks_num_elements.push_back(chunk_num_elements);
        chunks_num_bytes.push_back(chunk_num_bytes);

        // now cursor points to the chunk end
        // attemp to advance across the boundary
        atSeqEnd = cursor->Advance(true);
        if (!atSeqEnd) {
          // Cursor now points the next chunk start
          // rhasher shall detect a boundary pattern
          //   from last chunk end.
          ASSERT_TRUE(atBoundary) << "Level " << level
                                  << " Chunk "
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
  bool verbose = true;
};

// a prolly tree with single root node
class NodeBuilderSimple:public NodeBuilderEnv {
 protected:
  virtual void SetUp() {
    NodeBuilderEnv::SetUp();
    verbose = false;
    const ustore::byte_t raw_data[] = "abcdefghijklmn";

    size_t num_bytes = sizeof(raw_data) - 1;
    num_origin_bytes_ = num_bytes;
    original_content = new ustore::byte_t[num_bytes];
    std::memcpy(original_content, raw_data, num_bytes);

    std::vector<const ustore::byte_t*> elements_data;
    std::vector<size_t> elements_num_bytes;

    for (size_t i = 0; i < num_bytes; i++) {
      ustore::byte_t* element_data = new ustore::byte_t[1];
      std::memcpy(element_data, raw_data + i, 1);

      elements_data.push_back(element_data);
      elements_num_bytes.push_back(1);
    }

    ustore::NodeBuilder builder = ustore::NodeBuilder();

    builder.SpliceEntries(0, elements_data, elements_num_bytes);
    root_chunk = builder.Commit(ustore::BlobNode::MakeChunk);

    // root_hash = chunk->hash();
  }

  void Test_Splice(size_t splice_idx, ustore::byte_t* insert_bytes,
                   size_t num_insert_bytes, size_t num_delete_bytes,
                   bool isVerbose = false) {
    verbose = isVerbose;
    ustore::InitLogging("");
    ustore::NodeBuilder* b = ustore::NodeBuilder::NewNodeBuilderAtIndex(
        root_chunk->hash(), splice_idx, loader_);
    // Splice at seq beginning
    std::vector<const ustore::byte_t*> elements_data;
    std::vector<size_t> elements_num_bytes;

    for (size_t i = 0; i < num_insert_bytes; ++i) {
      elements_data.push_back(new ustore::byte_t[1]{insert_bytes[i]});
      elements_num_bytes.push_back(1);
    }

    b->SpliceEntries(num_delete_bytes, elements_data, elements_num_bytes);
    const ustore::Chunk* c = b->Commit(ustore::BlobNode::MakeChunk);

    Test_Tree_Integrity(c->hash(), loader_);
    const ustore::byte_t* expected_data =
        ustore::SpliceBytes(original_content, num_origin_bytes_, splice_idx,
                            num_delete_bytes, insert_bytes, num_insert_bytes);
    Test_Same_Content(c->hash(), loader_, expected_data);

    delete[] expected_data;
    delete b;
    delete c;
    verbose = false;
  }

  virtual void TearDown() {
    NodeBuilderEnv::TearDown();
    delete[] original_content;
    delete root_chunk;
  }

  const ustore::Chunk* root_chunk;
  size_t num_origin_bytes_;
  ustore::byte_t* original_content;
};

TEST_F(NodeBuilderSimple, InitTree) {
<<<<<<< HEAD
    verbose = false;
    Test_Tree_Integrity(root_chunk->hash(), loader_);
    Test_Same_Content(root_chunk->hash(), loader_, expected_content);
=======
  verbose = false;
  Test_Tree_Integrity(root_chunk->hash(), loader_);
  Test_Same_Content(root_chunk->hash(), loader_, original_content);
>>>>>>> 7c2ab59... clean up node_builder test
}

TEST_F(NodeBuilderSimple, SpliceStart) {
  // Test on insertion at start point
<<<<<<< HEAD
  ustore::NodeBuilder* b1 = ustore::NodeBuilder
                                  ::NewNodeBuilderAtIndex(
                                                         root_chunk->hash(),
                                                         0,
                                                         loader_);
  // Splice at seq beginning
  std::vector<const ustore::byte_t*> elements_data;
  std::vector<size_t> elements_num_bytes;

  ustore::byte_t* x = new ustore::byte_t[1]{'x'};
  ustore::byte_t* y = new ustore::byte_t[1]{'y'};
  ustore::byte_t* z = new ustore::byte_t[1]{'z'};

  elements_data.push_back(x);
  elements_data.push_back(y);
  elements_data.push_back(z);

  elements_num_bytes.push_back(1);
  elements_num_bytes.push_back(1);
  elements_num_bytes.push_back(1);

  b1->SpliceEntries(1, elements_data, elements_num_bytes);
  const ustore::Chunk* c1 = b1->Commit(ustore::BlobNode::MakeChunk);

  Test_Tree_Integrity(c1->hash(), loader_);
  const ustore::byte_t expected_data[] = "xyzbcdefghijklmn";
  Test_Same_Content(c1->hash(), loader_, expected_data);

  delete b1;
  delete c1;

  // insert only
  ustore::NodeBuilder* b2 = ustore::NodeBuilder
                                  ::NewNodeBuilderAtIndex(
                                                         root_chunk->hash(),
                                                         0,
                                                         loader_);
  elements_data.clear();
  elements_num_bytes.clear();

  x = new ustore::byte_t[1]{'x'};
  y = new ustore::byte_t[1]{'y'};
  z = new ustore::byte_t[1]{'z'};

  elements_data.push_back(x);
  elements_data.push_back(y);
  elements_data.push_back(z);

  elements_num_bytes.push_back(1);
  elements_num_bytes.push_back(1);
  elements_num_bytes.push_back(1);

  b2->SpliceEntries(0, elements_data, elements_num_bytes);
  const ustore::Chunk* c2 = b2->Commit(ustore::BlobNode::MakeChunk);

  Test_Tree_Integrity(c2->hash(), loader_);
  const ustore::byte_t expected_data2[] = "xyzabcdefghijklmn";
  Test_Same_Content(c2->hash(), loader_, expected_data2);

  delete b2;
  delete c2;

  // remove only
  ustore::NodeBuilder* b3 = ustore::NodeBuilder
                                  ::NewNodeBuilderAtIndex(
                                                         root_chunk->hash(),
                                                         0,
                                                         loader_);
  elements_data.clear();
  elements_num_bytes.clear();

  b3->SpliceEntries(2, elements_data, elements_num_bytes);
  const ustore::Chunk* c3 = b3->Commit(ustore::BlobNode::MakeChunk);

  Test_Tree_Integrity(c3->hash(), loader_);
  const ustore::byte_t expected_data3[] = "cdefghijklmn";
  Test_Same_Content(c3->hash(), loader_, expected_data3);

  delete b3;
  delete c3;
}

TEST_F(NodeBuilderSimple, SpliceMiddle) {
  ustore::NodeBuilder* b1 = ustore::NodeBuilder
                                  ::NewNodeBuilderAtIndex(
                                                         root_chunk->hash(),
                                                         5,
                                                         loader_);
  std::vector<const ustore::byte_t*> elements_data;
  std::vector<size_t> elements_num_bytes;

  ustore::byte_t* m = new ustore::byte_t[1]{'m'};
  ustore::byte_t* n = new ustore::byte_t[1]{'n'};

  elements_data.push_back(m);
  elements_data.push_back(n);

  elements_num_bytes.push_back(1);
  elements_num_bytes.push_back(1);

  b1->SpliceEntries(1, elements_data, elements_num_bytes);
  const ustore::Chunk* c1 = b1->Commit(ustore::BlobNode::MakeChunk);

  Test_Tree_Integrity(c1->hash(), loader_);
  const ustore::byte_t expected_data[] = "abcdemnghijklmn";
  Test_Same_Content(c1->hash(), loader_, expected_data);

  delete b1;
  delete c1;

  // Insert Only
  ustore::NodeBuilder* b2 = ustore::NodeBuilder
                                  ::NewNodeBuilderAtIndex(
                                                         root_chunk->hash(),
                                                         5,
                                                         loader_);

  elements_data.clear();
  elements_num_bytes.clear();

  m = new ustore::byte_t[1]{'m'};
  n = new ustore::byte_t[1]{'n'};

  elements_data.push_back(m);
  elements_data.push_back(n);

  elements_num_bytes.push_back(1);
  elements_num_bytes.push_back(1);

  b2->SpliceEntries(0, elements_data, elements_num_bytes);
  const ustore::Chunk* c2 = b2->Commit(ustore::BlobNode::MakeChunk);

  Test_Tree_Integrity(c2->hash(), loader_);
  const ustore::byte_t expected_data2[] = "abcdemnfghijklmn";
  Test_Same_Content(c2->hash(), loader_, expected_data2);

  delete b2;
  delete c2;

  // Delete Only
  ustore::NodeBuilder* b3 = ustore::NodeBuilder
                                  ::NewNodeBuilderAtIndex(
                                                         root_chunk->hash(),
                                                         5,
                                                         loader_);

  elements_data.clear();
  elements_num_bytes.clear();

  b3->SpliceEntries(3, elements_data, elements_num_bytes);
  const ustore::Chunk* c3 = b3->Commit(ustore::BlobNode::MakeChunk);

  Test_Tree_Integrity(c3->hash(), loader_);
  const ustore::byte_t expected_data3[] = "abcdeijklmn";
  Test_Same_Content(c3->hash(), loader_, expected_data3);

  delete b3;
  delete c3;
}

TEST_F(NodeBuilderSimple, SpliceFromEnd) {
  ustore::NodeBuilder* b1 = ustore::NodeBuilder
                                  ::NewNodeBuilderAtIndex(
                                                         root_chunk->hash(),
                                                         20,
                                                         loader_);
  std::vector<const ustore::byte_t*> elements_data;
  std::vector<size_t> elements_num_bytes;

  ustore::byte_t* x = new ustore::byte_t[1]{'x'};
  ustore::byte_t* y = new ustore::byte_t[1]{'y'};

  elements_data.push_back(x);
  elements_data.push_back(y);

  elements_num_bytes.push_back(1);
  elements_num_bytes.push_back(1);

  b1->SpliceEntries(1, elements_data, elements_num_bytes);
  const ustore::Chunk* c1 = b1->Commit(ustore::BlobNode::MakeChunk);

  Test_Tree_Integrity(c1->hash(), loader_);
  const ustore::byte_t expected_data1[] = "abcdefghijklmnxy";
  Test_Same_Content(c1->hash(), loader_, expected_data1);

  delete b1;
  delete c1;

  // Insert Only
  ustore::NodeBuilder* b2 = ustore::NodeBuilder
                                  ::NewNodeBuilderAtIndex(
                                                         root_chunk->hash(),
                                                         20,
                                                         loader_);

  elements_data.clear();
  elements_num_bytes.clear();

  x = new ustore::byte_t[1]{'x'};
  y = new ustore::byte_t[1]{'y'};

  elements_data.push_back(x);
  elements_data.push_back(y);

  elements_num_bytes.push_back(1);
  elements_num_bytes.push_back(1);

  b2->SpliceEntries(0, elements_data, elements_num_bytes);
  const ustore::Chunk* c2 = b2->Commit(ustore::BlobNode::MakeChunk);

  Test_Tree_Integrity(c2->hash(), loader_);
  const ustore::byte_t expected_data2[] = "abcdefghijklmnxy";
  Test_Same_Content(c2->hash(), loader_, expected_data2);

  delete b2;
  delete c2;

  // Delete Only
  ustore::NodeBuilder* b3 = ustore::NodeBuilder
                                  ::NewNodeBuilderAtIndex(
                                                         root_chunk->hash(),
                                                         20,
                                                         loader_);

  elements_data.clear();
  elements_num_bytes.clear();

  b3->SpliceEntries(3, elements_data, elements_num_bytes);
  const ustore::Chunk* c3 = b3->Commit(ustore::BlobNode::MakeChunk);

  Test_Tree_Integrity(c3->hash(), loader_);
  const ustore::byte_t expected_data3[] = "abcdefghijklmn";
  Test_Same_Content(c3->hash(), loader_, expected_data3);

  delete b3;
  delete c3;
}

TEST_F(NodeBuilderSimple, SpliceToEnd) {
  ustore::NodeBuilder* b1 = ustore::NodeBuilder
                                  ::NewNodeBuilderAtIndex(
                                                         root_chunk->hash(),
                                                         12,
                                                         loader_);
  std::vector<const ustore::byte_t*> elements_data;
  std::vector<size_t> elements_num_bytes;

  ustore::byte_t* x = new ustore::byte_t[1]{'x'};
  ustore::byte_t* y = new ustore::byte_t[1]{'y'};
  ustore::byte_t* z = new ustore::byte_t[1]{'z'};

  elements_data.push_back(x);
  elements_data.push_back(y);
  elements_data.push_back(z);

  elements_num_bytes.push_back(1);
  elements_num_bytes.push_back(1);
  elements_num_bytes.push_back(1);

  b1->SpliceEntries(3, elements_data, elements_num_bytes);
  const ustore::Chunk* c1 = b1->Commit(ustore::BlobNode::MakeChunk);

  Test_Tree_Integrity(c1->hash(), loader_);
  const ustore::byte_t expected_data1[] = "abcdefghijklxyz";
  Test_Same_Content(c1->hash(), loader_, expected_data1);

  delete b1;
  delete c1;

  // Delete Only
  ustore::NodeBuilder* b3 = ustore::NodeBuilder
                                  ::NewNodeBuilderAtIndex(
                                                         root_chunk->hash(),
                                                         12,
                                                         loader_);

  elements_data.clear();
  elements_num_bytes.clear();

  b3->SpliceEntries(3, elements_data, elements_num_bytes);
  const ustore::Chunk* c3 = b3->Commit(ustore::BlobNode::MakeChunk);

  Test_Tree_Integrity(c3->hash(), loader_);
  const ustore::byte_t expected_data3[] = "abcdefghijkl";
  Test_Same_Content(c3->hash(), loader_, expected_data3);

  delete b3;
  delete c3;
=======
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
>>>>>>> 7c2ab59... clean up node_builder test
}

class NodeBuilderComplex:public NodeBuilderEnv {
 protected:
  virtual void SetUp() {
    NodeBuilderEnv::SetUp();
    verbose = true;
    const ustore::byte_t raw_data[] = {
      "SCENE I. Rome. A street.  Enter FLAVIUS, MARULLUS, and certain "
      "Commoners FLAVIUS Hence! home, you idle creatures get you home: Is this "
      "a holiday? what! know you not, Being mechanical, you ought not walk "
      "Upon a labouring day without the sign Of your profession? Speak, what "
      "trade art thou?  First Commoner Why, sir, a carpenter.  MARULLUS Where "
      "is thy leather apron and thy rule?  What dost thou with thy best "
      "apparel on?  You, sir, what trade are you?  Second Commoner Truly, sir, "
      "in respect of a fine workman, I am but, as you would say, a cobbler.  "
      "MARULLUS But what trade art thou? answer me directly.  Second Commoner "
      "I am, indeed, sir, a surgeon to old shoes; when they are in great "
      "danger, I recover them. As proper men as ever trod upon neat's leather "
      "have gone upon my handiwork.  FLAVIUS But wherefore art not in thy shop "
      "today?  Why dost thou lead these men about the streets?  Second "
      "Commoner Truly, sir, to wear out their shoes, to get myself into more "
      "work. But, indeed, sir, we make holiday, to see Caesar and to rejoice "
      "in his triumph.  MARULLUS Wherefore rejoice? What conquest brings he "
      "home?  What tributaries follow him to Rome, To grace in captive bonds "
      "his chariot-wheels?  You blocks, you stones, you worse than senseless "
      "things!  O you hard hearts, you cruel men of Rome, Knew you not Pompey? "
      "Many a time and oft Have you climb'd up to walls and battlements, To "
      "towers and windows, yea, to chimney-tops, Your infants in your arms, "
      "Caesar's trophies. I'll about, And drive away the vulgar from the "
      "streets: So do you too, where you perceive them thick.  These growing "
      "feathers pluck'd from Caesar's wing Will make him fly an ordinary "
      "pitch, Who else would soar above the view of men And keep us all in "
      "servile fearfulness. Exeunt"};

    original_num_bytes = sizeof(raw_data) - 1;
    original_content = new ustore::byte_t[original_num_bytes];
    std::memcpy(original_content, raw_data, original_num_bytes);

    std::vector<const ustore::byte_t*> elements_data;
    std::vector<size_t> elements_num_bytes;

    for (size_t i = 0; i < original_num_bytes; i++) {
      ustore::byte_t* element_data = new ustore::byte_t[1];
      std::memcpy(element_data, raw_data + i, 1);

      elements_data.push_back(element_data);
      elements_num_bytes.push_back(1);
    }

    ustore::NodeBuilder builder = ustore::NodeBuilder();

    builder.SpliceEntries(0, elements_data, elements_num_bytes);
    root_chunk = builder.Commit(ustore::BlobNode::MakeChunk);

    // root_hash = chunk->hash();
  }

<<<<<<< HEAD
  // a utility method to splice chars for testing node building result
  const ustore::byte_t* SpliceChars(const ustore::byte_t* src,
                                    size_t src_size,
                                    size_t start, size_t num_delete,
                                    const ustore::byte_t* append,
                                    size_t append_size) {
    if (start > src_size) start = src_size;
=======
  void test_splice_case(size_t splice_idx, const ustore::byte_t* append_data,
                        size_t append_num_bytes, size_t num_delete) {
    std::vector<const ustore::byte_t*> elements_data;
    std::vector<size_t> elements_num_bytes;

    ustore::NodeBuilder* b = ustore::NodeBuilder::NewNodeBuilderAtIndex(
        root_chunk->hash(), splice_idx, loader_);
>>>>>>> 7c2ab59... clean up node_builder test

    for (size_t i = 0; i < append_num_bytes; i++) {
      ustore::byte_t* d = new ustore::byte_t[1];
      *d = *(append_data + i);
      elements_data.push_back(d);
      elements_num_bytes.push_back(1);
    }

<<<<<<< HEAD
    ustore::byte_t* result = new ustore::byte_t
                                [src_size - real_delete + append_size];

    std::memcpy(result, src, start);
    std::memcpy(result + start, append, append_size);
    std::memcpy(result + start + append_size,
                src + start + real_delete,
                src_size - start - real_delete);
=======
    b->SpliceEntries(num_delete, elements_data, elements_num_bytes);
    const ustore::Chunk* c = b->Commit(ustore::BlobNode::MakeChunk);

    Test_Tree_Integrity(c->hash(), loader_);
    const ustore::byte_t* d =
        ustore::SpliceBytes(original_content, original_num_bytes, splice_idx,
                            num_delete, append_data, append_num_bytes);

    Test_Same_Content(c->hash(), loader_, d);
>>>>>>> 7c2ab59... clean up node_builder test

    delete b;
    delete c;
    delete[] d;
  }

  // test on three cases of splice from splice_idx element
  //   1. Normal Splice: First remove and then append elements
  //   2. No removal just appending
  //   3. No appending, just removal
  void Test_Splice(size_t splice_idx, bool isVerbose = false) {
    verbose = isVerbose;
    const ustore::byte_t append_data[] = {
      "Commoners FLAVIUS Hence! home, you idle creatures get you home: Is this "
      "a holiday? what! know you not, Being mechanical, you ought not walk "
      "Upon a labouring day without the sign Of your profession? Speak, what "
      "trade art thou?  First Commoner Why, sir, a carpenter.  MARULLUS Where "
      "is thy leather apron and thy rule?  What dost thou with thy best "
      "apparel on?  You, sir, what trade are you?  Second Commoner Truly, sir, "
      "in respect of a fine workman, I am but, as you would say, a cobbler."};

    size_t append_num_bytes = sizeof(append_data) - 1;

    ustore::InitLogging("");
    // LOG(INFO) << " Append Bytes: " << append_num_bytes;
    // LOG(INFO) << " Original Bytes: " << original_num_bytes;

<<<<<<< HEAD
    size_t num_delete = 500;
    std::vector<const ustore::byte_t*> elements_data;
    std::vector<size_t> elements_num_bytes;

    ustore::NodeBuilder* b1 = ustore::NodeBuilder
                                    ::NewNodeBuilderAtIndex(
                                                           root_chunk->hash(),
                                                           splice_idx,
                                                           loader_);


    for (size_t i = 0; i < append_num_bytes; i++) {
      ustore::byte_t* d = new ustore::byte_t[1];
      *d = *(append_data + i);
      elements_data.push_back(d);
      elements_num_bytes.push_back(1);
    }

    b1->SpliceEntries(num_delete, elements_data, elements_num_bytes);
    const ustore::Chunk* c1 = b1->Commit(ustore::BlobNode::MakeChunk);

    Test_Tree_Integrity(c1->hash(), loader_);
    const ustore::byte_t* d1 = SpliceChars(original_content,
                                           original_num_bytes,
                                           splice_idx, num_delete,
                                           append_data,
                                           append_num_bytes);

    Test_Same_Content(c1->hash(), loader_, d1);

    delete b1;
    delete c1;
    delete[] d1;


    // Insert Only
    ustore::NodeBuilder* b2 = ustore::NodeBuilder
                                    ::NewNodeBuilderAtIndex(
                                                           root_chunk->hash(),
                                                           splice_idx,
                                                           loader_);
    elements_data.clear();
    elements_num_bytes.clear();

    for (size_t i = 0; i < append_num_bytes; i++) {
      ustore::byte_t* d = new ustore::byte_t[1];
      *d = *(append_data + i);
      elements_data.push_back(d);
      elements_num_bytes.push_back(1);
    }

    b2->SpliceEntries(0, elements_data, elements_num_bytes);
    const ustore::Chunk* c2 = b2->Commit(ustore::BlobNode::MakeChunk);

    Test_Tree_Integrity(c2->hash(), loader_);
    const ustore::byte_t* d2 = SpliceChars(original_content,
                                           original_num_bytes,
                                           splice_idx, 0,
                                           append_data,
                                           append_num_bytes);
    Test_Same_Content(c2->hash(), loader_, d2);

    delete b2;
    delete c2;
    delete[] d2;

    // Delete Only
    ustore::NodeBuilder* b3 = ustore::NodeBuilder
                                    ::NewNodeBuilderAtIndex(
                                                           root_chunk->hash(),
                                                           splice_idx,
                                                           loader_);
    elements_data.clear();
    elements_num_bytes.clear();

    b3->SpliceEntries(num_delete, elements_data, elements_num_bytes);
    const ustore::Chunk* c3 = b3->Commit(ustore::BlobNode::MakeChunk);

    Test_Tree_Integrity(c3->hash(), loader_);
    const ustore::byte_t* d3 = SpliceChars(original_content,
                                           original_num_bytes,
                                           splice_idx, num_delete,
                                           append_data,
                                           0);

    Test_Same_Content(c3->hash(), loader_, d3);

    delete b3;
    delete c3;
    delete[] d3;
=======
    // Delete and Insert
    test_splice_case(splice_idx, append_data, append_num_bytes, 500);
    // Insert Only
    test_splice_case(splice_idx, append_data, append_num_bytes, 0);
    // Delete Only
    test_splice_case(splice_idx, append_data, 0, 500);
>>>>>>> 7c2ab59... clean up node_builder test
  }

  virtual void TearDown() {
    NodeBuilderEnv::TearDown();
    delete[] original_content;
    delete root_chunk;
  }

  const ustore::Chunk* root_chunk;
  ustore::byte_t* original_content;
  size_t original_num_bytes;
};

TEST_F(NodeBuilderComplex, InitTree) {
  verbose = false;
  Test_Tree_Integrity(root_chunk->hash(), loader_);
  Test_Same_Content(root_chunk->hash(), loader_, original_content);
}

TEST_F(NodeBuilderComplex, SpliceSeqStart) {
  Test_Splice(0);
}


TEST_F(NodeBuilderComplex, SpliceSeqMiddle) {
  Test_Splice(244);
}

TEST_F(NodeBuilderComplex, SpliceSeqEnd) {
  Test_Splice(5000);
}

TEST_F(NodeBuilderComplex, SpliceChunkStart) {
  // 298 the index of first element in 3rd chunk
  Test_Splice(299);
}

TEST_F(NodeBuilderComplex, SpliceChunkEnd) {
  // 480th-entry is the first entry 4th leaf chunk
  Test_Splice(481);
}
#endif  // TEST_NODEBUILDER
