// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "gtest/gtest.h"

#include "types/server/factory.h"
#include "types/server/sblob.h"

#include "utils/debug.h"
#include "utils/logging.h"

class SBlobEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    const char raw_data[] = {
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

    data_bytes_ = sizeof(raw_data) - 1;
    data_ = new ustore::byte_t[data_bytes_];
    std::memcpy(data_, raw_data, data_bytes_);

    const char* raw_data_append[] = {
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

    append_data_bytes_ = sizeof(raw_data_append) - 1;
    append_data_ = new ustore::byte_t[append_data_bytes_];
    std::memcpy(append_data_, raw_data_append, append_data_bytes_);

    ustore::ChunkableTypeFactory factory;
    ustore::Slice data(data_, data_bytes_);
    const ustore::SBlob sblob_ = factory.CreateBlob(data);
    blob_hash_ = sblob_.hash().Clone();
  }

  virtual void TearDown() {
    delete[] data_;
    delete[] append_data_;
  }

  ustore::byte_t* data_;
  size_t data_bytes_ = 0;

  ustore::byte_t* append_data_;
  size_t append_data_bytes_ = 0;

  ustore::Hash blob_hash_;
};

TEST_F(SBlobEnv, Iterator) {
  ustore::ChunkableTypeFactory factory;
  const ustore::SBlob sblob = factory.LoadBlob(blob_hash_);
  auto it = sblob.Scan();

  const ustore::byte_t* data_ptr = data_;
  do {
    ustore::Slice expected_val(data_ptr, 1);
    ustore::Slice actual_val = it.value();
    ASSERT_TRUE(expected_val == actual_val);

    ++data_ptr;
  } while (it.next());

  auto chunk_it = sblob.ScanChunk();

#ifdef TEST_NODEBUILDER
// Leaf Chunks are organized as the following
//   only if using the tested node builder environment

  data_ptr = data_;
  // Refer to node/node_builder.cc for constructed tree structure
  std::vector<size_t> leaf_chunk_bytes
      {67, 38, 193, 183, 512, 320, 53, 55, 74, 256 };

  size_t offset = 0;

  // Iterating forwards
  for (size_t leaf_idx = 0;
       leaf_idx < leaf_chunk_bytes.size(); ++leaf_idx) {
    ASSERT_TRUE(ustore::Slice(data_ptr + offset,
                              leaf_chunk_bytes[leaf_idx])
                == chunk_it.value())
        << " Fail at leaf " << leaf_idx;

    ASSERT_FALSE(chunk_it.head());
    ASSERT_FALSE(chunk_it.end());
    offset += leaf_chunk_bytes[leaf_idx];
    chunk_it.next();
  }
  ASSERT_FALSE(chunk_it.head());
  ASSERT_TRUE(chunk_it.end());

  // Iterating Backwards
  for (size_t leaf_idx = leaf_chunk_bytes.size();
       leaf_idx > 0 ; --leaf_idx) {
    ASSERT_TRUE(chunk_it.previous());
    offset -= leaf_chunk_bytes[leaf_idx - 1];
    ASSERT_TRUE(ustore::Slice(data_ptr + offset,
                              leaf_chunk_bytes[leaf_idx - 1])
                == chunk_it.value())
        << " Fail at leaf " << leaf_idx;

    ASSERT_FALSE(chunk_it.head());
    ASSERT_FALSE(chunk_it.end());
  }

  // Retreat to cursor head
  ASSERT_FALSE(chunk_it.previous());
  ASSERT_TRUE(chunk_it.head());
  ASSERT_FALSE(chunk_it.end());

  // Advance to first leaf chunk
  ASSERT_TRUE(chunk_it.next());
  ASSERT_TRUE(ustore::Slice(data_ptr,
                            leaf_chunk_bytes[0])
              == chunk_it.value());
#endif  // Test NodeBuilder
}

TEST_F(SBlobEnv, Splice) {
  ustore::ChunkableTypeFactory factory;
  const ustore::SBlob sblob = factory.LoadBlob(blob_hash_);

  size_t splice_idx = 666;
  size_t num_delete = 777;
  ustore::SBlob new_sblob = factory.LoadBlob(
      sblob.Splice(splice_idx, num_delete, append_data_, append_data_bytes_));

  size_t expected_len = data_bytes_ - num_delete + append_data_bytes_;
  ASSERT_EQ(expected_len, new_sblob.size());

  ustore::byte_t* actual_bytes = new ustore::byte_t[expected_len];
  EXPECT_EQ(expected_len, new_sblob.Read(0, expected_len, actual_bytes));

  const ustore::byte_t* expected_bytes =
      ustore::SpliceBytes(data_, data_bytes_, splice_idx, num_delete,
                          append_data_, append_data_bytes_);

  EXPECT_EQ(ustore::byte2str(expected_bytes, expected_len),
            ustore::byte2str(actual_bytes, expected_len));

  delete[] actual_bytes;
  delete[] expected_bytes;
}

// Number of elements to delete exceeds the blob end
TEST_F(SBlobEnv, SpliceOverflow) {
  ustore::ChunkableTypeFactory factory;
  const ustore::SBlob sblob = factory.LoadBlob(blob_hash_);

  size_t num_delete = 777;
  size_t real_delete = 400;
  size_t splice_idx = data_bytes_ - real_delete;

  ustore::SBlob new_sblob = factory.LoadBlob(
      sblob.Splice(splice_idx, num_delete, append_data_, append_data_bytes_));

  size_t expected_len = data_bytes_ - real_delete + append_data_bytes_;
  ASSERT_EQ(expected_len, new_sblob.size());

  ustore::byte_t* actual_bytes = new ustore::byte_t[expected_len];
  EXPECT_EQ(expected_len, new_sblob.Read(0, expected_len, actual_bytes));

  const ustore::byte_t* expected_bytes =
      ustore::SpliceBytes(data_, data_bytes_, splice_idx, num_delete,
                          append_data_, append_data_bytes_);

  EXPECT_EQ(ustore::byte2str(expected_bytes, expected_len),
            ustore::byte2str(actual_bytes, expected_len));

  delete[] actual_bytes;
  delete[] expected_bytes;
}

TEST_F(SBlobEnv, Insert) {
  ustore::ChunkableTypeFactory factory;
  const ustore::SBlob sblob = factory.LoadBlob(blob_hash_);

  size_t insert_idx = 888;
  ustore::SBlob new_sblob = factory.LoadBlob(
      sblob.Insert(insert_idx, append_data_, append_data_bytes_));

  size_t expected_len = data_bytes_ + append_data_bytes_;
  ASSERT_EQ(expected_len, new_sblob.size());

  ustore::byte_t* actual_bytes = new ustore::byte_t[expected_len];
  EXPECT_EQ(expected_len, new_sblob.Read(0, expected_len, actual_bytes));

  const ustore::byte_t* expected_bytes = ustore::SpliceBytes(
      data_, data_bytes_, insert_idx, 0, append_data_, append_data_bytes_);

  EXPECT_EQ(ustore::byte2str(expected_bytes, expected_len),
            ustore::byte2str(actual_bytes, expected_len));

  delete[] actual_bytes;
  delete[] expected_bytes;
}

TEST_F(SBlobEnv, Delete) {
  ustore::ChunkableTypeFactory factory;
  const ustore::SBlob sblob = factory.LoadBlob(blob_hash_);

  size_t delete_idx = 999;
  size_t num_delete = 500;
  ustore::SBlob new_sblob = factory.LoadBlob(
      sblob.Delete(delete_idx, num_delete));

  size_t expected_len = data_bytes_ - num_delete;
  ASSERT_EQ(expected_len, new_sblob.size());

  ustore::byte_t* actual_bytes = new ustore::byte_t[expected_len];
  EXPECT_EQ(expected_len, new_sblob.Read(0, expected_len, actual_bytes));

  const ustore::byte_t* expected_bytes = ustore::SpliceBytes(
      data_, data_bytes_, delete_idx, num_delete, nullptr, 0);

  EXPECT_EQ(ustore::byte2str(expected_bytes, expected_len),
            ustore::byte2str(actual_bytes, expected_len));

  delete[] actual_bytes;
  delete[] expected_bytes;
}

// Number of elements to delete exceeds the blob end
TEST_F(SBlobEnv, DeleteOverflow) {
  ustore::ChunkableTypeFactory factory;
  const ustore::SBlob sblob = factory.LoadBlob(blob_hash_);

  size_t num_delete = 500;
  size_t real_delete = 300;
  size_t delete_idx = data_bytes_ - real_delete;

  ustore::SBlob new_sblob = factory.LoadBlob(
      sblob.Delete(delete_idx, num_delete));

  size_t expected_len = data_bytes_ - real_delete;
  ASSERT_EQ(expected_len, new_sblob.size());

  ustore::byte_t* actual_bytes = new ustore::byte_t[expected_len];
  EXPECT_EQ(expected_len, new_sblob.Read(0, expected_len, actual_bytes));

  const ustore::byte_t* expected_bytes = ustore::SpliceBytes(
      data_, data_bytes_, delete_idx, num_delete, nullptr, 0);

  EXPECT_EQ(ustore::byte2str(expected_bytes, expected_len),
            ustore::byte2str(actual_bytes, expected_len));

  delete[] actual_bytes;
  delete[] expected_bytes;
}

TEST_F(SBlobEnv, Append) {
  ustore::ChunkableTypeFactory factory;
  const ustore::SBlob sblob = factory.LoadBlob(blob_hash_);

  ustore::SBlob new_sblob = factory.LoadBlob(
      sblob.Append(append_data_, append_data_bytes_));

  size_t expected_len = data_bytes_ + append_data_bytes_;
  ASSERT_EQ(expected_len, new_sblob.size());

  ustore::byte_t* actual_bytes = new ustore::byte_t[expected_len];
  EXPECT_EQ(expected_len, new_sblob.Read(0, expected_len, actual_bytes));

  const ustore::byte_t* expected_bytes = ustore::SpliceBytes(
      data_, data_bytes_, data_bytes_, 0, append_data_, append_data_bytes_);

  EXPECT_EQ(ustore::byte2str(expected_bytes, expected_len),
            ustore::byte2str(actual_bytes, expected_len));

  delete[] actual_bytes;
  delete[] expected_bytes;
}

TEST_F(SBlobEnv, Read) {
  ustore::ChunkableTypeFactory factory;
  const ustore::SBlob sblob = factory.LoadBlob(blob_hash_);
  EXPECT_EQ(sblob.size(), data_bytes_);

  // Read from Middle
  size_t len = 1000;
  size_t pos = 100;
  ustore::byte_t* buffer = new ustore::byte_t[len];
  EXPECT_EQ(len, sblob.Read(pos, len, buffer));

  EXPECT_EQ(ustore::byte2str(data_ + pos, len), ustore::byte2str(buffer, len));
  delete[] buffer;

  // Read ALL
  len = data_bytes_;
  pos = 0;
  buffer = new ustore::byte_t[len];
  EXPECT_EQ(sblob.Read(pos, len, buffer), len);

  EXPECT_EQ(ustore::byte2str(data_, len), ustore::byte2str(buffer, len));

  // Read exceeding the end
  len = 1000;
  // Leave only 700 remaining elements to read
  pos = data_bytes_ - 300;
  size_t real_len = sblob.Read(pos, len, buffer);
  EXPECT_EQ(size_t(300), real_len);

  EXPECT_EQ(ustore::byte2str(data_ + pos, real_len),
            ustore::byte2str(buffer, real_len));
  delete[] buffer;
}

TEST(SimpleSBlob, Load) {
  ustore::ChunkableTypeFactory factory;
  const ustore::byte_t raw_data[] =
      "The quick brown fox jumps over the lazy dog";
  size_t len = sizeof(raw_data);

  ///////////////////////////////////////
  // Create a junk to load
  ustore::Chunk chunk(ustore::ChunkType::kBlob, len);
  std::memcpy(chunk.m_data(), raw_data, sizeof(raw_data));
  // Put the chunk into storage
  ustore::store::GetChunkStore()->Put(chunk.hash(), chunk);
  ///////////////////////////////////////

  ustore::SBlob sblob = factory.LoadBlob(chunk.hash());

  // size()
  EXPECT_EQ(len, sblob.size());

  // Read
  size_t pos = 0;
  ustore::byte_t* buffer = new ustore::byte_t[len];
  EXPECT_EQ(len, sblob.Read(pos, len, buffer));

  EXPECT_EQ(ustore::byte2str(raw_data, len), ustore::byte2str(buffer, len));
  delete[] buffer;


  // Test for move assignment
  ustore::SBlob sblob_m = std::move(sblob);

  // size()
  EXPECT_EQ(len, sblob_m.size());

  // Read
  pos = 0;
  buffer = new ustore::byte_t[len];
  EXPECT_EQ(len, sblob_m.Read(pos, len, buffer));

  EXPECT_EQ(ustore::byte2str(raw_data, len), ustore::byte2str(buffer, len));
  delete[] buffer;

  // Test for move ctor
  ustore::SBlob sblob_m1(std::move(sblob_m));

  // size()
  EXPECT_EQ(len, sblob_m1.size());

  // Read
  pos = 0;
  buffer = new ustore::byte_t[len];
  EXPECT_EQ(len, sblob_m1.Read(pos, len, buffer));

  EXPECT_EQ(ustore::byte2str(raw_data, len), ustore::byte2str(buffer, len));
  delete[] buffer;
}

TEST(SBlob, Empty) {
  ustore::ChunkableTypeFactory factory;
  ustore::Slice empty;
  ustore::SBlob sblob = factory.CreateBlob(empty);

  ASSERT_EQ(size_t(0), sblob.numElements());

  // Append 3 elements
  ustore::byte_t d[] = "abc";
  ustore::SBlob s1 = factory.LoadBlob(sblob.Append(d, 3));
  ASSERT_EQ(size_t(3), s1.numElements());

  // Remove the only 3 elements
  ustore::SBlob s2 = factory.LoadBlob(s1.Delete(0, 3));
  ASSERT_EQ(size_t(0), s2.numElements());

  // Test on normal iterator
  auto it = s2.Scan();
  ASSERT_TRUE(it.end());
  ASSERT_FALSE(it.next());

  ASSERT_TRUE(it.end());
  ASSERT_FALSE(it.head());

  ASSERT_FALSE(it.previous());

  ASSERT_FALSE(it.end());
  ASSERT_TRUE(it.head());

  // Test on chunk iterator
  auto chunk_it = s2.ScanChunk();

  ASSERT_TRUE(chunk_it.end());
  ASSERT_FALSE(chunk_it.next());

  ASSERT_TRUE(chunk_it.end());
  ASSERT_FALSE(chunk_it.head());

  ASSERT_FALSE(chunk_it.previous());

  ASSERT_FALSE(chunk_it.end());
  ASSERT_TRUE(chunk_it.head());
}
