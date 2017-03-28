// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "gtest/gtest.h"
#include "chunk/chunk.h"
#include "node/blob_node.h"
#include "store/chunk_store.h"
#include "types/ublob.h"
#include "utils/debug.h"
#include "utils/logging.h"

class UBlobEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
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

    data_bytes_ = sizeof(raw_data) - 1;
    data_ = new ustore::byte_t[data_bytes_];
    std::memcpy(data_, raw_data, data_bytes_);

    const ustore::byte_t raw_data_append[] = {
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

    ublob_ = ustore::UBlob::Create(data_, data_bytes_);
    blob_hash_ = ublob_->hash();
  }

  virtual void TearDown() {
    delete[] data_;
    delete[] append_data_;
  }

  ustore::byte_t* data_;
  size_t data_bytes_ = 0;

  ustore::byte_t* append_data_;
  size_t append_data_bytes_ = 0;

  const ustore::UBlob* ublob_;
  ustore::Hash blob_hash_;
};

TEST_F(UBlobEnv, Splice) {
  size_t splice_idx = 666;
  size_t num_delete = 777;
  const ustore::UBlob* new_ublob =
      ublob_->Splice(splice_idx, num_delete, append_data_, append_data_bytes_);

  size_t expected_len = data_bytes_ - num_delete + append_data_bytes_;
  ASSERT_EQ(expected_len, new_ublob->size());

  ustore::byte_t* actual_bytes = new ustore::byte_t[expected_len];
  EXPECT_EQ(expected_len, new_ublob->Read(0, expected_len, actual_bytes));

  const ustore::byte_t* expected_bytes =
      ustore::SpliceBytes(data_, data_bytes_, splice_idx, num_delete,
                          append_data_, append_data_bytes_);

  EXPECT_EQ(ustore::byte2str(expected_bytes, expected_len),
            ustore::byte2str(actual_bytes, expected_len));

  delete[] expected_bytes;
  delete new_ublob;
}

// Number of elements to delete exceeds the blob end
TEST_F(UBlobEnv, SpliceOverflow) {
  size_t num_delete = 777;
  size_t real_delete = 400;
  size_t splice_idx = data_bytes_ - real_delete;

  const ustore::UBlob* new_ublob =
      ublob_->Splice(splice_idx, num_delete, append_data_, append_data_bytes_);

  size_t expected_len = data_bytes_ - real_delete + append_data_bytes_;
  ASSERT_EQ(expected_len, new_ublob->size());

  ustore::byte_t* actual_bytes = new ustore::byte_t[expected_len];
  EXPECT_EQ(expected_len, new_ublob->Read(0, expected_len, actual_bytes));

  const ustore::byte_t* expected_bytes =
      ustore::SpliceBytes(data_, data_bytes_, splice_idx, num_delete,
                          append_data_, append_data_bytes_);

  EXPECT_EQ(ustore::byte2str(expected_bytes, expected_len),
            ustore::byte2str(actual_bytes, expected_len));

  delete[] expected_bytes;
  delete new_ublob;
}

TEST_F(UBlobEnv, Insert) {
  size_t insert_idx = 888;
  const ustore::UBlob* new_ublob =
      ublob_->Insert(insert_idx, append_data_, append_data_bytes_);

  size_t expected_len = data_bytes_ + append_data_bytes_;
  ASSERT_EQ(expected_len, new_ublob->size());

  ustore::byte_t* actual_bytes = new ustore::byte_t[expected_len];
  EXPECT_EQ(expected_len, new_ublob->Read(0, expected_len, actual_bytes));

  const ustore::byte_t* expected_bytes = ustore::SpliceBytes(
      data_, data_bytes_, insert_idx, 0, append_data_, append_data_bytes_);

  EXPECT_EQ(ustore::byte2str(expected_bytes, expected_len),
            ustore::byte2str(actual_bytes, expected_len));

  delete[] expected_bytes;
  delete new_ublob;
}

TEST_F(UBlobEnv, Delete) {
  size_t delete_idx = 999;
  size_t num_delete = 500;
  const ustore::UBlob* new_ublob = ublob_->Delete(delete_idx, num_delete);

  size_t expected_len = data_bytes_ - num_delete;
  ASSERT_EQ(expected_len, new_ublob->size());

  ustore::byte_t* actual_bytes = new ustore::byte_t[expected_len];
  EXPECT_EQ(expected_len, new_ublob->Read(0, expected_len, actual_bytes));

  const ustore::byte_t* expected_bytes = ustore::SpliceBytes(
      data_, data_bytes_, delete_idx, num_delete, nullptr, 0);

  EXPECT_EQ(ustore::byte2str(expected_bytes, expected_len),
            ustore::byte2str(actual_bytes, expected_len));

  delete[] expected_bytes;
  delete new_ublob;
}

// Number of elements to delete exceeds the blob end
TEST_F(UBlobEnv, DeleteOverflow) {
  size_t num_delete = 500;
  size_t real_delete = 300;
  size_t delete_idx = data_bytes_ - real_delete;

  const ustore::UBlob* new_ublob = ublob_->Delete(delete_idx, num_delete);

  size_t expected_len = data_bytes_ - real_delete;
  ASSERT_EQ(expected_len, new_ublob->size());

  ustore::byte_t* actual_bytes = new ustore::byte_t[expected_len];
  EXPECT_EQ(expected_len, new_ublob->Read(0, expected_len, actual_bytes));

  const ustore::byte_t* expected_bytes = ustore::SpliceBytes(
      data_, data_bytes_, delete_idx, num_delete, nullptr, 0);

  EXPECT_EQ(ustore::byte2str(expected_bytes, expected_len),
            ustore::byte2str(actual_bytes, expected_len));

  delete[] expected_bytes;
  delete new_ublob;
}

TEST_F(UBlobEnv, Append) {
  const ustore::UBlob* new_ublob =
      ublob_->Append(append_data_, append_data_bytes_);

  size_t expected_len = data_bytes_ + append_data_bytes_;
  ASSERT_EQ(expected_len, new_ublob->size());

  ustore::byte_t* actual_bytes = new ustore::byte_t[expected_len];
  EXPECT_EQ(expected_len, new_ublob->Read(0, expected_len, actual_bytes));

  const ustore::byte_t* expected_bytes = ustore::SpliceBytes(
      data_, data_bytes_, data_bytes_, 0, append_data_, append_data_bytes_);

  EXPECT_EQ(ustore::byte2str(expected_bytes, expected_len),
            ustore::byte2str(actual_bytes, expected_len));

  delete[] expected_bytes;
  delete new_ublob;
}

TEST_F(UBlobEnv, Read) {
  EXPECT_EQ(ublob_->size(), data_bytes_);

  // Read from Middle
  size_t len = 1000;
  size_t pos = 100;
  ustore::byte_t* buffer = new ustore::byte_t[len];
  EXPECT_EQ(len, ublob_->Read(pos, len, buffer));

  EXPECT_EQ(ustore::byte2str(data_ + pos, len), ustore::byte2str(buffer, len));

  // Read ALL
  len = data_bytes_;
  pos = 0;
  buffer = new ustore::byte_t[len];
  EXPECT_EQ(ublob_->Read(pos, len, buffer), len);

  EXPECT_EQ(ustore::byte2str(data_, len), ustore::byte2str(buffer, len));

  // Read exceeding the end
  len = 1000;
  // Leave only 700 remaining elements to read
  pos = data_bytes_ - 300;
  size_t real_len = ublob_->Read(pos, len, buffer);
  EXPECT_EQ(300, real_len);

  EXPECT_EQ(ustore::byte2str(data_ + pos, real_len),
            ustore::byte2str(buffer, real_len));

  delete[] buffer;
}

// Check the loaded blob is identical to the created blob
TEST_F(UBlobEnv, Load) {
  const ustore::UBlob* loaded_blob = ustore::UBlob::Load(blob_hash_);
  EXPECT_EQ(ublob_->size(), loaded_blob->size());

  // Read ALL
  size_t len = data_bytes_;
  size_t pos = 0;

  ustore::byte_t* loaded_data = new ustore::byte_t[len];
  EXPECT_EQ(len, ublob_->Read(pos, len, loaded_data));

  ustore::byte_t* created_data = new ustore::byte_t[len];
  EXPECT_EQ(len, loaded_blob->Read(pos, len, created_data));

  EXPECT_EQ(ustore::byte2str(created_data, len),
            ustore::byte2str(loaded_data, len));

  delete[] loaded_data;
  delete[] created_data;
}

TEST(SimpleUBlob, Load) {
  const ustore::byte_t raw_data[] =
      "The quick brown fox jumps over the lazy dog";
  size_t len = sizeof(raw_data);

  ///////////////////////////////////////
  // Create a junk to load
  ustore::Chunk chunk(ustore::ChunkType::kBlob, len);
  std::memcpy(chunk.m_data(), raw_data, sizeof(raw_data));
  ustore::ChunkStore* cs = ustore::store::GetChunkStore();
  // Put the chunk into storage
  cs->Put(chunk.hash(), chunk);
  ///////////////////////////////////////

  const ustore::UBlob* ublob = ustore::UBlob::Load(chunk.hash());

  // size()
  EXPECT_EQ(len, ublob->size());

  // Read
  size_t pos = 0;
  ustore::byte_t* buffer = new ustore::byte_t[len];
  EXPECT_EQ(len, ublob->Read(pos, len, buffer));

  EXPECT_EQ(ustore::byte2str(raw_data, len), ustore::byte2str(buffer, len));
}
