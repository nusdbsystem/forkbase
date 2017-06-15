// Copyright (c) 2017 The Ustore Authors.
#include <stdlib.h>
#include "gtest/gtest.h"
#include "proto/messages.pb.h"
#include "types/type.h"
#include "hash/hash.h"

using ustore::UMessage;
using ustore::RequestPayload;
using ustore::ResponsePayload;
using ustore::ValuePayload;
using ustore::UType;
using ustore::Hash;
using ustore::byte_t;
using ustore::ErrorCode;
const size_t TEST_KEY_SIZE = 32;
const size_t TEST_VERSION_SIZE = 32;
const size_t TEST_BRANCH_SIZE = 32;
const size_t TEST_VALUE_SIZE = 128;
const size_t VALUE2_NVALS = 5;

void randomVals(byte_t* buf, int size) {
  srand(time(NULL));
  for (int i = 0; i < size; i++)
    buf[i] = rand() & 0xFF;
}

void populateHeader(UMessage *msg, UMessage::Type type) {
  msg->set_type(type);

  // generate random vals
  byte_t key[TEST_KEY_SIZE];
  byte_t version[TEST_VERSION_SIZE];
  byte_t branch[TEST_BRANCH_SIZE];
  randomVals(key, TEST_KEY_SIZE);
  randomVals(version, TEST_VERSION_SIZE);
  randomVals(branch, TEST_BRANCH_SIZE);

  // source: 1 (first client thread)
  msg->set_source(1);

  // set fields
  auto request = msg->mutable_request_payload();
  request->set_key(key, TEST_KEY_SIZE);
  request->set_version(version, TEST_VERSION_SIZE);
  request->set_branch(branch, TEST_BRANCH_SIZE);

}

bool checkPayload(const byte_t* original, int size_original,
                   const byte_t* recovered, int size_recovered) {
  if (size_original != size_recovered)
    return false;

  for (int i = 0; i < size_original; i++)
    if (original[i] != recovered[i])
      return false;

  return true;
}


TEST(TestMessage, TestPutRequest) {
  UMessage msg;
  populateHeader(&msg, UMessage::PUT_REQUEST);

  // add payload
  auto request = msg.mutable_request_payload();
  auto val = msg.mutable_value_payload();
  val->set_type(static_cast<int>(UType::kList));
  byte_t base[Hash::kByteLength];
  randomVals(base, Hash::kByteLength);
  val->set_base(base, Hash::kByteLength);
  val->set_pos(0);
  val->set_dels(0);
  for (int i=0; i<VALUE2_NVALS; i++) {
    byte_t value[TEST_VALUE_SIZE];
    randomVals(value, TEST_VALUE_SIZE);
    val->add_values(value, TEST_VALUE_SIZE);

    byte_t key[TEST_KEY_SIZE];
    randomVals(key, TEST_KEY_SIZE);
    val->add_keys(key, TEST_KEY_SIZE);
  }

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.has_request_payload(), true);
  EXPECT_EQ(recovered_msg.type(), UMessage::PUT_REQUEST);
  EXPECT_EQ(recovered_msg.has_value_payload(), true);
  EXPECT_EQ(recovered_msg.value_payload().type(),
            static_cast<int>(UType::kList));
  // check that there are VALUE2_NVALS items in vals and keys
  EXPECT_EQ(recovered_msg.value_payload().values_size(), VALUE2_NVALS);
  EXPECT_EQ(recovered_msg.value_payload().keys_size(), VALUE2_NVALS);
  EXPECT_EQ(checkPayload(base, Hash::kByteLength,
        (const byte_t*)recovered_msg.value_payload().base().data(),
        recovered_msg.value_payload().base().length()), true);

  delete[] serialized;
}

TEST(TestMessage, TestResponse) {
  UMessage msg;

  msg.set_type(UMessage::RESPONSE);
  msg.set_source(1);

  auto response = msg.mutable_response_payload();
  // add status
  response->set_stat((int)ErrorCode::kOK);
  // add payload
  byte_t version[TEST_VERSION_SIZE];
  randomVals(version, TEST_VERSION_SIZE);
  response->set_value(version, TEST_VERSION_SIZE);

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.has_response_payload(), true);
  EXPECT_EQ(recovered_msg.type(), UMessage::RESPONSE);
  EXPECT_EQ(recovered_msg.response_payload().has_stat(), true);
  EXPECT_EQ(recovered_msg.response_payload().stat(), (int)ErrorCode::kOK);
  EXPECT_EQ(checkPayload(version, TEST_VERSION_SIZE,
     (const byte_t*)recovered_msg.response_payload().value().data(),
     recovered_msg.response_payload().value().length()), true);

  delete[] serialized;
}

TEST(TestMessage, TestGetRequest) {
  UMessage msg;
  populateHeader(&msg, UMessage::GET_REQUEST);

  // no payload to add

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UMessage::GET_REQUEST);

  delete[] serialized;
}


TEST(TestMessage, TestBranchRequest) {
  UMessage msg;
  populateHeader(&msg, UMessage::BRANCH_REQUEST);

  // add payload
  auto request = msg.mutable_request_payload();
  byte_t branch[TEST_BRANCH_SIZE];
  randomVals(branch, TEST_BRANCH_SIZE);
  request->set_branch(branch, TEST_BRANCH_SIZE);

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UMessage::BRANCH_REQUEST);
  EXPECT_EQ(recovered_msg.has_request_payload(), true);
  EXPECT_EQ(checkPayload(branch, TEST_BRANCH_SIZE,
      (const byte_t*)recovered_msg.request_payload().branch().data(),
      recovered_msg.request_payload().branch().length()), true);

  delete[] serialized;
}

// MOVE_REQUEST and MOVE_RESPONSE are the same as BRANCH_REQUEST and RESPONSE
TEST(TestMessage, TestMergeRequest) {
  UMessage msg;
  populateHeader(&msg, UMessage::MERGE_REQUEST);

  // add payload
  auto request = msg.mutable_request_payload();
  byte_t ref_branch[TEST_BRANCH_SIZE];
  randomVals(ref_branch, TEST_BRANCH_SIZE);
  request->set_ref_branch(ref_branch, TEST_BRANCH_SIZE);

  // add ValuePayload
  auto val = msg.mutable_value_payload();
  val->set_type(static_cast<int>(UType::kList));
  byte_t base[Hash::kByteLength];
  randomVals(base, Hash::kByteLength);
  val->set_base(base, Hash::kByteLength);
  val->set_pos(0);
  val->set_dels(0);
  for (int i=0; i<VALUE2_NVALS; i++) {
    byte_t value[TEST_VALUE_SIZE];
    randomVals(value, TEST_VALUE_SIZE);
    val->add_values(value, TEST_VALUE_SIZE);

    byte_t key[TEST_KEY_SIZE];
    randomVals(key, TEST_KEY_SIZE);
    val->add_keys(key, TEST_KEY_SIZE);
  }

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UMessage::MERGE_REQUEST);
  EXPECT_EQ(recovered_msg.has_request_payload(), true);
  EXPECT_EQ(checkPayload(ref_branch, TEST_BRANCH_SIZE,
    (const byte_t*)recovered_msg.request_payload().ref_branch().data(),
    recovered_msg.request_payload().ref_branch().length()), true);
  EXPECT_EQ(recovered_msg.has_value_payload(), true);
  EXPECT_EQ(recovered_msg.value_payload().values_size(), VALUE2_NVALS);
  EXPECT_EQ(recovered_msg.value_payload().keys_size(), VALUE2_NVALS);
  EXPECT_EQ(checkPayload(base, Hash::kByteLength,
        (const byte_t*)recovered_msg.value_payload().base().data(),
        recovered_msg.value_payload().base().length()), true);

  delete[] serialized;
}
