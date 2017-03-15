// Copyright (c) 2017 The Ustore Authors.
#include <stdlib.h>
#include "gtest/gtest.h"
#include "proto/messages.pb.h"
#include "types/type.h"

using ustore::UStoreMessage;
using ustore::byte_t;
const size_t TEST_KEY_SIZE = 32;
const size_t TEST_VERSION_SIZE = 32;
const size_t TEST_BRANCH_SIZE = 32;
const size_t TEST_VALUE_SIZE = 128;

void randomVals(byte_t* buf, int size) {
  srand(time(NULL));
  for (int i = 0; i < size; i++)
    buf[i] = rand() & 0xFF;
}

void populateHeader(UStoreMessage *msg, UStoreMessage::Type type) {
  msg->set_type(type);

  // generate random vals
  byte_t key[TEST_KEY_SIZE];
  byte_t version[TEST_VERSION_SIZE];
  byte_t branch[TEST_BRANCH_SIZE];

  randomVals(key, TEST_KEY_SIZE);
  randomVals(version, TEST_VERSION_SIZE);
  randomVals(branch, TEST_BRANCH_SIZE);

  // set fields
  msg->set_key(key, TEST_KEY_SIZE);
  msg->set_version(version, TEST_VERSION_SIZE);
  msg->set_branch(branch, TEST_BRANCH_SIZE);
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
  UStoreMessage msg;
  populateHeader(&msg, UStoreMessage::PUT_REQUEST);

  // add payload
  UStoreMessage::PutRequestPayload *payload = msg.mutable_put_request_payload();
  byte_t value[TEST_VALUE_SIZE];
  randomVals(value, TEST_VALUE_SIZE);
  payload->set_value(value, TEST_VALUE_SIZE);

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UStoreMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UStoreMessage::PUT_REQUEST);
  EXPECT_EQ(recovered_msg.has_put_request_payload(), true);
  EXPECT_EQ(checkPayload(value, TEST_VALUE_SIZE,
        (const byte_t*)recovered_msg.put_request_payload().value().data(),
          recovered_msg.put_request_payload().value().length()), true);

  delete[] serialized;
}

TEST(TestMessage, TestPutResponse) {
  UStoreMessage msg;
  populateHeader(&msg, UStoreMessage::PUT_RESPONSE);

  // add payload
  UStoreMessage::PutResponsePayload *payload
    = msg.mutable_put_response_payload();
  byte_t version[TEST_VERSION_SIZE];
  randomVals(version, TEST_VERSION_SIZE);
  payload->set_new_version(version, TEST_VERSION_SIZE);

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UStoreMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UStoreMessage::PUT_RESPONSE);
  EXPECT_EQ(recovered_msg.has_put_response_payload(), true);
  EXPECT_EQ(checkPayload(version, TEST_VERSION_SIZE,
     (const byte_t*)recovered_msg.put_response_payload().new_version().data(),
     recovered_msg.put_response_payload().new_version().length()), true);

  delete[] serialized;
}

TEST(TestMessage, TestGetRequest) {
  UStoreMessage msg;
  populateHeader(&msg, UStoreMessage::GET_REQUEST);

  // no payload to add

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UStoreMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UStoreMessage::GET_REQUEST);

  delete[] serialized;
}


// Get_Response is very similar to Put_Request
TEST(TestMessage, TestGetResponse) {
  UStoreMessage msg;
  populateHeader(&msg, UStoreMessage::GET_RESPONSE);

  // add payload
  UStoreMessage::GetResponsePayload *payload
    = msg.mutable_get_response_payload();
  byte_t value[TEST_VALUE_SIZE];
  randomVals(value, TEST_VALUE_SIZE);
  payload->set_value(value, TEST_VALUE_SIZE);

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UStoreMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UStoreMessage::GET_RESPONSE);
  EXPECT_EQ(recovered_msg.has_get_response_payload(), true);
  EXPECT_EQ(checkPayload(value, TEST_VALUE_SIZE,
        (const byte_t*)recovered_msg.get_response_payload().value().data(),
        recovered_msg.get_response_payload().value().length()), true);

  delete[] serialized;
}

TEST(TestMessage, TestBranchRequest) {
  UStoreMessage msg;
  populateHeader(&msg, UStoreMessage::BRANCH_REQUEST);

  // add payload
  UStoreMessage::BranchRequestPayload *payload
    = msg.mutable_branch_request_payload();
  byte_t branch[TEST_BRANCH_SIZE];
  randomVals(branch, TEST_BRANCH_SIZE);
  payload->set_new_branch(branch, TEST_BRANCH_SIZE);

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UStoreMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UStoreMessage::BRANCH_REQUEST);
  EXPECT_EQ(recovered_msg.has_branch_request_payload(), true);
  EXPECT_EQ(checkPayload(branch, TEST_BRANCH_SIZE,
      (const byte_t*)recovered_msg.branch_request_payload().new_branch().data(),
      recovered_msg.branch_request_payload().new_branch().length()), true);

  delete[] serialized;
}

TEST(TestMessage, TestBranchResponse) {
  UStoreMessage msg;
  populateHeader(&msg, UStoreMessage::BRANCH_RESPONSE);

  // add payload
  UStoreMessage::BranchResponsePayload *payload
    = msg.mutable_branch_response_payload();
  payload->set_status(false);

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UStoreMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UStoreMessage::BRANCH_RESPONSE);
  EXPECT_EQ(recovered_msg.has_branch_response_payload(), true);
  EXPECT_EQ(recovered_msg.branch_response_payload().status(), false);

  delete[] serialized;
}

// MOVE_REQUEST and MOVE_RESPONSE are the same as BRANCH_REQUEST and RESPONSE


TEST(TestMessage, TestMergeRequest) {
  UStoreMessage msg;
  populateHeader(&msg, UStoreMessage::MERGE_REQUEST);

  // add payload
  UStoreMessage::MergeRequestPayload *payload
    = msg.mutable_merge_request_payload();
  byte_t target_branch[TEST_BRANCH_SIZE];
  randomVals(target_branch, TEST_BRANCH_SIZE);
  payload->set_target_branch(target_branch, TEST_BRANCH_SIZE);
  byte_t ref_branch[TEST_BRANCH_SIZE];
  randomVals(ref_branch, TEST_BRANCH_SIZE);
  payload->set_ref_branch(ref_branch, TEST_BRANCH_SIZE);
  byte_t value[TEST_VALUE_SIZE];
  randomVals(value, TEST_VALUE_SIZE);
  payload->set_value(value, TEST_VALUE_SIZE);

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UStoreMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UStoreMessage::MERGE_REQUEST);
  EXPECT_EQ(recovered_msg.has_merge_request_payload(), true);
  EXPECT_EQ(checkPayload(target_branch, TEST_BRANCH_SIZE,
    (const byte_t*)recovered_msg.merge_request_payload().target_branch().data(),
    recovered_msg.merge_request_payload().target_branch().length()), true);
  EXPECT_EQ(checkPayload(ref_branch, TEST_BRANCH_SIZE,
    (const byte_t*)recovered_msg.merge_request_payload().ref_branch().data(),
    recovered_msg.merge_request_payload().ref_branch().length()), true);
  EXPECT_EQ(checkPayload(value, TEST_VALUE_SIZE,
    (const byte_t*)recovered_msg.merge_request_payload().value().data(),
    recovered_msg.merge_request_payload().value().length()), true);

  delete[] serialized;
}

TEST(TestMessage, TestMergeResponse) {
  UStoreMessage msg;
  populateHeader(&msg, UStoreMessage::MERGE_RESPONSE);

  // add payload
  UStoreMessage::MergeResponsePayload *payload
    = msg.mutable_merge_response_payload();
  payload->set_status(true);

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UStoreMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UStoreMessage::MERGE_RESPONSE);
  EXPECT_EQ(recovered_msg.has_merge_response_payload(), true);
  EXPECT_EQ(recovered_msg.merge_response_payload().status(), true);

  delete[] serialized;
}

