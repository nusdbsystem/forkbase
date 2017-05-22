// Copyright (c) 2017 The Ustore Authors.
#include <stdlib.h>
#include "gtest/gtest.h"
#include "proto/messages.pb.h"
#include "types/type.h"
#include "hash/hash.h"

using ustore::UStoreMessage;
using ustore::GetResponsePayload;
using ustore::PutRequestPayload;
using ustore::PutResponsePayload;
using ustore::BranchRequestPayload;
using ustore::RenameRequestPayload;
using ustore::MergeRequestPayload;
using ustore::MergeResponsePayload;
using ustore::Value2Payload;
using ustore::UCellPayload;
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

  // source: 1 (first client thread)
  msg->set_source(1); 
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
  PutRequestPayload *payload = msg.mutable_put_request_payload();
  Value2Payload *val = payload->mutable_value();
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
  UStoreMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UStoreMessage::PUT_REQUEST);
  EXPECT_EQ(recovered_msg.has_put_request_payload(), true);
  EXPECT_EQ(recovered_msg.put_request_payload().value().type(),
              static_cast<int>(UType::kList));
  // check that there are VALUE2_NVALS items in vals and keys
  EXPECT_EQ(recovered_msg.put_request_payload().value().values_size(),
                  VALUE2_NVALS);
  EXPECT_EQ(recovered_msg.put_request_payload().value().keys_size(),
                  VALUE2_NVALS);
  EXPECT_EQ(checkPayload(base, Hash::kByteLength,
        (const byte_t*)recovered_msg.put_request_payload().value()
                        .base().data(),
          recovered_msg.put_request_payload().value().base().length()),
          true);

  delete[] serialized;
}

TEST(TestMessage, TestPutResponse) {
  UStoreMessage msg;
  populateHeader(&msg, UStoreMessage::PUT_RESPONSE);

  // add status
  msg.set_status((int)ErrorCode::kOK);
  // add payload
  PutResponsePayload *payload
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
  EXPECT_EQ(recovered_msg.has_status(), true); 
  EXPECT_EQ(recovered_msg.status(), (int)ErrorCode::kOK);
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

  // add status
  msg.set_status((int)ErrorCode::kOK);
  // add payload
  GetResponsePayload *payload
    = msg.mutable_get_response_payload();
  UCellPayload *ucell = payload->mutable_meta();
  
  byte_t value[TEST_VALUE_SIZE];
  randomVals(value, TEST_VALUE_SIZE);
  ucell->set_value(value, TEST_VALUE_SIZE);

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UStoreMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UStoreMessage::GET_RESPONSE);
  EXPECT_EQ(recovered_msg.has_get_response_payload(), true);
  EXPECT_EQ(recovered_msg.has_status(), true);
  EXPECT_EQ(recovered_msg.status(), (int)ErrorCode::kOK);
  EXPECT_EQ(checkPayload((const byte_t*)recovered_msg.get_response_payload().
                        meta().value().data(), TEST_VALUE_SIZE,
                        value, TEST_VALUE_SIZE), true); 
  delete[] serialized;
}

TEST(TestMessage, TestBranchRequest) {
  UStoreMessage msg;
  populateHeader(&msg, UStoreMessage::BRANCH_REQUEST);

  // add payload
  BranchRequestPayload *payload
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

// BRANCH_RESPONSE, MOVE_RESPONSE, MERGE_RESPONSE are the same
TEST(TestMessage, TestBranchResponse) {
  UStoreMessage msg;
  populateHeader(&msg, UStoreMessage::BRANCH_RESPONSE);
  
  // test another status other than SUCESS
  msg.set_status((int)ErrorCode::kInvalidRange);
  // no payload

  // serialized
  int msg_size = msg.ByteSize();
  byte_t* serialized = new byte_t[msg_size];
  EXPECT_EQ(msg.SerializeToArray(serialized, msg_size), true);

  // deserialized
  UStoreMessage recovered_msg;
  EXPECT_EQ(recovered_msg.ParseFromArray(serialized, msg_size), true);
  EXPECT_EQ(recovered_msg.type(), UStoreMessage::BRANCH_RESPONSE);
  EXPECT_EQ(recovered_msg.has_status(), true);
  EXPECT_EQ(recovered_msg.status(), (int)ErrorCode::kInvalidRange);

  delete[] serialized;
}

// MOVE_REQUEST and MOVE_RESPONSE are the same as BRANCH_REQUEST and RESPONSE
TEST(TestMessage, TestMergeRequest) {
  UStoreMessage msg;
  populateHeader(&msg, UStoreMessage::MERGE_REQUEST);

  // add payload
  MergeRequestPayload *payload
    = msg.mutable_merge_request_payload();
  byte_t target_branch[TEST_BRANCH_SIZE];
  randomVals(target_branch, TEST_BRANCH_SIZE);
  payload->set_target_branch(target_branch, TEST_BRANCH_SIZE);
  byte_t ref_branch[TEST_BRANCH_SIZE];
  randomVals(ref_branch, TEST_BRANCH_SIZE);
  payload->set_ref_branch(ref_branch, TEST_BRANCH_SIZE);

  // add Value2Payload 
  Value2Payload *val = payload->mutable_value();
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

  EXPECT_EQ(recovered_msg.merge_request_payload().value().values_size(),
                  VALUE2_NVALS);
  EXPECT_EQ(recovered_msg.merge_request_payload().value().keys_size(),
                  VALUE2_NVALS);
  EXPECT_EQ(checkPayload(base, Hash::kByteLength,
        (const byte_t*)recovered_msg.merge_request_payload().value()
                        .base().data(),
          recovered_msg.merge_request_payload().value().base().length()),
          true);

  delete[] serialized;
}
