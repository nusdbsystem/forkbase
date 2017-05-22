// Copyright (c) 2017 The Ustore Authors

#include<cstring>
#include<string>
#include<sstream>
#include<utility>
#include<iomanip>
#include"gtest/gtest.h"
#include"recovery/log_record.h"
#include"utils/logging.h"

char testkey[] = "12djasfkj4iadusf4efjsdf";
char testvalue[] = "odadesades1sda1645f";
int64_t checksum_result = 1237;

TEST(Recovery, LogRecordChecksum) {
  ustore::recovery::LogRecord record;
  EXPECT_EQ(record.version, 1);
  EXPECT_EQ(record.key, nullptr);
  EXPECT_EQ(record.value, nullptr);
  record.logcmd = ustore::recovery::LogCommand::kUpdate;
  record.log_sequence_number = 11;
  record.key = reinterpret_cast<const ustore::byte_t*>(testkey);
  record.value = reinterpret_cast<const ustore::byte_t*>(testvalue);
  record.key_length = strlen(testkey);
  record.value_length = strlen(testvalue);
  int64_t checksum_ret = record.ComputeChecksum();
  LOG(INFO) << "returned checksum: " << checksum_ret;
}

TEST(Recovery, LogRecordToString) {
  ustore::recovery::LogRecord record;
  record.logcmd = ustore::recovery::LogCommand::kUpdate;
  record.log_sequence_number = 11;
  record.key = reinterpret_cast<const ustore::byte_t*>(testkey);
  record.value = reinterpret_cast<const ustore::byte_t*>(testvalue);
  record.key_length = strlen(testkey);
  record.value_length = strlen(testvalue);
  record.ComputeChecksum();
	std::string str = record.ToString();
  EXPECT_EQ(record.data_length, 86);
}
