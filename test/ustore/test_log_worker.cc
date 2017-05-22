// Copyright (c) 2017 The Ustore Authors

#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include "gtest/gtest.h"

#include "hash/hash.h"
#include "recovery/log_record.h"
#include "recovery/log_worker.h"

const char log_dir[] = ".";
const char log_file[] = "testlog.rec";
const char log_path[] = "./testlog.rec";
const ustore::byte_t raw_str[] = "The quick brown fox jumps over the lazy dog";
const char name_data[] = "I am a branch name";
const char new_name[] = "I am a new name";

TEST(Recovery, LogWorkerInit) {
  ustore::recovery::LogWorker worker;
  int check_log_file = access(log_path, F_OK);
  int delete_file;
  if(check_log_file == 0) {
    delete_file = unlink(log_path);
    if(delete_file == -1) {
      LOG(WARNING) << "log file already exists";
    }
  }
  bool ret = worker.Init(log_dir, log_file);
  EXPECT_EQ(ret, true);
  worker.setTimeout(4000);
  EXPECT_EQ(worker.timeout(), 4000);
  EXPECT_EQ(worker.bufferIndice(), 0);
  EXPECT_TRUE(worker.setSyncType(1));
  EXPECT_EQ(worker.syncType(), 1);
  ustore::Slice branch_name(name_data);
  ustore::Slice new_branch_name(new_name);
  ustore::Hash version = ustore::Hash::ComputeFrom(raw_str, 43);
  EXPECT_EQ(worker.Update(branch_name, version), 1);
  EXPECT_EQ(worker.Rename(branch_name, new_branch_name), 2);
  EXPECT_EQ(worker.Remove(new_branch_name), 3);
}

TEST(Recovery, LogWorkerRead) {
  ustore::recovery::LogWorker worker(1);
  bool ret = worker.Init(log_dir, log_file);
  EXPECT_EQ(ret, true);
  ustore::recovery::LogRecord record;
  bool read_flag;
  // Update Command
  read_flag = worker.ReadOneLogRecord(&record);
  EXPECT_EQ(read_flag, true);
  EXPECT_TRUE(record.logcmd == ustore::recovery::LogCommand::kUpdate);
  EXPECT_EQ(record.key_length, strlen(name_data));
  size_t hash_len = ustore::Hash::kByteLength;
  EXPECT_EQ(record.value_length, hash_len);
  EXPECT_EQ(0, std::memcmp(record.key, name_data, record.key_length));
  // Rename Command
  read_flag = worker.ReadOneLogRecord(&record);
  EXPECT_EQ(read_flag, true);
  EXPECT_EQ(record.key_length, strlen(name_data));
  EXPECT_EQ(record.value_length, strlen(new_name));
  EXPECT_TRUE(record.logcmd == ustore::recovery::LogCommand::kRename);
  EXPECT_EQ(0, std::memcmp(record.key, name_data, record.key_length));
  EXPECT_EQ(0, std::memcmp(record.value, new_name, record.value_length));
  // Remove Command
  read_flag = worker.ReadOneLogRecord(&record);
  EXPECT_EQ(read_flag, true);
  EXPECT_EQ(record.key_length, strlen(new_name));
  EXPECT_EQ(record.value_length, 0);
  EXPECT_TRUE(record.logcmd == ustore::recovery::LogCommand::kRemove);
  EXPECT_EQ(0, std::memcmp(record.key, new_name, record.key_length));
}
