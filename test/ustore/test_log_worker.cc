// Copyright (c) 2017 The Ustore Authors

#include<cstring>
#include<string>
#include<sstream>
#include<utility>
#include"gtest/gtest.h"
#include"recovery/log_worker.h"
#include"hash/hash.h"
#include"utils/logging.h"

const char log_dir[] = ".";
const char log_file[] = "testlog";
const char log_path[] = "./testlog";
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
	worker.SetTimeout(4000);
	EXPECT_EQ(worker.GetTimeout(), 4000);
	EXPECT_EQ(worker.GetBufferIndice(), 0);
	EXPECT_TRUE(worker.SetSyncType(1));
	EXPECT_EQ(worker.GetSyncType(), 1);
	ustore::Slice branch_name(name_data);
	ustore::Slice new_branch_name(new_name);
	ustore::Hash version;
	version.Compute(raw_str, 43);
	EXPECT_EQ(worker.Update(branch_name, version), 1);
	EXPECT_EQ(worker.Rename(branch_name, new_branch_name), 2);
	EXPECT_EQ(worker.Remove(new_branch_name), 3);
}
