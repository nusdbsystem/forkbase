// Copyright (c) 2017 The Ustore Authors

#include"recovery/log_record.h"

#include<stdio.h>
#include<stdlib.h>
#include<string.h>

namespace ustore {
namespace recovery {

LogRecord::LogRecord() {
  checksum = 0;
  version = 1;
  logcmd = 0;
  log_sequence_number = 0;
  key_length = 0;
  value_length = 0;
  key = nullptr;
  value = nullptr;
  data_length = 0;
}

LogRecord::~LogRecord() {
	//if(key != nullptr) delete[] key;
	//if(value != nullptr) delete[] value;
}

std::string LogRecord::ToString() {
  char ret[200];
		int64_t reserve_space = 0;
  size_t pos = 0;
  memset(ret, 0, sizeof(ret));
		/*
			* Reserve space for data_length
			* */
		memcpy(ret+pos, &reserve_space,sizeof(int64_t));
		pos = pos + sizeof(int64_t);
  memcpy(ret+pos, &checksum, sizeof(int64_t));
  pos = pos + sizeof(int64_t);
  memcpy(ret+pos, &version, sizeof(int16_t));
  pos = pos + sizeof(int16_t);
		memcpy(ret+pos, &logcmd, sizeof(int16_t));
		pos = pos + sizeof(int16_t);
  memcpy(ret+pos, &log_sequence_number, sizeof(int64_t));
  pos = pos + sizeof(int64_t);
  memcpy(ret+pos, &key_length, sizeof(int64_t));
  pos = pos + sizeof(int64_t);
  memcpy(ret+pos, &value_length, sizeof(int16_t));
  pos = pos + sizeof(int16_t);
  if (key != nullptr) {
    memcpy(ret+pos, key, (size_t)key_length);
    pos = pos + (size_t) key_length;
  }
  if (value != nullptr) {
    memcpy(ret+pos, value, (size_t)value_length);
    pos = pos + (size_t) value_length;
  }
  data_length = (int64_t) pos;
		memcpy(ret, &data_length, sizeof(int64_t));  // fill the data_length
		std::string ret_str(ret, pos); 
  return ret_str;
}

int64_t LogRecord::GetLength() {
  if (data_length > 0) {
    return data_length;
  } else {
    int64_t ret = 0;
    size_t a1 = sizeof(int64_t)*4 + sizeof(int16_t)*3;
    ret = (int64_t)a1;
    if (key != nullptr) {
      ret = ret + key_length;
    }
    if (value != nullptr) {
      ret = ret + value_length;
    }
    return ret;
  }
}

/*
 * Currently, the checksum function is very simple, can change to complex one
 * */
int64_t LogRecord::ComputeChecksum() {
  checksum = version * 12 + logcmd * (log_sequence_number % 100) + key_length
             - value_length;
  return checksum;
}

}  // end of namespace recovery
}  // end of namespace ustore
