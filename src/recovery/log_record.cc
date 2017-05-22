// Copyright (c) 2017 The Ustore Authors

#include "recovery/log_record.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "utils/logging.h"

namespace ustore {
namespace recovery {

std::string LogRecord::ToString() {
  std::string ret;
  int64_t reserve_space = 0;
  /*
   * Reserve space for data_length
   * */
  ret.append(reinterpret_cast<const char*>(&reserve_space),
             sizeof(reserve_space));
  ret.append(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
  ret.append(reinterpret_cast<const char*>(&version), sizeof(version));
  ret.append(reinterpret_cast<const char*>(&logcmd), sizeof(logcmd));
  ret.append(reinterpret_cast<const char*>(&log_sequence_number),
             sizeof(log_sequence_number));
  ret.append(reinterpret_cast<const char*>(&key_length), sizeof(key_length));
  ret.append(reinterpret_cast<const char*>(&value_length),
             sizeof(value_length));
  if (key != nullptr) {
    ret.append(reinterpret_cast<const char*>(key), key_length);
  }
  if (value != nullptr) {
    ret.append(reinterpret_cast<const char*>(value), value_length);
  }
  data_length = ret.length();
  ret.replace(0, sizeof(data_length),
              reinterpret_cast<const char*>(&data_length), sizeof(data_length));
  return ret;
}

void LogRecord::FromString(const char* log_data) {
  size_t pos = 0;
  checksum = *reinterpret_cast<const int64_t*>(log_data + pos);
  pos += sizeof(checksum);
  version = *reinterpret_cast<const int16_t*>(log_data + pos);
  pos += sizeof(version);
  logcmd = *reinterpret_cast<const LogCommand*>(log_data + pos);
  pos += sizeof(logcmd);
  log_sequence_number = *reinterpret_cast<const int64_t*>(log_data + pos);
  pos += sizeof(log_sequence_number);
  key_length = *reinterpret_cast<const int64_t*>(log_data + pos);
  pos += sizeof(key_length);
  value_length = *reinterpret_cast<const int64_t*>(log_data + pos);
  pos += sizeof(value_length);
  key = nullptr;
  value = nullptr;
  if (key_length != 0) {
    key_data.reset(new byte_t[key_length]);
    memcpy(key_data.get(), log_data + pos, key_length);
    pos += key_length;
    key = key_data.get();
  }
  if (value_length != 0) {
    value_data.reset(new byte_t[value_length]);
    memcpy(value_data.get(), log_data + pos, value_length);
    pos += value_length;
    value = value_data.get();
  }
  data_length = pos + sizeof(data_length);
}

/*
 * Currently, the checksum function is very simple, can change to complex one
 * */
int64_t LogRecord::ComputeChecksum() {
  checksum = version * 12 + static_cast<int16_t>(logcmd)
             * (log_sequence_number % 100) + key_length - value_length;
  return checksum;
}

}  // end of namespace recovery
}  // end of namespace ustore
