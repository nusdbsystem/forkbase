// Copyright (c) 2017 The Ustore Authors

#ifndef USTORE_RECOVERY_LOG_RECORD_H_
#define USTORE_RECOVERY_LOG_RECORD_H_

#include <string>

namespace ustore {
namespace recovery {

// TODO(yaochang): change enum to enum class
enum LogCommand : uint16_t {
  kUpdate = 111,  // Update(branch_name, version)
  kRename = 112,  // Rename(branch_name, new_branch_name)
  kRemove = 113   // Remove(branch_name)
};
  /*
   * the structure of one log record is:
   * checksum||version||lsn||key_len||value_len||key||value
   **/
struct LogRecord {
 public:
  LogRecord();
  ~LogRecord() {}
  // TODO(yaochang): change char* to std::string
  char* ToString();  // generate all the content to a string
  int64_t GetLength();  // return the string length from ToString()
  int64_t ComputeChecksum();  // Compute the checksum according to the content

  int64_t checksum;  // compute the checksum after the other fields are filled
  int16_t version;  // by default
  int16_t logcmd;  // what kind of operation
  int64_t log_sequence_number;
  int64_t key_length;
  int16_t value_length;
  // TODO(yaochang): key and value cannot be const char* ?
  char* key;
  char* value;
  int64_t data_length;
};

}  // namespace recovery
}  // namespace ustore

#endif  // USTORE_RECOVERY_LOG_RECORD_H_
