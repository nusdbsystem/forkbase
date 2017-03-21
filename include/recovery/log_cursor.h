// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_RECOVERY_LOG_CURSOR_H_
#define USTORE_RECOVERY_LOG_CURSOR_H_

#include <string>
#include "recovery/log_entry.h"

namespace ustore {
namespace recovery {

class LogCursor {
 public:
  LogCursor();
  ~LogCursor();

  /*
   * make sure all the fields are >= 0
   * */
  bool IsValid() const;
  void Reset();
  void Set(uint64_t file_id, uint64_t log_id, uint64_t offset);
  /*
   * put the cursor content to buffer
   * */
  int Serialize(char* buf, uint64_t length, uint64_t pos) const;
  /*
   * deserialize the cursor from the buffer 
   * */
  int Deserialize(const char* buf, uint64_t length, uint64_t pos);
  std::string ToString() const;
  uint64_t ToString(char* buf, uint64_t length) const;
  /*
   * @brief Read Log entry from log buffer according to the cursor
   * */
  int LoadEntry(LogCommand cmd, const char* log_data, uint64_t data_length,
                LogEntry* entry) const;
  /*
   * @brief forward the curpos forward
   * */
  int Advance(LogCommand cmd, uint64_t seq_id, uint64_t data_length);
  int Advance(const LogEntry& entry);
  /*
   * compare the age of the cursor
   * */
  bool operator<(const LogCursor& other) const;
  bool operator>(const LogCursor& other) const;
  bool operator==(const LogCursor& other) const;

 private:
  uint64_t file_id_;
  uint64_t log_id_;
  uint64_t offset_;
};  // LogCursor

// TODO(yaochang): add atomic log cursor

}  // namespace recovery
}  // namespace ustore

#endif  // USTORE_RECOVERY_LOG_CURSOR_H_
