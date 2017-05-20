// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_RECOVERY_LOG_WORKER_H_
#define USTORE_RECOVERY_LOG_WORKER_H_

#include"recovery/log_record.h"

#include <condition_variable>
#include <mutex>

#include "hash/hash.h"
#include "recovery/log_thread.h"
#include "spec/slice.h"

namespace ustore {
namespace recovery {

/*
 * Each site should create a LogWorker to write log for the hashtable
 * */
class LogWorker : public LogThread {
 public:
  LogWorker(int stage = 0);
  ~LogWorker();
  bool Init(const char* log_dir, const char* log_filename);

  /*
   * @brief: get the defined timeout
   * */
  int64_t GetTimeout();
  /*
   * @brief: set the timeout according to your application requirement
   * */
  void SetTimeout(int64_t timeout_ms);
  /*
   * @brief: get the size of the log buffer that is maintained in memory
   * */
  int64_t GetBufferSize();
  /*
   * @brief: set the size of the log buffer, and should resize the buffer
   * */
  bool SetBufferSize(int64_t buf_size);
  /*
   * @brief: get the condition_variable
   * */
  std::condition_variable* GetFlushCV();
  std::condition_variable* GetWorkerCV();
  /*
   * @brief: set the condition_variable
   * */
  void SetFlushCV(std::condition_variable* cv);
  void SetWorkerCV(std::condition_variable* cv);
  /*
   * @brief: get the current position in the log buffer
   * */
  int64_t GetBufferIndice();
  /*
   * @brief: append log data in the log buffer, the log data should contain meta
   * infomation, which is also useful for the recovery.
   * */
  bool WriteLog(const char* data, uint64_t data_length);
  /*
   * @brief: flush the data in the log buffer to disk and reset related
   * variables
   * */
  int Flush();
  /*
   * @brief: get the synchronization type
   * */
  int GetSyncType();
  /*
   * @brief: set the synchroniation type
   * @params [in] type: 1==>strong consistency, 0==>weak consistency
   * */
  bool SetSyncType(int type);

  /*
   * @brief: create a thread to do the work
   * */
  void* Run();

  /*
   * @brief: Update branch version
   * @return: return the log sequence number for the update
   * */
  // TODO(yaochang): change Slice -> const Slice&, Hash -> const Hash&
  // TODO(yaochang): could remove Slice*, Hash* methods
  int64_t Update(const Slice& branch_name, const Hash& new_version);
  //int64_t Update(Slice* branch_name, Hash* new_version);
  /*
   * @brief: Rename the branch name to a new one
   * @return: return the log sequence number for the rename operation
   * */
  int64_t Rename(const Slice& branch_name, const Slice& new_branch_name);
  //int64_t Rename(Slice* branch_name, Slice* new_branch_name);
  /*
   * @brief: Remove the branch from the branch head table
   * @return: return the log sequence number for the remove operation
   * */
  int64_t Remove(const Slice& branch_name);
  //int64_t Remove(Slice* branch_name);

		/*
			* @brief: Read one log record from log file
			* @return: return one log record.
			* */
		bool ReadOneLogRecord(LogRecord& record);

 private:
		int stage_; 	// 0 -> normal execution, 1 -> restart/recovery 
  int64_t timeout_;       // timeout to flush the log to disk
  // TODO(yaochang): why not directly use instance instead of a pointer?
		// operator= on instance is disabled
  std::condition_variable* flush_cv_;  // wait_for timeout or the buffer is full
  std::mutex flush_mutex_;
  int64_t buffer_size_;    // the size of the log buffer
  int64_t buffer_indice_;  // current position
  std::mutex buffer_lock_;
  char* buffer_;  // the log data buffer
  const char* log_dir_;
  const char* log_filename_;
  int fd_;
  /*
   * log synchronization type may affect the system performance
   * strong consistency ==> 1, each operation can commit only after the log is
   * flushed to disk
   * weak consistency ==> 0, which is also the default setting, the log is
   * flushed to disk when the buffer is full or the timeout is due
   * */
  int log_sync_type_;
  int64_t log_sequence_number_;
};

}  // namespace recovery
}  // namespace ustore

#endif   // USTORE_RECOVERY_LOG_WORKER_H_
