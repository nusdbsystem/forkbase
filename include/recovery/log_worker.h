// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_RECOVERY_LOG_WORKER_H_
#define USTORE_RECOVERY_LOG_WORKER_H_

#include "recovery/log_thread.h"
#include "spec/slice.h"
#include "hash/hash.h"

#include<mutex>
#include<condition_variable>

namespace ustore {
namespace recovery {


/*
 * Each site should create a LogWorker to write log for the hashtable
 * */
class LogWorker : public LogThread {
 public:
  LogWorker();
  ~LogWorker();
  bool Init(const char* log_dir, char* log_filename);

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
  bool WriteLog(char* data, uint64_t data_length);
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
		int64_t Update(Slice branch_name, Hash new_version);
		int64_t Update(Slice* branch_name, Hash* new_version);
		/*
			*	@brief: Rename the branch name to a new one
			*	@return: return the log sequence number for the rename operation
			* */
		int64_t Rename(Slice branch_name, Slice new_branch_name);
		int64_t Rename(Slice* branch_name, Slice* new_branch_name);
		/*
			* @brief: Remove the branch from the branch head table
			* @return: return the log sequence number for the remove operation
			* */
		int64_t Remove(Slice branch_name);
		int64_t Remove(Slice* branch_name);

 private:
  int64_t timeout_;       // timeout to flush the log to disk
  std::condition_variable* flush_cv_;  // wait_for timeout or the buffer is full
  std::mutex flush_mutex_;
  int64_t buffer_size_;    // the size of the log buffer
  int64_t buffer_indice_;  // current position
  std::mutex* buffer_lock_;
  char* buffer_;  // the log data buffer
  char* log_dir_;
  char* log_filename_;
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
};  // end of class LogWorker
}  // end of namespace recovery
}  // end of namespace ustore
#endif   // USTORE_RECOVERY_LOG_WORKER_H_
