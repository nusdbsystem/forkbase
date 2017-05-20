// Copyright (c) 2017 The UStore Authors.

#include "recovery/log_worker.h"
#include "recovery/log_record.h"

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <iostream>

#include "utils/logging.h"

namespace ustore {
namespace recovery {

LogWorker::LogWorker() {
  // TODO(yaochang): for such configuration, should have a static variable,
  //  rather than hard code use magic numbers
  timeout_ = 5000;  // default value: 5 seconds
  flush_cv_ = new std::condition_variable();
  buffer_lock_ = new std::mutex();
  buffer_size_ = 4 * 1024 * 1024;  // the default value is 4MB
  buffer_indice_ = 0;
  buffer_ = new char[buffer_size_];
  log_dir_ = nullptr;
  log_filename_ = nullptr;
  fd_ = -1;
  log_sync_type_ = 0;  // by default weak consistency is adopted
  log_sequence_number_ = 0;  // LSN
}

LogWorker::~LogWorker() {
  delete flush_cv_;
  delete buffer_lock_;
  delete[] buffer_;
}

bool LogWorker::Init(const char* log_dir, const char* log_filename) {
  log_dir_ = strdup(log_dir);
  log_filename_ = strdup(log_filename);
  char absoluate_path[200];
  int written_char = 0;
  written_char = snprintf(absoluate_path, sizeof(absoluate_path), "%s",
                          log_dir_);
  snprintf(absoluate_path + written_char, sizeof(absoluate_path), "/");
  written_char = written_char + 1;
  snprintf(absoluate_path + written_char, sizeof(absoluate_path), "%s",
           log_filename_);
  fd_ = open(absoluate_path, O_RDWR|O_CREAT, 00666);
  if (fd_ < 0) {
    LOG(INFO) << "Log File Path: " << absoluate_path;
    LOG(FATAL) << "UStore::Recovery::LogWorker Init: open log file fails!";
    return false;
  }
  return true;
}

int64_t LogWorker::GetTimeout() {
  return timeout_;
}

void LogWorker::SetTimeout(int64_t timeout_ms) {
  timeout_ = timeout_ms;
}

int64_t LogWorker::GetBufferSize() {
  return buffer_size_;
}

bool LogWorker::SetBufferSize(int64_t buf_size) {
  if (buf_size <= 0) {
    LOG(WARNING) << "UStore::Recovery::LogWorker SetBufferSize: new size < 0";
    return false;
  }
  if (buf_size < buffer_size_) {
    buffer_size_ = buf_size;
  } else {
    delete [] buffer_;
    buffer_ = new char[buf_size];
    buffer_size_ = buf_size;
  }
  return true;
}

std::condition_variable* LogWorker::GetFlushCV() {
  return flush_cv_;
}

void LogWorker::SetFlushCV(std::condition_variable* cv) {
  flush_cv_ = cv;
}

int64_t LogWorker::GetBufferIndice() {
  return buffer_indice_;
}

int LogWorker::GetSyncType() {
  return log_sync_type_;
}

bool LogWorker::SetSyncType(int type) {
  if (type != 0 && type != 1) {
    LOG(WARNING) << "UStore::Recovery::LogWorker "
                 << "SetSyncType: input is illegal!";
    return false;
  }
  log_sync_type_ = type;
  return true;
}

bool LogWorker::WriteLog(char* data, uint64_t data_length) {
  // buffer_lock_->lock();
  int ret = -1;
  if (log_sync_type_ == 1) {
    ret = write(fd_, data, data_length);
    if (ret == -1) {
      LOG(WARNING) << "LogWorker write logs failure!";
      // buffer_lock_->unlock();
      return false;
    }
    fsync(fd_);
  } else {  // weak consistency
    if (data_length + buffer_indice_ > buffer_size_) {  // log buffer is full
      ret = Flush();
      if (ret == -1) {
        LOG(WARNING)
         << "LogWorker write log failure: weak synchronization type!";
        // buffer_lock_->unlock();
        return false;
      }
      flush_cv_->notify_all();  // update the loop in log worker
    }
    memcpy(buffer_ + buffer_indice_, data, (size_t) data_length);
  }
  // buffer_lock_->unlock();
  return true;
}

int LogWorker::Flush() {
  int ret = -1;
  ret = write(fd_, buffer_, (size_t) buffer_indice_);
  if (ret == -1) {
    LOG(WARNING) << "LogWorker::Flush Fail to flush the log to disk!";
    return ret;
  }
  buffer_indice_ = 0;
  fsync(fd_);
  return 0;
}

void* LogWorker::Run() {
  std::unique_lock<std::mutex> lck(flush_mutex_);
  while (1) {
    if (flush_cv_->wait_for(lck, std::chrono::milliseconds(timeout_))
      == std::cv_status::timeout) {
       buffer_lock_->lock();
       Flush();
       buffer_lock_->unlock();
    }
  }
  return nullptr;
}

// TODO(yaochang): all update/rename/remove have almost same impl.
//  Should create a function to avoid code redundency
int64_t LogWorker::Update(Slice branch_name, Hash new_version) {
  // TODO(yaochang): memory leak here!
  LogRecord* record = new LogRecord();
  record->key_length = (int64_t) branch_name.len();
  record->value_length = (int64_t) Hash::kByteLength;
  // TODO(yaochang): why not change to const char*, so can avoid const cast
  record->key = branch_name.data();
  record->value = new_version.value();
  record->logcmd = kUpdate;
  buffer_lock_->lock();
  int64_t ret_lsn = log_sequence_number_ + 1;
  record->log_sequence_number = ret_lsn;
  record->ComputeChecksum();
  char* log_data = record->ToString();
  int64_t log_data_length = record->GetLength();
  bool write_ret = WriteLog(log_data, log_data_length);
  if (write_ret == false) ret_lsn = -1;
  log_sequence_number_ = ret_lsn;
  buffer_lock_->unlock();
  return ret_lsn;
}

int64_t LogWorker::Update(Slice* branch_name, Hash* new_version) {
  LogRecord* record = new LogRecord();
  record->key_length = (int64_t) branch_name->len();
  record->value_length = (int16_t) Hash::kByteLength;
  record->key = branch_name->data();
  record->value = new_version->value();
  record->logcmd = kUpdate;
  buffer_lock_->lock();  // enter the critical section
  int64_t ret_lsn = log_sequence_number_ + 1;
  record->log_sequence_number = ret_lsn;
  record->ComputeChecksum();
  char* log_data = record->ToString();
  int64_t log_data_length = record->GetLength();
  bool write_ret = WriteLog(log_data, log_data_length);
  if (write_ret == false) ret_lsn = -1;
  log_sequence_number_ = ret_lsn;
  buffer_lock_->unlock();
  return ret_lsn;
}

int64_t LogWorker::Rename(Slice branch_name, Slice new_branch_name) {
  LogRecord* record = new LogRecord();
  record->key_length = (int64_t) branch_name.len();
  record->value_length = (int16_t) new_branch_name.len();
  record->key = branch_name.data();
  record->value = new_branch_name.data();
  record->logcmd = kRename;
  buffer_lock_->lock();
  int64_t ret_lsn = log_sequence_number_ + 1;
  record->log_sequence_number = ret_lsn;
  record->ComputeChecksum();
  char* log_data = record->ToString();
  int64_t log_data_length = record->GetLength();
  bool write_ret = WriteLog(log_data, log_data_length);
  if(write_ret == false) {
   ret_lsn = -1;
  }
  log_sequence_number_ = ret_lsn;
  buffer_lock_->unlock();
  return ret_lsn;
}

int64_t LogWorker::Rename(Slice* branch_name, Slice* new_branch_name) {
  LogRecord* record = new LogRecord();
  record->key_length = (int64_t) branch_name->len();
  record->value_length = (int16_t) new_branch_name->len();
  record->key = branch_name->data();
  record->value = new_branch_name->data();
  record->logcmd = kRename;
  buffer_lock_->lock();
  int64_t ret_lsn = log_sequence_number_ + 1;
  record->log_sequence_number = ret_lsn;
  record->ComputeChecksum();
  char* log_data = record->ToString();
  int64_t log_data_length = record->GetLength();
  bool write_ret = WriteLog(log_data, log_data_length);
  if(write_ret == false) {
   ret_lsn = -1;
  }
  buffer_lock_->unlock();
  log_sequence_number_ = ret_lsn;
  return ret_lsn;
}

int64_t LogWorker::Remove(Slice branch_name) {
  LogRecord* record = new LogRecord();
  record->key_length = (int64_t) branch_name.len();
  record->value_length = 0;
  record->key = branch_name.data();
  record->value = nullptr;
  record->logcmd = kRemove;
  buffer_lock_->lock();
  int64_t ret_lsn = log_sequence_number_ + 1;
  record->ComputeChecksum();
  char* log_data = record->ToString();
  int64_t log_data_length = record->GetLength();
  bool write_ret = WriteLog(log_data, log_data_length);
  if(write_ret == false) {
   ret_lsn = -1;
  }
  buffer_lock_->unlock();
  log_sequence_number_ = ret_lsn;
  return ret_lsn;
}

int64_t LogWorker::Remove(Slice* branch_name) {
  LogRecord* record = new LogRecord();
  record->key_length = (int64_t) branch_name->len();
  record->value_length = 0;
  record->key = branch_name->data();
  record->value = nullptr;
  record->logcmd = kRemove;
  buffer_lock_->lock();
  int64_t ret_lsn = log_sequence_number_ + 1;
  record->ComputeChecksum();
  char* log_data = record->ToString();
  int64_t log_data_length = record->GetLength();
  bool write_ret = WriteLog(log_data, log_data_length);
  if(write_ret == false) {
   ret_lsn = -1;
  }
  log_sequence_number_ = ret_lsn;
  buffer_lock_->unlock();
  return ret_lsn;
}

}  // end of namespace recovery
}  // end of namespace ustore
