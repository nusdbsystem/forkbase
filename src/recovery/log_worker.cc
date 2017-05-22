// Copyright (c) 2017 The UStore Authors.

#include "recovery/log_worker.h"

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <iostream>

#include "recovery/log_record.h"
#include "utils/logging.h"

namespace ustore {
namespace recovery {

LogWorker::LogWorker(int stage) {
  stage_ = stage;
  buffer_ = new char[buffer_size_];
}

LogWorker::~LogWorker() {
  delete[] buffer_;
}

bool LogWorker::Init(const char* log_dir, const char* log_filename) {
  std::string path = string(log_dir) + "/" + string(log_filename);
  if (stage_ == 0) {
    fd_ = open(path.c_str(), O_RDWR|O_CREAT, 00666);
  } else {
    fd_ = open(path.c_str(), O_RDWR, 00666);
  }
  if (fd_ < 0) {
    LOG(INFO) << "Log File Path: " << path;
    LOG(FATAL) << "UStore::Recovery::LogWorker Init: open log file fails!";
    return false;
  }
  return true;
}

bool LogWorker::setBufferSize(int64_t buf_size) {
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

bool LogWorker::setSyncType(int type) {
  if (type != 0 && type != 1) {
    LOG(WARNING) << "UStore::Recovery::LogWorker "
                 << "SetSyncType: input is illegal!";
    return false;
  }
  log_sync_type_ = type;
  return true;
}

bool LogWorker::WriteLog(const char* data, uint64_t data_length) {
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
      flush_cv_.notify_all();  // update the loop in log worker
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
    if (flush_cv_.wait_for(lck, std::chrono::milliseconds(timeout_))
        == std::cv_status::timeout) {
      buffer_lock_.lock();
      Flush();
      buffer_lock_.unlock();
    }
  }
  return nullptr;
}

int64_t LogWorker::Update(const Slice& branch_name, const Hash& new_version) {
  return WriteRecord(LogCommand::kUpdate, branch_name,
                     Slice(new_version.value(), Hash::kByteLength));
}

int64_t LogWorker::Rename(const Slice& branch_name,
                          const Slice& new_branch_name) {
  return WriteRecord(LogCommand::kRename, branch_name, new_branch_name);
}

int64_t LogWorker::Remove(const Slice& branch_name) {
  return WriteRecord(LogCommand::kRemove, branch_name, Slice());
}

int64_t LogWorker::WriteRecord(LogCommand cmd,
                               const Slice& key, const Slice& value) {
  LogRecord record;
  record.key = key.data();
  record.key_length = key.len();
  record.value = value.data();
  record.value_length = value.len();
  record.logcmd = cmd;
  buffer_lock_.lock();
  int64_t ret_lsn = log_sequence_number_ + 1;
  record.ComputeChecksum();
  std::string log_data = record.ToString();
  bool write_ret = WriteLog(log_data.data(), log_data.length());
  if (write_ret == false) {
    ret_lsn = -1;
  }
  log_sequence_number_ = ret_lsn;
  buffer_lock_.unlock();
  return ret_lsn;
}

bool LogWorker::ReadOneLogRecord(LogRecord* record) {
  int64_t data_length = 0;
  int read_ret = 0;
  read_ret = read(fd_, &data_length, sizeof(data_length));
  if (read_ret == -1) {
    LOG(FATAL) << "Read log length fails";
    return false;
  }
  char* log_data = new char[data_length];
  read_ret = read(fd_, log_data, data_length - sizeof(data_length));
  if (read_ret == -1) {
    LOG(FATAL) << "Read log data fails";
    return false;
  }
  record->FromString(log_data);
  delete[] log_data;
  return true;
}

}  // end of namespace recovery
}  // end of namespace ustore
