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

LogWorker::LogWorker(int stage) {
  // TODO(yaochang): for such configuration, should have a static variable,
  //  rather than hard code use magic numbers
		stage_ = stage;
  timeout_ = 5000;  // default value: 5 seconds
		flush_cv_ = new std::condition_variable();
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
		if(stage_ == 0) { 
  	fd_ = open(absoluate_path, O_RDWR|O_CREAT, 00666);
		}else{
			fd_ = open(absoluate_path, O_RDWR, 00666);
		}
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
       buffer_lock_.lock();
       Flush();
       buffer_lock_.unlock();
    }
  }
  return nullptr;
}

// TODO(yaochang): all update/rename/remove have almost same impl.
//  Should create a function to avoid code redundency
int64_t LogWorker::Update(const Slice& branch_name, const Hash& new_version) {
  LogRecord record;
  record.key_length = (int64_t) branch_name.len();
  record.value_length = (int64_t) Hash::kByteLength;
  record.key = branch_name.data();
  record.value = new_version.value();
  record.logcmd = (int16_t)LogCommand::kUpdate;
  buffer_lock_.lock();
  int64_t ret_lsn = log_sequence_number_ + 1;
  record.log_sequence_number = ret_lsn;
  record.ComputeChecksum();
		std::string log_data = record.ToString();
  int64_t log_data_length = record.GetLength();
  bool write_ret = WriteLog(log_data.c_str(), log_data_length);
  if (write_ret == false) ret_lsn = -1;
  log_sequence_number_ = ret_lsn;
  buffer_lock_.unlock();
  return ret_lsn;
}

int64_t LogWorker::Rename(const Slice& branch_name, const Slice& new_branch_name) {
  LogRecord record;
  record.key_length = (int64_t) branch_name.len();
  record.value_length = (int16_t) new_branch_name.len();
  record.key = branch_name.data();
  record.value = new_branch_name.data();
  record.logcmd = (int16_t)LogCommand::kRename;
  buffer_lock_.lock();
  int64_t ret_lsn = log_sequence_number_ + 1;
  record.log_sequence_number = ret_lsn;
  record.ComputeChecksum();
		std::string log_data = record.ToString();
  int64_t log_data_length = record.GetLength();
  bool write_ret = WriteLog(log_data.c_str(), log_data_length);
  if(write_ret == false) {
   ret_lsn = -1;
  }
  log_sequence_number_ = ret_lsn;
  buffer_lock_.unlock();
  return ret_lsn;
}

int64_t LogWorker::Remove(const Slice& branch_name) {
  LogRecord record;
  record.key_length = (int64_t) branch_name.len();
  record.value_length = 0;
  record.key = branch_name.data();
  record.value = nullptr;
  record.logcmd = (int16_t)LogCommand::kRemove;
  buffer_lock_.lock();
  int64_t ret_lsn = log_sequence_number_ + 1;
  record.ComputeChecksum();
		std::string log_data = record.ToString();
  int64_t log_data_length = record.GetLength();
  bool write_ret = WriteLog(log_data.c_str(), log_data_length);
  if(write_ret == false) {
   ret_lsn = -1;
  }
		log_sequence_number_ = ret_lsn;
  buffer_lock_.unlock();
  return ret_lsn;
}

bool LogWorker::ReadOneLogRecord(LogRecord& record) {
		int64_t data_length = 0;
		int read_ret = 0;
		read_ret = read(fd_, &data_length, sizeof(int64_t));
		if(read_ret == -1) {
			LOG(FATAL) << "Read log length fails";
			return false;	
		}
		char log_data[200]; 
		read_ret = read(fd_, log_data, (size_t)data_length - sizeof(int64_t));
		if(read_ret == -1) {
			LOG(FATAL) << "Read log data fails";
			return false;
		}
		record.data_length = data_length;
		size_t pos = 0;
		memcpy(&record.checksum, log_data + pos, sizeof(int64_t));
		pos = pos + sizeof(int64_t);
		memcpy(&record.version, log_data + pos, sizeof(int16_t));
		pos = pos + sizeof(int16_t);
		memcpy(&record.logcmd, log_data + pos, sizeof(int16_t));
		pos = pos + sizeof(int16_t);
		memcpy(&record.log_sequence_number, log_data + pos, sizeof(int64_t));
		pos = pos + sizeof(int64_t);
		memcpy(&record.key_length, log_data + pos, sizeof(int64_t));
		pos = pos + sizeof(int64_t);
		memcpy(&record.value_length, log_data + pos, sizeof(int16_t));
		pos = pos + sizeof(int16_t);
		byte_t* key = new byte_t[record.key_length]; //handled by delete
		byte_t* value = new byte_t[record.value_length];
		if(record.key_length != 0) {
			//std::string key(log_data+pos, record.key_length);
			memcpy(key, log_data + pos, record.key_length);
			pos = pos + (size_t) record.key_length;
			//record.key = reinterpret_cast<const byte_t*>(key.c_str());
			record.key = key;
		}
		if(record.value_length != 0) {
			//std::string value(log_data+pos, record.value_length);
			memcpy(value, log_data + pos, record.value_length);
			pos = pos + (size_t) record.value_length;
			//record.value = reinterpret_cast<const byte_t*>(value.c_str());
			record.value = value;
		}
		return true;
}


}  // end of namespace recovery
}  // end of namespace ustore
