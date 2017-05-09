// Copyright (c) 2017 The Ustore Authors

#ifndef USTORE_RECOVERY_LOG_RECORD_H_
#define USTORE_RECOVERY_LOG_RECORD_H_

#include<string>

namespace ustore {
namespace recovery {

enum LogCommand {
		LOG_UPDATE = 111,  // Update(branch_name, version)
		LOG_RENAME = 112,  // Rename(branch_name, new_branch_name)
		LOG_REMOVE = 113		// Remove(branch_name)
};
	/*
		* the structure of one log record is:
		* checksum||version||lsn||key_len||value_len||key||value
		* */
class LogRecord {
	public:
		LogRecord();
		~LogRecord() {};
		char* ToString(); // generate all the content to a string
		int64_t GetLength();  //return the string length from ToString()
		int64_t ComputeChecksum();  // Compute the checksum according to the content
	public:
		int64_t checksum; 	// compute the checksum after the other fields are filled
		int16_t version;  // by default
		int16_t logcmd; 		// what kind of operation
		int64_t log_sequence_number;
		int64_t key_length;
		int16_t value_length;
		char* key;
		char* value;
		int64_t data_length;
};  // end of class LogRecord

}  // end of namespace recovery
}  // end of namespace ustore
#endif  // USTORE_RECOVERY_LOG_RECORD_H_
