#ifndef USTORE_LOGGING_RECOVERY_LOG_ENTRY_H
#define USTORE_LOGGING_RECOVERY_LOG_ENTRY_H

#include "ustore_record_header.h"

namespace ustore {

    namespace logging_recovery {
    
      enum LogCommand {
        USTORE_LOG_CHECKPOINT = 101;        //write checkpoints
        /*
         *@TODO Should add log command when all the logics are done!!!
         * */
      };


     /*
      * @brief Introduction to UStoreLogEntry
      * Log Entry for UStore
      * one Log record consists of four parts:
      *         UStoreRecordHeader + Sequence ID + Log Command ID + Log content
      * */
           
       struct UStoreLogEntry {
            UStoreRecordHeader  header_;
            uint64_t            seq_id_;
            int32_t             cmd_id_;

            static const int16_t LOG_VERSION = 1;

            UStoreLogEntry()
            {
                memset(this, 0x00, sizeof(UStoreLogEntry));
            }

            int64_t to_string(char* destbuf, const int64_t destbuf_len) const;

            /* 
             * @brief Set log sequence id
             * */
            void set_log_seq_id(const uint64_t seq_id);

            /*
             * @brief Set log cmd id
             * */
            void set_log_cmd_id(const int32_t  cmd_id);

            /*
             * @brief Set record header in the Log Entry
             * @param log buffer address
             * @param content length
             * */
            int fill_record_header(const char* log_data, const int64_t data_len);


            /*
             * @brief Calculating the checksum of the log entry: sequence_id + cmd_id + log content
             * @param log buffer address
             * @param content length
             * */
            int64_t compute_checksum(const char* log_data, const int64_t data_len) const;

            /*
             * @brief Return the length of the log data 
             * */
            int32_t get_logdata_length() const;

            /*
             * @brief Check the structure correctnesss of the record header
             * */
            int check_record_header() const;

            /*
             * @brief Check the correctness of the log data
             * */
            int check_logdata() const;

            /*
             * @brief Return the record header size
             * */
            static int get_record_header_size();

       }; //end of UStoreLogEntry definition


    };//end of logging_recovery namespace

};//end of ustore namespace

#endif //end UStore_Logging_Recovery_Log_Entry
