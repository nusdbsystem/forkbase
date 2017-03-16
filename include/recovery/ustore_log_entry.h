#ifndef USTORE_LOGGING_RECOVERY_LOG_ENTRY_H
#define USTORE_LOGGING_RECOVERY_LOG_ENTRY_H

#include "ustore_record_header.h"

namespace ustore {

    namespace recovery {
    
      enum LogCommand : uint32_t  
      {
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
            UStoreLogEntry()
            {
                memset(this, 0x00, sizeof(UStoreLogEntry));
            }

            char*   toString() const;
            int64_t toString(char* destbuf, const uint64_t destbuf_len) const;

            /* 
             * @brief Set log sequence id
             * */
            void setLogSeqId(const uint64_t seq_id);

            /*
             * @brief Set log cmd id
             * */
            void setLogCmdId(const uint32_t  cmd_id);

            /*
             * @brief Set record header in the Log Entry
             * @param log buffer address
             * @param content length
             * */
            int fillRecordHeader(const char* log_data, const uint64_t data_len);


            /*
             * @brief Calculating the checksum of the log entry: sequence_id + cmd_id + log content
             * @param log buffer address
             * @param content length
             * */
            int64_t computeChecksum(const char* log_data, const uint64_t data_len) const;

            /*
             * @brief Return the length of the log data 
             * */
            uint32_t getLogdataLength() const;

            /*
             * @brief Check the structure correctnesss of the record header
             * */
            int checkRecordHeader() const;

            /*
             * @brief Check the correctness of the log data
             * */
            int checkLogdata() const;

            /*
             * @brief Return the record header size
             * */
            static uint64_t getRecordHeaderSize();

            
            
            UStoreRecordHeader     header_;
            uint64_t               seq_id_;
            uint32_t               cmd_id_;

            static const uint16_t LOG_VERSION = 1;

       }; //end of UStoreLogEntry definition


    };//end of logging_recovery namespace

};//end of ustore namespace

#endif //end UStore_Logging_Recovery_Log_Entry
