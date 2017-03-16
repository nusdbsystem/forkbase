#ifndef USTORE_RECOVERY_LOG_WRITER_H_
#define USTORE_RECOVERY_LOG_WRITER_H_

/*
 * TODO: need to define a FIleAppender
 * */

#include "ustore_log_cursor.h"

namespace ustore {

    namespace recovery {
        
        /*
         * UStore Log Writer class, can be inherited by others
         * */
        class UStoreLogWriter {
            public:
                UStoreLogWriter();
                virtual ~UStoreLogWriter();
                
                int     init(const char* log_dir, int64_t align_mask, int64_t log_sync_type);
                int     reset();

                /*
                 * check the log cursor and and do all the checking works before writing the logs out
                 * */
                int     startLog(const UStoreLogCursor& log_cursor);

                /*
                 * Before write the log out, it needs to check the states, do the parsing, etc.
                 * */
                int     writeLog(const char* log_data, uint64_t data_length);

                /*
                 * @params [out] log_cursor
                 * */
                int     getLogCursor(UStoreLogCursor& log_cursor) const;
                int     flushLogToDisk();
           
            protected:
                int     checkState() const;
                int     checkInit() const;

            protected:
                char*               log_dir_;
                int64_t             align_mask_;
                int64_t             log_sync_type_;
                UStoreLogCursor     log_cursor_;
                UStoreFileAppender  file_;
        };//end of class UStoreLogWriter

        /*
         * @brief open UStoreFileAppender for a UStoreWriter class
         * @params [out] file
         * @params [in]  log_dir
         * @params [in]  log_file_id
         * @params [in]  is truncation or not
         * */
        static int openLogFile(UStoreFileAppender&      file, 
                                const char*             log_dir, 
                                const uint64_t          log_file_id, 
                                const bool              is_trunc);
        
        /*
         * if the log file is out of space, then a new file are needed
         * @params [out] file
         * @params [in]  log_dir
         * @params [in]  old_log_file_id
         * @params [in]  new_log_file_id
         * @params [in]  is truncation or not
         * */
        static int openNewLogFile(UStoreFileAppender&   file, 
                                    const char*         log_dir, 
                                    const uint64_t      old_log_file_id, 
                                    const uint64_t      new_log_file_id,
                                    const bool          is_trunc);

        /*
         * parse the log data in the buffer
         * @params [in] log_data
         * @params [in] data_length
         * @params [in] start log cursor
         * @params [out] end log cursor
         * @params [in] check integrity or not
         * */
        static int parseLogBuffer(const char*               log_data, 
                                    uint64_t                data_length, 
                                    const UStoreLogCursor&  start_cursor, 
                                    UStoreLogCursor&        end_cursor,
                                    bool                    check_data_integrity = false);


    }//end of namespace recovery

}//end of namespace ustore


#endif //end of USTORE_RECOVERY_LOG_WRITER_H_
