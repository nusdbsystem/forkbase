#ifndef USTORE_LOG_READER_SINGLE_H_
#define USTORE_LOG_READER_SINGLE_H_


namespace ustore {

    namespace recovery {
        
        /*
         * TODO: need to define UStoreFileReader and UStoreDataBuffer classes.
         * */


        //TODO: define a const log size
        static const uint64_t DEFAULT_LOG_SIZE  =   2 * 1024 * 1024; //2M

        class UStoreSingleLogReader {
            public:
                static const uint64_t LOG_BUFFER_MAX_LENGTH;
            public:
                /*
                 * constructors and destructor
                 * */
                UStoreSingleLogReader();
                //UStoreSingleLogReader(const char* log_dir);
                virtual ~UStoreSingleLogReader();

                /*
                 * @brief   initialize UStoreSingleLogReader and it is designed for 
                 *          malloc function rather than new function
                 *          init() function must be invoked before open() and readLog() are used
                 *          the default buffer size is LOG_BUFFER_MAX_LENGTH
                 * @param   [in]    log_dir
                 * @return  USTORE_LOG_SUCCESS, USTORE_LOG_INIT_TWICE, USTORE_LOG_ERROR
                 * */
                int init(const char* log_dir);

                /*
                 * @brief   open a log file and close() function should be invoked to close the file
                 *          this open() function can be invoked more than one time and the data buffer
                 *          is the same one
                 * @param   [in]    file_id the log file id
                 * @parm    [in]    last_log_seq_id, last log sequence id, with which system can judge
                 *                  whether is consecutive or not. The default value is 0, which indicates
                 *                  system ignore the checking.
                 * @return  USTORE_LOG_SUCCESS, USTORE_LOG_ERROR
                 * */
                int open(const uint64_t file_id, const uint64_t last_log_seq_id = 0);

                /*
                 * @brief   close the log file. All the structures can be reused by invoke init() function again
                 * */
                int close();

                /*
                 * @brief   reset all the states and release buffer
                 * */
                int reset();

                /*
                 * @brief   read a log entry from the log file.
                 * @param   [out]   cmd:        log command that is read from log file
                 * @param   [out]   log_seq:    log sequence number
                 * @param   [out]   log_data:   log content
                 * @param   [out]   log_length: the length of the log buffer
                 * @return  USTORE_LOG_SUCCESS, USTORE_LOG_NULL, USTORE_LOG_ERROR
                 * */
                virtual int readSingleLogEntry(LogCommand& cmd, uint64_t &log_seq, char*& log_data, uint64_t& data_length) = 0;

                /*
                 * @brief   get the current file id
                 * */
                inline uint64_t getCurLogFileId()
                {
                    return file_id_;
                }

                /*
                 * @brief   get the last sequence id
                 * */
                inline uint64_t getLastLogSeqId()
                {
                    return last_log_seq_id_;
                }

                /*
                 * @brief   get the offset of the log data
                 * */
                inline uint64_t getLastLogOffset()
                {
                    return log_offset_;
                }
                

                /*
                 * @brief   get the maximum log id under the log directory
                 * */
                int        getMaxLogFileId(uint64_t& max_log_file_id);

                /*
                 * @brief   get the minimum log id under the log directory
                 * */
                int        getMinLogFileId(uint64_t& min_log_file_id);

            protected:
                /*
                 * @brief  read a log entry from the log data
                 * */
                int         readHeader(UStoreLogEntry& entry);
                /*
                 * @brief  to make sure the alignment, zeros may appear in the end of structure
                 *         this function should be invoked to remove those padding zeros.
                 * */
                int         trimLastZeroPadding(uint64_t header_size);

            protected:
                UStoreFileReader    file_;
                uint64_t            file_id_;           
                uint64_t            last_log_seq_id_;
                UStoreDataBuffer    log_buffer_;
                char                file_name_[USTORE_MAX_FILE_NAME_LENGTH];  //TODO: constant--> USTORE_MAX_FILE_NAME_LENGTH
                char                log_dir_[USTORE_MAX_FILE_NAME_LENGTH];
                uint64_t            log_offset_;
                bool                is_init_;                                 //indicates whether it is initialzied or not

        };//end of UStoreSinleLogReader class

    }//end of namespace recovery

}//end of namespace ustore






#endif //end of USTORE_LOG_READER_SINGLE_H_
