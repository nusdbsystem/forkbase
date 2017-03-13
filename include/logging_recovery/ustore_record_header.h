#ifndef USTORE_LOGGING_RECOVERY_RECORD_HEADER_H_
#define USTORE_LOGGING_RECOVERY_RECORD_HEADER_H_

namespace ustore {

    namespace logging_recovery {
    
        /*
         * TODO: should replace the magic number
         * */
        static const int16_t MAGIC_NUMBER = static_cast<int16_t>(0xFFFF);

        /*
         * @brief Introduction to UStoreRecordHeader
         * */
        struct UStoreRecordHeader {
            int16_t     magic_;               
            int16_t     header_length_;
            int16_t     version_;
            int16_t     header_checksum_;
            int32_t     data_length_;
            int32_t     data_length_compressed_;
            int64_t     data_checksum_;

            /*
             * @brief Constructor of UStoreRecordHeader
             * */
            UStoreRecordHeader();

            int64_t     to_string(char* destbuf, const int64_t destbuf_len) const;

            /*
             * Set the magic number
             * */
            inline void set_magic_number(const int16_t magic = MAGIC_NUMBER) {
                magic_ = magic;
            }

            /*
             * set checksum of the header
             * */
            void set_header_checksum();

            /*
             * check the checksum of the header
             * */
            int check_header_checksum() const;

            /*
             * check the magic number of the header
             * */
            int check_magic_number(const int16_t magic = MAGIC_NUMBER) const;

            /*
             * check the compressed data length is correct or not
             * */
            int check_data_length_compressed(const int32_t compressed_length) const;

            /*
             * check the data length is correct or not
             * */
            int check_data_length(const int32_t data_length) const;

            /*
             * check whether the header is compressed or not
             * */
            bool is_compressed() const;

            /*
             * check the checksum of the header
             * */
            int check_header_checksum(const char* buf, const int64_t length) const;

            /*
             * check record after being read
             * @param string buffer to store the bytes string
             * @param total length of record and record header
             * @param magic number
             * */
            static int check_record(const char* buf, const int64_t length, const int16_t magic);


            /*
             * check the record after reading the header
             * @param record header
             * @param string buffer after the record header
             * @param length the of buffer
             * @param magic number that is used to check the correctness
             * */
            static int check_record(const UStoreRecordHeader& record_header,
                                    const char*     payload_buf,
                                    const int64_t   payload_length,
                                    const int16_t   magic);

            /*
             * check the record buffer and do the extraction
             * @param raw data buffer address
             * @param size of the raw data buffer
             * @param magic number that is used to do the checking
             * @param [out] record header
             * @param [out] content address
             * @param [out] content size
             * */
            static int check_record(const char*     rawdata,
                                    const int64_t   rawdata_size,
                                    const int16_t   magic,
                                    UStoreRecordHeader& header,
                                    const char*&    payload_ptr,
                                    int64_t&        payload_size);

            /*
             * do not check and directly extract header and payload address
             * @param raw data buffer address
             * @param size of the raw data buffer
             * @param [out] record header
             * @param [out] content address
             * @param [out] content size
             * */
            static int get_record_header(const char*    rawdata,
                                        const int64_t rawdata_size,
                                        UStoreRecordHeader& header,
                                        const char*&    payload_ptr,
                                        int64_t&        payload_size);
        }; //end of UStoreRecordHeader

        static const int USTORE_RECORD_HEADER_LENGTH = sizeof(UStoreRecordHeader);

    }//end of namespace logging_recovery

}//end of namepsace ustore

#endif //end USTORE_LOGGING_RECOVERY_RECORD_HEADER_H_
