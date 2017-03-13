#ifndef USTORE_LOGGING_RECOVERY_LOG_CURSOR_H_
#define USTORE_LOGGING_RECOVERY_LOG_CURSOR_H_

#include "ustore_log_entry.h"

namespace ustore {

    namespace logging_recovery {
        
        struct UStoreLogCursor {
            int64_t     file_id_;
            int64_t     log_id_;
            int64_t     offset_;
    
            UStoreLogCursor();
            ~UStoreLogCursor();
            
            /*
             * make sure all the fields are >= 0
             * */
            bool        is_valid() const;
            int         init();
            void        reset();

            /*
             * put the cursor content to buffer
             * */
            int         serialize(char* buf, int64_t length, int64_t& pos) const;
            /*
             * deserialize the cursor from the buffer 
             * */
            int         deserialize(const char* buf, int64_t length, int64_t& pos) const;
            char*       c_str() const;
            int64_t     to_string(char* buf, const int64_t length) const;
            
            /*
             * @brief Read Log entry from log buffer according to the cursor
             * */
            int         get_entry(UStoreLogEntry& entry, const LogCommand cmd, const char* log_data, const int64_t data_length) const; 

            /*
             * @brief forward the curpos forward
             * */
            int         advance(LogCommand cmd, int64_t seq_id, const int64_t data_length);
            int         advance(const UStoreLogEntry& entry);

            bool        younger(const UStoreLogCursor& that) const;
            bool        equal(const UStoreLogCursor& that)  const;

        };//end of UStoreLogCursor

        UStoreLogCursor&    set_log_cursor(UStoreLogCursor& cursor, const int64_t file_id, const int64_t log_id, const int64_t offset);
        
        /*
         * TODO: add atomic Log cursor
         * */

    }//end of namespace logging_recovery

}//end of namespace ustore


#endif //end of USTORE_LOGGING_RECOVERY_LOG_CURSOR_H_
