#ifndef USTORE_LOGGING_RECOVERY_LOG_CURSOR_H_
#define USTORE_LOGGING_RECOVERY_LOG_CURSOR_H_

#include "ustore_log_entry.h"

namespace ustore {

    namespace recovery {
        
        struct UStoreLogCursor {
            uint64_t     file_id_;
            uint64_t     log_id_;
            uint64_t     offset_;
    
            UStoreLogCursor();
            ~UStoreLogCursor();
            
            /*
             * make sure all the fields are >= 0
             * */
            bool        isValid() const;
            void        reset();

            /*
             * put the cursor content to buffer
             * */
            int         serialize(char* buf, uint64_t length, uint64_t& pos) const;
            /*
             * deserialize the cursor from the buffer 
             * */
            int          deserialize(const char* buf, uint64_t length, uint64_t& pos) const;
            char*        toString() const;
            uint64_t     toString(char* buf, const uint64_t length) const;
            
            /*
             * @brief Read Log entry from log buffer according to the cursor
             * */
            int         getEntry(UStoreLogEntry& entry, const LogCommand cmd, const char* log_data, const uint64_t data_length) const; 

            /*
             * @brief forward the curpos forward
             * */
            int         advance(LogCommand cmd, uint64_t seq_id, const uint64_t data_length);
            int         advance(const UStoreLogEntry& entry);
            
           //@deprecated  
           // bool        younger(const UStoreLogCursor& that) const;
           // bool        equal(const UStoreLogCursor& that)  const;

            /*
             * compare the age of the cursor
             * */
            bool        operator<(const UStoreLogCursor& that) const;
            bool        operator>(const UStoreLogCursor& that) const;
            bool        operator==(const UStoreLogCursor& that) const;

        };//end of UStoreLogCursor

        UStoreLogCursor&    setLogCursor(UStoreLogCursor& cursor, const uint64_t file_id, const uint64_t log_id, const uint64_t offset);
        
        /*
         * TODO: add atomic Log cursor
         * */

    }//end of namespace logging_recovery

}//end of namespace ustore


#endif //end of USTORE_LOGGING_RECOVERY_LOG_CURSOR_H_
