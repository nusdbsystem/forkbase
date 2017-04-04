// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_STORE_LST_STORE_H_
#define USTORE_STORE_LST_STORE_H_ 

#include <cstdint>
#include <cstring>

#include <limits>
#include <array>
#include <functional>
#include <unordered_map>

#include "chunk/chunk.h"
#include "chunk_store.h"
#include "hash/hash.h"
#include "types/type.h"
#include "utils/singleton.h"
#include "utils/noncopyable.h"
#include "utils/chars.h"

namespace ustore {
namespace lst_store {
    struct LSTHash {
        const byte_t *hash_;
        LSTHash(const byte_t *ptr) noexcept : hash_(ptr) {}
        bool operator==(const LSTHash& rhs) const noexcept {
            return std::memcmp(hash_, rhs.hash_, ::ustore::Hash::kByteLength) == 0;
        }
        const Hash* toHash() {
            return new Hash(const_cast<byte_t*>(hash_));
        }
    };
}
}

namespace std{
    template<> struct hash<::ustore::lst_store::LSTHash> {
    size_t operator()(const ::ustore::lst_store::LSTHash& key) const noexcept {
            size_t ret;
            std::copy(key.hash_, key.hash_ + sizeof(ret), reinterpret_cast<::ustore::byte_t*>(&ret));
            return ret;
        }
    };
} /* namespace std*/

namespace ustore {
namespace lst_store {

    const size_t META_LOG_SIZE = 4096;
    const size_t SEGMENT_SIZE = (1<<22);
    const size_t NUM_SEGMENTS = 16;
    const size_t LOG_FILE_SIZE = SEGMENT_SIZE * NUM_SEGMENTS+ META_LOG_SIZE;
    const size_t MAX_PENDING_SYNC_CHUNKS = 1024;
    const uint64_t MAX_SYNC_TIMEOUT_MILLSECONDS = 3000;

    struct LSTChunk{
        const byte_t *chunk_;
        LSTChunk(const byte_t *ptr) noexcept: chunk_(ptr) {}
        const Chunk* toChunk() {
            return new Chunk(chunk_);
        }
    };

    /**
     * @brief layout: 8-byte prev offset | 8-byte next offset | [20-byte hash,
     * chunks] | 20-byte hash which is the same as the hash of the first chunk
     * and acts as the crc code
     */
    struct LSTSegment {
        static void* base_addr_;
        LSTSegment *prev_, *next_;
        size_t nchunks_;
        void* segment_;

        inline LSTSegment(void* segment) noexcept : segment_(segment) {}
    };

    class LSTStore : private Noncopyable, public Singleton<LSTStore>, public ChunkStore {

        friend class Singleton<LSTStore>;

        private:
        inline LSTStore(const char* dir = nullptr,
                const char* log_file = "ustore.log"){
            mmapUstoreLogFile(dir, log_file);
        }

        ~LSTStore();

        inline size_t getFreeSpaceMajor() {
            return SEGMENT_SIZE - ::ustore::Hash::kByteLength - major_segment_offset_;
        }

        inline size_t getFreeSpaceMinor() {
            return SEGMENT_SIZE - ::ustore::Hash::kByteLength - minor_segment_offset_;
        }
        void gc() {}
        void load(void*);
        size_t loadFromValidSegment(LSTSegment*);
        size_t loadFromLastSegment(LSTSegment*);
        void* mmapUstoreLogFile(const char* dir, const char* log_file);
        LSTSegment* allocate(LSTSegment*);
        LSTSegment* allocateMajor();
        LSTSegment* allocateMinor();

        std::unordered_map<LSTHash, LSTChunk> chunk_map_;
        LSTSegment *free_list_, *major_list_, *minor_list_; 
        LSTSegment *current_major_segment_;
        LSTSegment *current_minor_segment_;
        size_t major_segment_offset_;
        size_t minor_segment_offset_;

        public:
        void sync();
        virtual const Chunk* Get(const Hash& key);
        virtual bool Put(const Hash& key, const Chunk& chunk);
    
    };
} /* namespace lst_store*/
} /* namespace ustore */
#endif

