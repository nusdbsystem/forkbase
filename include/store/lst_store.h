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
            return std::memcmp(hash_, rhs.hash_, ::ustore::Hash::HASH_BYTE_LEN) == 0;
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
            std::copy(key.hash_, key.hash_ + sizeof(ret), &ret);
            return ret;
        }
    };
} /* namespace std*/

namespace ustore {
namespace lst_store {

    const size_t SEGMENT_SIZE = (1<<22);
    const size_t LOG_FILE_SIZE = ((size_t)1 << 34);

    struct LSTChunk{
        const byte_t *chunk_;
        LSTChunk(const byte_t *ptr) noexcept: chunk_(ptr) {}
        const Chunk* toChunk() {
            return new Chunk(chunk_);
        }
    };

    struct LSTSegment {
        static void* base_addr_;
        LSTSegment* prev_, *next_;
        size_t nchunks_;
        size_t offset_;

        LSTSegment(void* addr) noexcept : nchunks_(0), offset_(0) {
            size_t v1, v2;
            readInteger(reinterpret_cast<char*>(addr), v1, v2);
            prev_ = reinterpret_cast<LSTSegment*>((uintptr_t)base_addr_ + v1);
            next_ = reinterpret_cast<LSTSegment*>((uintptr_t)base_addr_ + v2);
        }            
    };

    class LSTStore : private Noncopyable, public Singleton<LSTStore>, ChunkStore {

        friend class Singleton<LSTStore>;

        private:
        LSTStore(){}
        void gc() {}
        void* load(const char* log_file = "ustore.log", const char* dir = nullptr);

        std::unordered_map<LSTHash, LSTChunk> chunk_map_;
        void* address_;
        LSTSegment *free_list_head_, *free_list_tail_;
        LSTSegment *major_list_head_, *major_list_tail_;
        LSTSegment *minor_list_head_, *minor_list_tail_;


        public:

        void* init(const char* log_file = "ustore.log", const char* dir = nullptr);

        virtual const Chunk* Get(const Hash& key);

        virtual bool Put(const Hash& key, const Chunk& chunk);
    
    };
} /* namespace lst_store*/
} /* namespace ustore */
#endif

