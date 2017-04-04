// Copyright (c) 2017 The Ustore Authors.

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include <chrono>
#include <type_traits>

#include <boost/filesystem.hpp>

#include "utils/chars.h"
#include "utils/logging.h"
#include "store/lst_store.h"

namespace ustore {
namespace lst_store {

void* LSTSegment::base_addr_ = 0;

namespace fs = boost::filesystem;

void destroySegmentList(LSTSegment* head) {
    while (head != nullptr) {
        auto next = head->next_;
        delete head;
        head = next;
    }
}

/**
 * @brief persist @param length bytes of @param buf to @param fd
 *
 * @param fd
 * @param buf
 * @param length
 */
static void fdWriteThenSync(int fd, const void* buf, size_t length) {
    ssize_t bytes = 0;
    while (bytes < length) {
        int nb = ::write(fd, reinterpret_cast<const char*>(buf) + bytes, length - bytes); 
        CHECK_GE(nb, 0);
        bytes += nb;
    }

    int ret = fsync(fd); // sync to disk
    CHECK_NE(ret, -1);
}

static void syncToDisk(void* addr, size_t len) {
    int ret = msync(addr, len, MS_SYNC);
    if (ret == -1)
        LOG(FATAL) << "MSYNC ERROR: " << std::strerror(errno);
}

constexpr static inline bool isChunkValid(ChunkType type) noexcept {
    return type == ChunkType::kNull
                || type == ChunkType::kCell 
                || type == ChunkType::kMeta
                || type == ChunkType::kBlob
                || type == ChunkType::kString
                || type == ChunkType::kInvalid;
}

constexpr static inline bool isEndChunk(ChunkType type) noexcept {
    return type == ChunkType::kNull;
}

constexpr static inline LSTSegment* offsetToLstSegmentPtr(size_t offset) noexcept {
    return (offset == 0) ? nullptr : 
        new LSTSegment(reinterpret_cast<char*>(LSTSegment::base_addr_) + offset);
}

constexpr static inline size_t segmentPtrToOffset(const LSTSegment* ptr) noexcept {
    return ptr == nullptr ? 0 : ((uintptr_t)ptr->segment_ - (uintptr_t)LSTSegment::base_addr_);
}

constexpr static inline byte_t* getValidateHashPtr(const LSTSegment* segment) noexcept {
    return reinterpret_cast<byte_t*>(segment->segment_) + SEGMENT_SIZE 
        - ::ustore::Hash::kByteLength;
}

constexpr static inline byte_t* getFirstHashPtr(const LSTSegment* segment) noexcept {
    return reinterpret_cast<byte_t*>(const_cast<void*>(segment->segment_)) + 2 * sizeof(size_t);
}

static void linkSegmentList (LSTSegment* begin) noexcept {
    LSTSegment *iter = begin;
    LSTSegment *last = nullptr;
    while (iter != nullptr) {
        iter->prev_ = last;
        size_t v[2];
        readInteger(reinterpret_cast<char*>(iter->segment_), v);
        LSTSegment *next = offsetToLstSegmentPtr(v[1]);
        iter->next_ = next;
        last = iter;
        iter = next;
    }
}

/**
 * @brief mmap the log file; if the give @param file does not exist, create a new file
 * with the given name in the given @param dir.
 *
 * @param dir
 * @param file
 *
 * @return the address of the memory region where the log file is mapped into 
 */
void* LSTStore::mmapUstoreLogFile(const char* dir, const char* file) {
    fs::path path;
    if (dir == nullptr)
        path = fs::current_path();
    else
        path = fs::path(dir);

    path /= std::string(file);

    int fd;
    if (fs::exists(path) && fs::file_size(path) == LOG_FILE_SIZE) {
        fd = ::open(path.c_str(), O_RDWR, S_IRWXU);
    } else {
        // init the log
        fd = ::open(path.c_str(), O_RDWR | O_CREAT, S_IRWXU);
        size_t* buf1 = new size_t[META_LOG_SIZE/sizeof(size_t)];
        size_t* buf2 = new size_t[SEGMENT_SIZE/sizeof(size_t)];
        char* meta = reinterpret_cast<char*>(buf1);
        char* segment = reinterpret_cast<char*>(buf2);
        std::memset(meta, 0, META_LOG_SIZE);
        std::memset(segment, 0, SEGMENT_SIZE);

        size_t first_segment_offset = META_LOG_SIZE;
        // only a free list
        appendInteger(meta, first_segment_offset); 
        LOG(INFO) << "init the meta segment";
        fdWriteThenSync(fd, meta, META_LOG_SIZE);

        for (int i = 0; i < NUM_SEGMENTS; ++i)
        {
            LOG(INFO) << "init the " << i << "-th segment";
           size_t prev_segment_offset = 0, next_segment_offset = 0; 
           if (i > 0)
               prev_segment_offset = (i - 1) * SEGMENT_SIZE + META_LOG_SIZE;
           if (i < NUM_SEGMENTS - 1)
               next_segment_offset = (i + 1) * SEGMENT_SIZE + META_LOG_SIZE;
           appendInteger(segment, prev_segment_offset, next_segment_offset);
           fdWriteThenSync(fd, segment, SEGMENT_SIZE);
        }
    }

    CHECK_GE(fd, 0);

    void* address = ::mmap(nullptr 
            , LOG_FILE_SIZE  
            , PROT_READ | PROT_WRITE
            , MAP_SHARED 
            , fd
            , 0
            );

    if (address == (void*)-1) {
        LOG(FATAL) << "MMAP ERROR: " << std::strerror(errno);
    }

    // lock the mmap'ed memory to guarantee in-memory access
    ::mlock(address, LOG_FILE_SIZE);

    LSTSegment::base_addr_ = address;

    this->load(address);

    return address;
}

LSTSegment* LSTStore::allocateMajor() {
    current_major_segment_ = allocate(current_major_segment_);
    if (major_list_ == nullptr)
        major_list_ = current_major_segment_;
    // persist the log meta block
    appendInteger(static_cast<char*>(LSTSegment::base_addr_)
            , segmentPtrToOffset(free_list_)
            , segmentPtrToOffset(major_list_)
            , segmentPtrToOffset(current_major_segment_)
            , segmentPtrToOffset(minor_list_)
            , segmentPtrToOffset(current_minor_segment_));
    syncToDisk(LSTSegment::base_addr_, META_LOG_SIZE);
    major_segment_offset_ = 2 * sizeof(size_t);
    return current_major_segment_;
}

LSTSegment* LSTStore::allocateMinor() {
    current_minor_segment_ = allocate(current_minor_segment_);
    if (minor_list_ == nullptr)
        minor_list_ = current_minor_segment_;
    // persist the log meta block
    appendInteger(static_cast<char*>(LSTSegment::base_addr_)
            , segmentPtrToOffset(free_list_)
            , segmentPtrToOffset(major_list_)
            , segmentPtrToOffset(current_major_segment_)
            , segmentPtrToOffset(minor_list_)
            , segmentPtrToOffset(current_minor_segment_));
    syncToDisk(LSTSegment::base_addr_, META_LOG_SIZE);
    minor_segment_offset_ = 2 * sizeof(size_t);
    return current_minor_segment_;
}

LSTSegment* LSTStore::allocate(LSTSegment* current) {
    if (free_list_ == nullptr) 
        LOG(FATAL) << "Chunk store: out of log memory";
    LSTSegment* next = free_list_;

    if (current != nullptr) {
        // write the hash of the first chunk into the last 20 bytes of the
        // current segment
        byte_t* first_hash = getFirstHashPtr(current);
        std::copy(first_hash, first_hash + ::ustore::Hash::kByteLength, 
                getValidateHashPtr(current));
        appendInteger(reinterpret_cast<char*>(current->segment_) + sizeof(size_t), 
                segmentPtrToOffset(next));
        syncToDisk(current->segment_, SEGMENT_SIZE);
    }

    // persist the meta data of the new allocated segment
    size_t prev_offset = segmentPtrToOffset(current), next_offset = 0;
    appendInteger(reinterpret_cast<char*>(next->segment_), prev_offset, next_offset);
    syncToDisk(next->segment_, META_LOG_SIZE);

    // update the segment list; for free list, one-direct link list suffices
    free_list_ = next->next_;
    next->prev_ = current;
    next->next_ = nullptr;
    current = next;

    return current;
}

size_t LSTStore::loadFromLastSegment(LSTSegment* segment) {
    // first check if it is the last segment, if not, simply make it the last
    size_t next_segment_offset;
    readInteger(reinterpret_cast<char*>(segment->segment_) + sizeof(size_t), 
            next_segment_offset);

    bool need_sync = false;
    if (next_segment_offset != 0) {
        appendInteger(reinterpret_cast<char*>(segment->segment_) + sizeof(size_t), (size_t)0);
        CHECK_EQ((uintptr_t)free_list_->segment_, 
                next_segment_offset + (uintptr_t)LSTSegment::base_addr_);
        need_sync = true;
    }

    byte_t* offset = reinterpret_cast<byte_t*>(segment->segment_) + 2 * sizeof(size_t);
    while (true) {
        Chunk chunk(offset + ::ustore::Hash::kByteLength);
        if (!isChunkValid(chunk.type()) || Hash(offset) != chunk.hash()) {
            // check failed; exit
            std::memset(offset, SEGMENT_SIZE + (uintptr_t)(segment->segment_) - (uintptr_t)offset, 0);
            need_sync = true;
            break;
        }

        if (isEndChunk(chunk.type()))
            break;

        if (chunk.type() == ChunkType::kInvalid)
            this->chunk_map_.erase(offset);
        else {
            chunk_map_.emplace(offset, chunk.head());
        }

        offset += chunk.numBytes() + ::ustore::Hash::kByteLength; 
    }

    if (need_sync)
        syncToDisk(segment->segment_, SEGMENT_SIZE);
    return reinterpret_cast<size_t>(uintptr_t(offset) - uintptr_t(segment->segment_));
}

size_t LSTStore::loadFromValidSegment(LSTSegment* segment) {
    // TODO: take alignment into consideration
    CHECK_NE(segment, nullptr);
    byte_t* offset = getFirstHashPtr(segment);
    byte_t* end_offset = getValidateHashPtr(segment) -
                ::ustore::Hash::kByteLength - ::ustore::Chunk::kMetaLength; 

    CHECK(Hash(offset) == Hash(getValidateHashPtr(segment)));
    while (true) {
        byte_t *hash_offset = offset;
        byte_t *chunk_offset = hash_offset + ustore::Hash::kByteLength;
        Chunk chunk(chunk_offset);

        // check whether all chunks have been loaded or not
        if (isEndChunk(chunk.type()))
            break;

        CHECK(isChunkValid(chunk.type()));

        if (chunk.type() == ChunkType::kInvalid)
            this->chunk_map_.erase(hash_offset);
        else {
            chunk_map_.emplace(hash_offset, chunk_offset);
        }

        offset = chunk_offset + chunk.numBytes();
        if (offset >= end_offset) 
            break;
    }

    return reinterpret_cast<size_t>(uintptr_t(offset) - uintptr_t(segment->segment_));
}

void LSTStore::load(void* address) {
    size_t v[5];
    readInteger(reinterpret_cast<char*>(address), v);

    free_list_             = offsetToLstSegmentPtr(v[0]);
    major_list_            = offsetToLstSegmentPtr(v[1]);
    minor_list_            = offsetToLstSegmentPtr(v[3]);

    linkSegmentList(free_list_);
    linkSegmentList(major_list_);
    linkSegmentList(minor_list_);

    // load from the major list first
    LSTSegment *iter = major_list_;
    while (segmentPtrToOffset(iter) != v[2]) {
        loadFromValidSegment(iter);
        iter = iter->next_;
    }

    if (iter != nullptr) {
        major_segment_offset_ = loadFromLastSegment(iter);
        current_major_segment_ = iter;
    }

    // and then from minor list
    iter = minor_list_;
    while (segmentPtrToOffset(iter) != v[4]) {
        loadFromValidSegment(iter);
        iter = iter->next_;
    }

    if (iter != nullptr) {
        minor_segment_offset_ = loadFromLastSegment(iter);
        current_minor_segment_ = iter;
    }
}

const Chunk* LSTStore::Get(const Hash& key) {
    LSTHash hash(key.value());
    CHECK(this->chunk_map_.count(hash) == 1);
    return this->chunk_map_.at(hash).toChunk();
}

bool LSTStore::Put(const Hash& key, const Chunk& chunk) {
    static size_t to_sync_chunks = 0;
    static auto last_sync_time_point = std::chrono::steady_clock::now();
    CHECK(this->chunk_map_.count(key.value()) == 0);

    size_t len = key.kByteLength + chunk.numBytes();
    if (current_major_segment_ == nullptr || getFreeSpaceMajor() < len) {
        // clear the remaining bytes
        if (current_major_segment_ != nullptr)
            std::memset(reinterpret_cast<char*>(current_major_segment_->segment_) + major_segment_offset_, 0, getFreeSpaceMajor());

        allocateMajor();
        last_sync_time_point = std::chrono::steady_clock::now();
    }

    byte_t* offset = reinterpret_cast<byte_t*>(current_major_segment_->segment_)
        + major_segment_offset_;

    std::copy(key.value(), key.value() + key.kByteLength, offset);
    std::copy(chunk.head(), chunk.head() + chunk.numBytes(), offset + key.kByteLength);
    this->chunk_map_.emplace(offset, offset + key.kByteLength);
    major_segment_offset_ += key.kByteLength + chunk.numBytes();
    to_sync_chunks++;
    if (to_sync_chunks >= MAX_PENDING_SYNC_CHUNKS || last_sync_time_point 
            + std::chrono::milliseconds(MAX_SYNC_TIMEOUT_MILLSECONDS) 
                < std::chrono::steady_clock::now()) {
        //LOG(INFO) << "LSTStore: sync " << to_sync_chunks << " chunks to segment " << (segmentPtrToOffset(current_major_segment_) - META_LOG_SIZE) / SEGMENT_SIZE;
        syncToDisk(current_major_segment_->segment_, SEGMENT_SIZE);
        to_sync_chunks = 0;
        last_sync_time_point = std::chrono::steady_clock::now();
    }
    return true;
}

void LSTStore::sync() {
    if (current_major_segment_ != nullptr)
        syncToDisk(current_major_segment_->segment_, SEGMENT_SIZE);
    if (current_minor_segment_ != nullptr)
        syncToDisk(current_minor_segment_->segment_, SEGMENT_SIZE);
}

LSTStore::~LSTStore() {
    sync();
    destroySegmentList(free_list_);
    destroySegmentList(major_list_);
    destroySegmentList(minor_list_);
}
    
} /* namespace lst_store */
} /* namespace ustore */
