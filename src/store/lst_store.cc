// Copyright (c) 2017 The Ustore Authors.

#include "store/lst_store.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include <boost/filesystem.hpp>

#include <chrono>
#include <type_traits>

#include "utils/chars.h"
#include "utils/logging.h"

namespace ustore {
namespace lst_store {

void* LSTSegment::base_addr_ = 0;

namespace fs = boost::filesystem;

void DestroySegmentList(LSTSegment* head) {
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
static void FdWriteThenSync(int fd, const void* buf, size_t length) {
  ssize_t bytes = 0;
  while (bytes < length) {
    int nb = ::write(fd, reinterpret_cast<const char*>(buf) + bytes,
                     length - bytes);
    CHECK_GE(nb, 0);
    bytes += nb;
  }
  int ret = fsync(fd);  // sync to disk
  CHECK_NE(ret, -1);
}

static void SyncToDisk(void* addr, size_t len) {
  int ret = msync(addr, len, MS_SYNC);
  if (ret == -1)
    LOG(FATAL) << "MSYNC ERROR: " << std::strerror(errno);
}

constexpr static inline bool IsChunkValid(ChunkType type) noexcept {
  return type == ChunkType::kNull
         || type == ChunkType::kCell
         || type == ChunkType::kMeta
         || type == ChunkType::kBlob
         || type == ChunkType::kString
         || type == ChunkType::kInvalid;
}

constexpr static inline bool IsEndChunk(ChunkType type) noexcept {
  return type == ChunkType::kNull;
}

constexpr static inline LSTSegment* OffsetToLstSegmentPtr(size_t offset)
    noexcept {
  return (offset == 0) ? nullptr :
    new LSTSegment(reinterpret_cast<char*>(LSTSegment::base_addr_) + offset);
}

constexpr static inline size_t SegmentPtrToOffset(const LSTSegment* ptr)
    noexcept {
  return ptr == nullptr ? 0 :
    ((uintptr_t)ptr->segment_ - (uintptr_t)LSTSegment::base_addr_);
}

constexpr static inline byte_t* GetValidateHashPtr(const LSTSegment* segment)
    noexcept {
  return reinterpret_cast<byte_t*>(segment->segment_) + kSegmentSize
    - Hash::kByteLength;
}

constexpr static inline byte_t* GetFirstHashPtr(const LSTSegment* segment)
  noexcept {
  return reinterpret_cast<byte_t*>(const_cast<void*>(segment->segment_))
    + 2 * sizeof(size_t);
}

static void LinkSegmentList(LSTSegment* begin) noexcept {
  LSTSegment *iter = begin;
  LSTSegment *last = nullptr;
  while (iter != nullptr) {
    iter->prev_ = last;
    size_t v[2];
    ReadInteger(reinterpret_cast<char*>(iter->segment_), v);
    LSTSegment *next = OffsetToLstSegmentPtr(v[1]);
    iter->next_ = next;
    last = iter;
    iter = next;
  }
}

/**
 * @brief mmap the log file; if the give @param file does not exist, create a
 * new file with the given name in the given @param dir.
 *
 * @param dir
 * @param file
 *
 * @return the address of the memory region where the log file is mapped into
 */
void* LSTStore::MmapUstoreLogFile(const char* dir, const char* file) {
  fs::path path;
  if (dir == nullptr)
    path = fs::current_path();
  else
    path = fs::path(dir);
  path /= std::string(file);

  int fd;
  if (fs::exists(path) && fs::file_size(path) == kLogFileSize) {
    fd = ::open(path.c_str(), O_RDWR, S_IRWXU);
  } else {
    // init the log
    fd = ::open(path.c_str(), O_RDWR | O_CREAT, S_IRWXU);
    size_t* buf1 = new size_t[kMetaLogSize/sizeof(size_t)];
    size_t* buf2 = new size_t[kSegmentSize/sizeof(size_t)];
    char* meta = reinterpret_cast<char*>(buf1);
    char* segment = reinterpret_cast<char*>(buf2);
    std::memset(meta, 0, kMetaLogSize);
    std::memset(segment, 0, kSegmentSize);

    size_t first_segment_offset = kMetaLogSize;
    // only a free list
    AppendInteger(meta, first_segment_offset);
    LOG(INFO) << "init the meta segment";
    FdWriteThenSync(fd, meta, kMetaLogSize);

    for (int i = 0; i < kNumSegments; ++i) {
      LOG(INFO) << "init the " << i << "-th segment";
      size_t prev_segment_offset = 0, next_segment_offset = 0;
      if (i > 0)
        prev_segment_offset = (i - 1) * kSegmentSize + kMetaLogSize;
      if (i < kNumSegments - 1)
        next_segment_offset = (i + 1) * kSegmentSize + kMetaLogSize;
      AppendInteger(segment, prev_segment_offset, next_segment_offset);
      FdWriteThenSync(fd, segment, kSegmentSize);
    }
  }

  CHECK_GE(fd, 0);
  void* address = ::mmap(nullptr, kLogFileSize, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, 0);
  if (address == reinterpret_cast<void*>(-1)) {
    LOG(FATAL) << "MMAP ERROR: " << std::strerror(errno);
  }
  // lock the mmap'ed memory to guarantee in-memory access
  ::mlock(address, kLogFileSize);
  LSTSegment::base_addr_ = address;
  this->Load(address);
  return address;
}

LSTSegment* LSTStore::AllocateMajor() {
  current_major_segment_ = Allocate(current_major_segment_);
  if (major_list_ == nullptr)
    major_list_ = current_major_segment_;
  // persist the log meta block
  AppendInteger(static_cast<char*>(LSTSegment::base_addr_),
                SegmentPtrToOffset(free_list_),
                SegmentPtrToOffset(major_list_),
                SegmentPtrToOffset(current_major_segment_),
                SegmentPtrToOffset(minor_list_),
                SegmentPtrToOffset(current_minor_segment_));
  SyncToDisk(LSTSegment::base_addr_, kMetaLogSize);
  major_segment_offset_ = 2 * sizeof(size_t);
  return current_major_segment_;
}

LSTSegment* LSTStore::AllocateMinor() {
  current_minor_segment_ = Allocate(current_minor_segment_);
  if (minor_list_ == nullptr)
    minor_list_ = current_minor_segment_;
  // persist the log meta block
  AppendInteger(static_cast<char*>(LSTSegment::base_addr_),
                SegmentPtrToOffset(free_list_),
                SegmentPtrToOffset(major_list_),
                SegmentPtrToOffset(current_major_segment_),
                SegmentPtrToOffset(minor_list_),
                SegmentPtrToOffset(current_minor_segment_));
  SyncToDisk(LSTSegment::base_addr_, kMetaLogSize);
  minor_segment_offset_ = 2 * sizeof(size_t);
  return current_minor_segment_;
}

LSTSegment* LSTStore::Allocate(LSTSegment* current) {
  if (free_list_ == nullptr)
    LOG(FATAL) << "Chunk store: out of log memory";
  LSTSegment* next = free_list_;
  if (current != nullptr) {
    // write the hash of the first chunk into the last 20 bytes of the
    // current segment
    byte_t* first_hash = GetFirstHashPtr(current);
    std::copy(first_hash, first_hash + ::ustore::Hash::kByteLength,
        GetValidateHashPtr(current));
    AppendInteger(reinterpret_cast<char*>(current->segment_) + sizeof(size_t),
        SegmentPtrToOffset(next));
    SyncToDisk(current->segment_, kSegmentSize);
  }

  // persist the meta data of the new allocated segment
  size_t prev_offset = SegmentPtrToOffset(current), next_offset = 0;
  AppendInteger(reinterpret_cast<char*>(next->segment_), prev_offset,
                next_offset);
  SyncToDisk(next->segment_, kMetaLogSize);

  // update the segment list; for free list, one-direct link list suffices
  free_list_ = next->next_;
  next->prev_ = current;
  next->next_ = nullptr;
  current = next;
  return current;
}

size_t LSTStore::LoadFromLastSegment(LSTSegment* segment) {
  // first check if it is the last segment, if not, simply make it the last
  size_t next_segment_offset;
  ReadInteger(reinterpret_cast<char*>(segment->segment_) + sizeof(size_t),
              next_segment_offset);

  bool need_sync = false;
  if (next_segment_offset != 0) {
    AppendInteger(reinterpret_cast<char*>(segment->segment_) + sizeof(size_t),
                  (size_t)0);
    CHECK_EQ((uintptr_t)free_list_->segment_,
      next_segment_offset + (uintptr_t)LSTSegment::base_addr_);
    need_sync = true;
  }

  byte_t* offset = reinterpret_cast<byte_t*>(segment->segment_)
                   + 2 * sizeof(size_t);
  while (true) {
    Chunk chunk(offset + ::ustore::Hash::kByteLength);
    if (!IsChunkValid(chunk.type()) || Hash(offset) != chunk.hash()) {
      // check failed; exit
      std::memset(offset, kSegmentSize + (uintptr_t)(segment->segment_)
                  - (uintptr_t)offset, 0);
      need_sync = true;
      break;
    }
    if (IsEndChunk(chunk.type()))
      break;
    if (chunk.type() == ChunkType::kInvalid)
      this->chunk_map_.erase(offset);
    else
      chunk_map_.emplace(offset, chunk.head());
    offset += chunk.numBytes() + ::ustore::Hash::kByteLength;
  }
  if (need_sync)
    SyncToDisk(segment->segment_, kSegmentSize);
  return reinterpret_cast<size_t>(uintptr_t(offset)
                                  - uintptr_t(segment->segment_));
}

size_t LSTStore::LoadFromValidSegment(LSTSegment* segment) {
  // TODO(qingchao): take alignment into consideration
  CHECK_NE(segment, nullptr);
  byte_t* offset = GetFirstHashPtr(segment);
  byte_t* end_offset = GetValidateHashPtr(segment) - Hash::kByteLength
                       - Chunk::kMetaLength;
  CHECK(Hash(offset) == Hash(GetValidateHashPtr(segment)));
  while (true) {
    byte_t *hash_offset = offset;
    byte_t *chunk_offset = hash_offset + ustore::Hash::kByteLength;
    Chunk chunk(chunk_offset);

    // check whether all chunks have been loaded or not
    if (IsEndChunk(chunk.type()))
      break;
    CHECK(IsChunkValid(chunk.type()));
    if (chunk.type() == ChunkType::kInvalid)
      this->chunk_map_.erase(hash_offset);
    else
      chunk_map_.emplace(hash_offset, chunk_offset);
    offset = chunk_offset + chunk.numBytes();
    if (offset >= end_offset)
      break;
  }
  return reinterpret_cast<size_t>(uintptr_t(offset)
                                  - uintptr_t(segment->segment_));
}

void LSTStore::Load(void* address) {
  size_t v[5];
  ReadInteger(reinterpret_cast<char*>(address), v);
  free_list_ = OffsetToLstSegmentPtr(v[0]);
  major_list_ = OffsetToLstSegmentPtr(v[1]);
  minor_list_ = OffsetToLstSegmentPtr(v[3]);
  LinkSegmentList(free_list_);
  LinkSegmentList(major_list_);
  LinkSegmentList(minor_list_);

  // load from the major list first
  LSTSegment *iter = major_list_;
  while (SegmentPtrToOffset(iter) != v[2]) {
    LoadFromValidSegment(iter);
    iter = iter->next_;
  }
  if (iter != nullptr) {
    major_segment_offset_ = LoadFromLastSegment(iter);
    current_major_segment_ = iter;
  }

  // and then from minor list
  iter = minor_list_;
  while (SegmentPtrToOffset(iter) != v[4]) {
    LoadFromValidSegment(iter);
    iter = iter->next_;
  }
  if (iter != nullptr) {
    minor_segment_offset_ = LoadFromLastSegment(iter);
    current_minor_segment_ = iter;
  }
}

const Chunk* LSTStore::Get(const Hash& key) {
  LSTHash hash(key.value());
  CHECK_EQ(this->chunk_map_.count(hash), 1);
  return this->chunk_map_.at(hash).toChunk();
}

bool LSTStore::Put(const Hash& key, const Chunk& chunk) {
  // if key already exists, return without error
  // CHECK_EQ(this->chunk_map_.count(key.value()), 0);
  if (chunk_map_.find(key.value()) != chunk_map_.end()) {
    DLOG(WARNING) << "key:" << key.ToBase32() << " already exists";
    return true;
  }
  
  static size_t to_sync_chunks = 0;
  static auto last_sync_time_point = std::chrono::steady_clock::now();

  size_t len = key.kByteLength + chunk.numBytes();
  if (current_major_segment_ == nullptr || GetFreeSpaceMajor() < len) {
    // clear the remaining bytes
    if (current_major_segment_ != nullptr)
      std::memset(reinterpret_cast<char*>(current_major_segment_->segment_)
                  + major_segment_offset_, 0, GetFreeSpaceMajor());
    AllocateMajor();
    last_sync_time_point = std::chrono::steady_clock::now();
  }

  byte_t* offset = reinterpret_cast<byte_t*>(current_major_segment_->segment_)
                   + major_segment_offset_;
  std::copy(key.value(), key.value() + key.kByteLength, offset);
  std::copy(chunk.head(), chunk.head() + chunk.numBytes(),
            offset + key.kByteLength);
  this->chunk_map_.emplace(offset, offset + key.kByteLength);
  major_segment_offset_ += key.kByteLength + chunk.numBytes();
  to_sync_chunks++;
  if (to_sync_chunks >= kMaxPendingSyncChunks || last_sync_time_point
      + std::chrono::milliseconds(kMaxSyncTimeoutMilliseconds)
      < std::chrono::steady_clock::now()) {
    // DLOG(INFO) << "LSTStore: sync " << to_sync_chunks
    //   << " chunks to segment " << (segmentPtrToOffset(current_major_segment_)
    //   - kMetaLogSize) / kSegmentSize;
    SyncToDisk(current_major_segment_->segment_, kSegmentSize);
    to_sync_chunks = 0;
    last_sync_time_point = std::chrono::steady_clock::now();
  }
  return true;
}

void LSTStore::Sync() {
  if (current_major_segment_ != nullptr)
    SyncToDisk(current_major_segment_->segment_, kSegmentSize);
  if (current_minor_segment_ != nullptr)
    SyncToDisk(current_minor_segment_->segment_, kSegmentSize);
}

LSTStore::~LSTStore() {
  Sync();
  DestroySegmentList(free_list_);
  DestroySegmentList(major_list_);
  DestroySegmentList(minor_list_);
}

}  // namespace lst_store
}  // namespace ustore