// Copyright (c) 2017 The Ustore Authors.

#include "store/lst_store.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include <boost/filesystem.hpp>

#include <chrono>
#include <future>
#include <type_traits>

#include "utils/chars.h"
#include "utils/iterator.h"
#include "utils/logging.h"
#include "utils/timer.h"

#define LOG_LST_STORE_FATAL_ERROR_IF(condition, errmsg) \
  do { \
    if (condition) { \
      LOG(FATAL) << (errmsg) << std::strerror(errno); \
    } \
  } while (false)

namespace ustore {
namespace lst_store {

void* LSTSegment::base_addr_ = nullptr;

namespace fs = boost::filesystem;

static void DestroySegmentList(LSTSegment* head) {
  while (head != nullptr) {
    auto next = head->next_;
    delete head;
    head = next;
  }
}

/**
 * @brief persist length bytes of buf to fd
 *
 * @param fd
 * @param buf
 * @param length
 */
static void FdWriteThenSync(int fd, const void* buf, size_t length) {
  size_t nb = ::write(fd, reinterpret_cast<const char*>(buf), length);
  CHECK_EQ(nb, length);
  LOG_LST_STORE_FATAL_ERROR_IF(-1 == fsync(fd), "MSYNC ERROR");
}

static void SyncToDisk(void* addr, size_t len) {
  LOG_LST_STORE_FATAL_ERROR_IF(-1 == msync(addr, len, MS_SYNC), "MSYNC ERROR");
}

static inline LSTSegment* OffsetToLstSegmentPtr(offset_t offset) {
  return (offset == 0) ? nullptr :
    new LSTSegment(reinterpret_cast<char*>(LSTSegment::base_addr_) + offset);
}

static inline offset_t SegmentPtrToOffset(const LSTSegment* ptr) {
  return ptr == nullptr ? 0 :
    ((uintptr_t)ptr->segment_ - (uintptr_t)LSTSegment::base_addr_);
}

static inline byte_t* GetValidateHashPtr(const LSTSegment* segment) {
  return reinterpret_cast<byte_t*>(segment->segment_) + kSegmentSize
    - Hash::kByteLength;
}

static size_t LinkSegmentList(LSTSegment** begin) {
  LSTSegment *iter = *begin;
  LSTSegment *last = nullptr;
  size_t number  = 0;
  while (iter != nullptr) {
    ++number;
    iter->prev_ = last;
    offset_t v[2];
    ReadInteger(reinterpret_cast<char*>(iter->segment_), v);
    LSTSegment *next = OffsetToLstSegmentPtr(v[1]);
    iter->next_ = next;
    last = iter;
    iter = next;
  }
  *begin = last;
  return number;
}

static void onNewChunk(StoreInfo* storeInfo, ChunkType type,
                       size_t chunkLength) {
  storeInfo->chunks++;
  storeInfo->chunkBytes += chunkLength;
  storeInfo->validChunks++;
  storeInfo->validChunkBytes += chunkLength;
  storeInfo->chunksPerType[type]++;
  storeInfo->bytesPerType[type] += chunkLength;
}

static void onRemoveChunk(StoreInfo* storeInfo, ChunkType type,
                          size_t chunkLength) {
  // storeInfo->chunks++;
  storeInfo->validChunks--;
  storeInfo->validChunkBytes -= chunkLength;
  storeInfo->chunksPerType[type]--;
  storeInfo->bytesPerType[type] -= chunkLength;
}

static void initStoreInfo(StoreInfo* info) {
  info->segments = 0;
  info->usedSegments = 0;
  info->freeSegments = 0;
  info->chunks = 0;
  info->chunkBytes = 0;
  info->validChunks = 0;
  info->validChunkBytes = 0;

  for (auto type : Enum<ChunkType>()) {
    info->bytesPerType[type] = 0;
    info->chunksPerType[type] = 0;
  }
}

void LSTStore::updateStore() {
  if (sync_block_.segments_ <= 0) goto out;

  if (free_list_ == nullptr) {
    free_list_ = sync_block_.head_;
    // update the meta log
    AppendInteger((char*)LSTSegment::base_addr_, SegmentPtrToOffset(free_list_));
    SyncToDisk(LSTSegment::base_addr_, kMetaLogSize);
  } else {
    DCHECK_NE(last_free_segment_, nullptr);
    char* last_segment = 
      (char*)LSTSegment::base_addr_ + SegmentPtrToOffset(last_free_segment_);
    last_free_segment_->next_ = sync_block_.head_;
    AppendInteger(last_segment + sizeof(offset_t), SegmentPtrToOffset(sync_block_.head_));
    SyncToDisk(last_segment, kMetaSegmentSize);
  }

  last_free_segment_ = sync_block_.last_;
  storeInfo.freeSegments += sync_block_.segments_;
  storeInfo.segments += sync_block_.segments_;
  std::cout << GetInfo();
out:
  thread_status_.store(ThreadStatus::kUnscheduled);
}

void LSTStore::enlarge() {
  // wait until the status is set kScheduled
  while(thread_status_.load() != ThreadStatus::kScheduled);

  int new_segments = this->segment_increment_;
  if (storeInfo.segments + new_segments > this->num_segments_)
    new_segments = num_segments_ - storeInfo.segments;

  sync_block_.segments_ = new_segments;
  if (new_segments <= 0) {
    thread_status_.store(ThreadStatus::kCompleted);
    return;
  } // out of log memory

  char* meta_segment = new char[kSegmentSize];

  offset_t end_of_log = kMetaLogSize + storeInfo.segments * kSegmentSize;
  offset_t offset = end_of_log;
  ::lseek(fd_, offset, SEEK_SET);

  char a = 1;
  for (size_t i = 0; i < new_segments; offset += kSegmentSize, ++i) {
    DLOG(INFO) << "init the " << i << "-th new segment";
    offset_t prev_segment_offset = 0, next_segment_offset = 0;
    int chunk_id = i + storeInfo.segments;
    if (i > 0)
      prev_segment_offset = (chunk_id - 1) * kSegmentSize + kMetaLogSize;
    else
      prev_segment_offset = SegmentPtrToOffset(last_free_segment_);
      
    if (i < new_segments - 1)
      next_segment_offset = (chunk_id + 1) * kSegmentSize + kMetaLogSize;

    AppendInteger(meta_segment, prev_segment_offset, next_segment_offset);
    ::write(fd_, meta_segment, kSegmentSize);
    //::lseek(fd_, offset + kSegmentSize - 1, SEEK_SET);
    //::write(fd_, &a, 1);
  }

  SyncToDisk(end_of_log + reinterpret_cast<char*>(LSTSegment::base_addr_),
      new_segments * kSegmentSize);
  LOG(INFO) << "enlarge segments done";

  if (new_segments > 0) {
    sync_block_.head_ = OffsetToLstSegmentPtr(end_of_log);
    sync_block_.last_ = sync_block_.head_;
    int ns = LinkSegmentList(&sync_block_.last_);
    CHECK_EQ(ns, new_segments);
  }

  thread_status_.store(ThreadStatus::kCompleted);
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
void* LSTStore::MmapUstoreLogFile(const std::string& dir,
                                  const std::string& file, bool persist) {
  fs::path path(dir);
  path /= file + ".dat";
  if (!persist) fs::remove(path);

  if (fs::exists(path) && (fs::file_size(path) - kMetaLogSize) % kSegmentSize == 0) {
    fd_ = ::open(path.c_str(), O_RDWR, S_IRWXU);
  } else {
    // init the log
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    char* meta_log = new char[kMetaLogSize];
    std::memset(meta_log, 0, kMetaLogSize);
    meta_log[kMetaLogSize - 1] = 'a';
    LOG(INFO) << "init meta segment";
    ::write(fd_, meta_log, kMetaLogSize);
    delete[] meta_log;
    LOG_LST_STORE_FATAL_ERROR_IF(-1 == fsync(fd_), "FSYNC ERROR");
    LOG(INFO) << "init segments done";
  }

  CHECK_GE(fd_, 0);
  void* address = ::mmap(nullptr, max_log_size_, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd_, 0);
  LOG_LST_STORE_FATAL_ERROR_IF(address == reinterpret_cast<void*>(-1),
                               "MMAP ERROR: ");

  // lock the mmap'ed memory to guarantee in-memory access
  ::mlock(address, max_log_size_);
  LSTSegment::base_addr_ = address;
  initStoreInfo(&storeInfo);
  this->Load(address);
  if (storeInfo.freeSegments < segment_increment_) {
    thread_status_.store(ThreadStatus::kScheduled);
    enlarge();
    updateStore();
  }

  return address;
}

LSTSegment* LSTStore::AllocateMajor() {
  LSTSegment* new_segment = Allocate(current_major_segment_);
  if (current_major_segment_ != nullptr)
    current_major_segment_->next_ = new_segment;
  current_major_segment_ = new_segment;
  if (major_list_ == nullptr)
    major_list_ = current_major_segment_;
  // persist the log meta block if necessary
  AppendInteger(static_cast<char*>(LSTSegment::base_addr_),
      SegmentPtrToOffset(free_list_),
      SegmentPtrToOffset(major_list_),
      SegmentPtrToOffset(current_major_segment_));
  SyncToDisk(LSTSegment::base_addr_, kMetaLogSize);
  major_segment_offset_ = kMetaSegmentSize;
  return current_major_segment_;
}

LSTSegment* LSTStore::Allocate(LSTSegment* current) {

  if (storeInfo.freeSegments < segment_increment_) {
    ThreadStatus status = thread_status_.load();
    if (status == ThreadStatus::kUnscheduled) {
      LOG(INFO) << "Only " << segment_increment_ << " free segments; allocate more";
      thread_status_.store(ThreadStatus::kScheduled);
      async_future_ = std::async(std::launch::async, &LSTStore::enlarge, this);
    } else if (status == ThreadStatus::kCompleted) {
      updateStore();
    }
  }

  if (free_list_ == nullptr) {
    // if there is no free segment
    // wait until the thread status is set to kCompleted or kUnscheduled
    ThreadStatus ts = thread_status_.load();
    if (ts == ThreadStatus::kScheduled) 
      async_future_.get();
    ts = thread_status_.load();
    if (ts == ThreadStatus::kCompleted)
      updateStore();

    if (free_list_ == nullptr) {
      LOG(FATAL) << "Chunk store: out of log memory";
    }
  }

  LSTSegment* next = free_list_;

  if (current != nullptr) {
    // write the hash of the first chunk into the last 20 bytes of the
    // current segment
    byte_t* first_chunk = GetFirstChunkPtr(current);
    byte_t* first_hash = first_chunk + PtrToChunkLength(first_chunk);
    std::copy(first_hash, first_hash + ::ustore::Hash::kByteLength,
        GetValidateHashPtr(current));
    SyncToDisk(current->segment_, kSegmentSize);
    AppendInteger(reinterpret_cast<char*>(current->segment_) + sizeof(offset_t),
        SegmentPtrToOffset(next));
    // need to sync the content before metasegment to ensure the correct load
    SyncToDisk(current->segment_, kSegmentSize);
  }

  // persist the meta data of the new allocated segment
  offset_t prev_offset = SegmentPtrToOffset(current), next_offset = 0;
  AppendInteger(reinterpret_cast<char*>(next->segment_), prev_offset,
                next_offset);
  SyncToDisk(next->segment_, kMetaSegmentSize);

  // update the segment list; for free list, one-way link list suffices
  free_list_ = free_list_->next_;

  next->prev_ = current;
  next->next_ = nullptr;
  current = next;

  // update store information
  ++storeInfo.usedSegments;
  --storeInfo.freeSegments;

  return current;
}

offset_t LSTStore::LoadFromLastSegment(LSTSegment* segment) {
  // first check if it is the last segment, if not, simply make it the last
  offset_t next_segment_offset;
  ReadInteger(reinterpret_cast<char*>(segment->segment_) + sizeof(offset_t),
              next_segment_offset);

  bool need_sync = false;
  if (next_segment_offset != 0) {
    // since the next segment is empty and included in the free list,
    // it is safe to erase the respective offset
    AppendInteger(reinterpret_cast<char*>(segment->segment_) + sizeof(offset_t),
                  (offset_t)0);
    //CHECK_EQ((uintptr_t)free_list_->segment_,
    //  next_segment_offset + (uintptr_t)LSTSegment::base_addr_);
    need_sync = true;
  }

  byte_t* chunk_offset = GetFirstChunkPtr(segment);
  while (true) {
    Chunk chunk(chunk_offset);
    byte_t* hash_offset = chunk_offset + chunk.numBytes();
    if (!IsChunkValid(chunk.type()) || Hash(hash_offset) != chunk.hash()) {
      // check failed; exit
      std::memset(chunk_offset, 0, Chunk::kMetaLength);
      need_sync = true;
      break;
    }

    if (IsEndChunk(chunk.type()))
      break;

    if (chunk.type() == ChunkType::kInvalid) {
      this->chunk_map_.erase(hash_offset);
      onRemoveChunk(&storeInfo, chunk.type(),
                    PtrToChunkLength(this->chunk_map_.at(LSTHash(hash_offset)).chunk_));
    } else {
      chunk_map_.emplace(hash_offset, chunk.head());
      onNewChunk(&storeInfo, chunk.type(), chunk.numBytes());
    }
    chunk_offset += chunk.numBytes() + Hash::kByteLength;
  }

  if (need_sync)
    SyncToDisk(segment->segment_, kSegmentSize);

  return reinterpret_cast<offset_t>(uintptr_t(chunk_offset)
                                  - uintptr_t(segment->segment_));
}

offset_t LSTStore::LoadFromValidSegment(LSTSegment* segment) {
  // TODO(qingchao): take alignment into consideration
  CHECK_NE(segment, nullptr);
  const byte_t* chunk_offset = GetFirstChunkPtr(segment);
  CHECK(Hash(chunk_offset + PtrToChunkLength(chunk_offset)) == Hash(GetValidateHashPtr(segment)));

  while (true) {

    ChunkType type = PtrToChunkType(chunk_offset);

    // check whether all chunks have been loaded or not
    if (IsEndChunk(type))
      break;

    DCHECK(IsChunkValid(type));

    uint32_t chunk_length = PtrToChunkLength(chunk_offset);
    const byte_t *hash_offset = chunk_offset + chunk_length;
    if (type == ChunkType::kInvalid) {
      this->chunk_map_.erase(hash_offset);
      onRemoveChunk(&storeInfo, type,
                    chunk_length);
    } else {
      chunk_map_.emplace(hash_offset, chunk_offset);
      onNewChunk(&storeInfo, type, chunk_length);
    }

    chunk_offset += chunk_length + Hash::kByteLength;;
  }

  return reinterpret_cast<offset_t>(uintptr_t(chunk_offset)
      - uintptr_t(segment->segment_));
}

void LSTStore::Load(void* address) {
  offset_t v[5];
  ReadInteger(reinterpret_cast<char*>(address), v);
  free_list_ = OffsetToLstSegmentPtr(v[0]);
  major_list_ = OffsetToLstSegmentPtr(v[1]);

  LSTSegment* segment = free_list_;
  storeInfo.segments += LinkSegmentList(&segment);
  last_free_segment_ = segment;
  segment = major_list_;
  storeInfo.segments += LinkSegmentList(&segment);

  int loaded = 0;

  // load from the major list first
  LSTSegment *iter = major_list_;
  while (iter != segment) {
    LoadFromValidSegment(iter);
    iter = iter->next_;
    ++loaded;
  }

  if (iter != nullptr) {
    major_segment_offset_ = LoadFromLastSegment(iter);
    ++loaded;
  }
  current_major_segment_ = iter;

  storeInfo.usedSegments = loaded;
  storeInfo.freeSegments = storeInfo.segments - loaded;

  LOG(INFO) << "load completed";
  std::cout << GetInfo();
}

Chunk LSTStore::Get(const Hash& key) {
  LSTHash hash(key.value());
  auto it = chunk_map_.find(hash);
  if (it != chunk_map_.end())
    return Chunk(it->second.chunk_, it->first.hash_);
  LOG(WARNING) << "Key: " << key << " does not exist in chunk store";
  return Chunk();
}

bool LSTStore::Put(const Hash& key, const Chunk& chunk) {
  if (Exists(key)) return true;
  static Timer& timer = TimerPool::GetTimer("Write Chunk");
	timer.Start(); 

  static size_t to_sync_chunks = 0;
  static auto last_sync_time_point = std::chrono::steady_clock::now();

  size_t len = key.kByteLength + chunk.numBytes();
  if (current_major_segment_ == nullptr || GetFreeSpaceMajor() < len) {
    AllocateMajor();
    last_sync_time_point = std::chrono::steady_clock::now();
    to_sync_chunks = 0;
  }

  byte_t* offset = reinterpret_cast<byte_t*>(current_major_segment_->segment_)
                   + major_segment_offset_;
  // first write chunk and then write hash
  std::copy(chunk.head(), chunk.head() + chunk.numBytes(), offset);
  std::copy(key.value(), key.value() + key.kByteLength, offset + chunk.numBytes());
  this->chunk_map_.emplace(offset + chunk.numBytes(), offset);
  major_segment_offset_ += key.kByteLength + chunk.numBytes();
  to_sync_chunks++;
  onNewChunk(&storeInfo, chunk.type(), chunk.numBytes());
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
	timer.Stop();
  return true;
}

void LSTStore::Sync() const {
  if (current_major_segment_ != nullptr)
    SyncToDisk(current_major_segment_->segment_, kSegmentSize);
}

LSTStore::~LSTStore() noexcept(false) {
  Sync();
  std::cout << GetInfo();
  DestroySegmentList(free_list_);
  DestroySegmentList(major_list_);
  ::munmap(LSTSegment::base_addr_, max_log_size_);
  ::close(fd_);
}

}  // namespace lst_store
}  // namespace ustore
