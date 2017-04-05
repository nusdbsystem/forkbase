// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_CHUNKER_H_
#define USTORE_NODE_CHUNKER_H_

#include <cstddef>
#include <vector>
#include <utility>

#include "chunk/chunk.h"

namespace ustore {
class Segment {
  // Segment points to a continuous memory holding multiple entries
 public:
  Segment(const byte_t* data, size_t num_bytes)
      : data_(data), num_bytes_(num_bytes) {}

  // create an empty segment, to be prolonged later
  explicit Segment(const byte_t* data) : data_(data), num_bytes_(0) {}

  virtual ~Segment() {}

  virtual const byte_t* entry(size_t idx) const = 0;
  // prolong the segment by entries,
  //   for VarSegment can only prolong by one entry,
  //   for FixedSegment can prolong by multiple entries.
  // given its number of bytes
  //   prolong will keep the pointed data unchanged
  // return the number of entries after prolong operation
  virtual size_t prolong(size_t entry_num_bytes_) = 0;
  virtual size_t entryNumBytes(size_t idx) const = 0;
  virtual size_t numEntries() const = 0;
  // Split a segment at idx-entry into two segments
  //   the idx entry is the first of the second segments
  //   if idx = numEntries(), the second segment is empty
  virtual std::pair<const Segment*, const Segment*> Split(size_t idx) const = 0;
  // Append this segment on the chunk buffer
  //   number of appended bytes = numBytes()
  // Current implementation is the same for VarSegment and FixSegment
  virtual void AppendForChunk(byte_t* chunk_buffer) const;

  size_t numBytes() const { return num_bytes_; }
  bool empty() const { return numEntries() == 0; }
  const byte_t* data() const { return data_; }

 protected:
  const byte_t* data_;
  size_t num_bytes_;
};

class FixedSegment : public Segment {
  // Entries in FixedSegment are of the same number of bytes
 public:
  FixedSegment(const byte_t* data, size_t num_bytes, size_t bytes_per_entry)
      : Segment(data, num_bytes), bytes_per_entry_(bytes_per_entry) {}
  // create an empty FixedSegment given the data pointer
  FixedSegment(const byte_t* data, size_t bytes_per_entry)
      : Segment(data), bytes_per_entry_(bytes_per_entry) {}
  ~FixedSegment() override {}

  const byte_t* entry(size_t idx) const override;
  size_t prolong(size_t entry_num_bytes) override;
  size_t entryNumBytes(size_t idx) const override;
  inline size_t numEntries() const override {
    return num_bytes_ / bytes_per_entry_;
  }
  std::pair<const Segment*, const Segment*> Split(size_t idx) const override;

 private:
  size_t bytes_per_entry_;
};

class VarSegment : public Segment {
  // Entries in FixedSegment are of variable number of bytes
 public:
  VarSegment(const byte_t* data, size_t num_bytes,
             std::vector<size_t>&& entry_offsets)
      : Segment(data, num_bytes), entry_offsets_(entry_offsets) {}
  // An empty var segment to be prolonged later
  explicit VarSegment(const byte_t* data) : Segment(data) {}
  ~VarSegment() override {}

  const byte_t* entry(size_t idx) const override;
  size_t prolong(size_t entry_num_bytes_) override;
  size_t entryNumBytes(size_t idx) const override;
  inline size_t numEntries() const override { return entry_offsets_.size(); }
  std::pair<const Segment*, const Segment*> Split(size_t idx) const override;

 private:
  std::vector<size_t> entry_offsets_;
};

struct ChunkInfo {
  const Chunk* chunk;
  // a Segment that holding a single MetaEntry bytes
  const Segment* meta_seg;
};

class Chunker {
  // An interface to make chunk from multiple segments.
  //   Each type, e.g, Blob, MetaNode shall have one.
 public:
  virtual const ChunkInfo make(const std::vector<const Segment*>& segments)
      const = 0;
};
}  // namespace ustore
#endif  // USTORE_NODE_CHUNKER_H_
