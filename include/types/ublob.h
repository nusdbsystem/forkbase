// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UBLOB_H_
#define USTORE_TYPES_UBLOB_H_

#include <memory>
#include <utility>

#include "types/base.h"

namespace ustore {
class UBlob : public ChunkableType {
 public:
  // Return the number of bytes in this Blob
  inline size_t size() const { return root_node_->numElements(); }

  /** Read the blob data and copy into buffer
   *    Args:
   *      pos: the number of position to read
   *      len: the number of subsequent bytes to read into buffer
   *      buffer: the byte array which the data is copied to
   *
   *    Return:
   *      the number of bytes that actually read
   */
  size_t Read(size_t pos, size_t len, byte_t* buffer) const;

   /** Delete some bytes from a position and insert new bytes
   *
   *  Args:
   *    pos: the byte position to remove or insert bytes
   *    num_delete: the number of bytes to be deleted
   *    data: the byte array to insert after deletion
   *    num_insert: number of bytes in array to be inserted into current blob
   *
   *  Return:
   *    the new Blob reflecting the operation
   */

  virtual Hash Splice(size_t pos, size_t num_delete,
                      const byte_t* data,
                      size_t num_insert) const = 0;

  // * Insert bytes given a position

  //  *  Use Splice internally
  Hash Insert(size_t pos, const byte_t* data,
              size_t num_insert) const;

  /** Delete bytes from a given position
   *
   *  Use Splice internally
   */
  Hash Delete(size_t pos, size_t num_delete) const;

  /** Append bytes from the last position of Blob
   *
   *  Use Splice internally
   */
  Hash Append(byte_t* data, size_t num_insert) const;

 protected:
  explicit UBlob(std::shared_ptr<ChunkLoader> loader) noexcept :
      ChunkableType(loader) {}

  virtual ~UBlob() = default;

  bool SetNodeForHash(const Hash& hash) override;
};

class SBlob : public UBlob {
 public:
  // Load an exsiting SBlob
  explicit SBlob(const Hash& root_hash) noexcept;

  // Create a new SBlob
  explicit SBlob(const Slice& slice) noexcept;

  SBlob() noexcept : UBlob(std::make_shared<ChunkLoader>()) {}

  ~SBlob() = default;

  Hash Splice(size_t pos, size_t num_delete,
              const byte_t* data, size_t num_insert) const override;
};

}  // namespace ustore
#endif  // USTORE_TYPES_UBLOB_H_
