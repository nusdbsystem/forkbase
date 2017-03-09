// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CURSOR_H_
#define USTORE_TYPES_CURSOR_H_

#include <cstddef>

#include "types/type.h"
#include "types/node.h"
#include "types/orderedkey.h"
#include "types/chunk_manager.h"

namespace ustore {

class NodeCursor {
 public:
  // Init Cursor to point at idx element at leaf in a tree
  // rooted at SeqNode with Hash
  NodeCursor(const Hash& hash, size_t idx, ChunkLoader* ch_loader_);

  // Init Cursor to point a element at leaf in a tree
  // The element has the smallest key larger than the parameter key
  NodeCursor(const SeqNode* seq_node, const OrderedKey& key);

  // Copy constructor used to clone a NodeCursor
  // Need to recursively copy the parent NodeCursor
  NodeCursor(const NodeCursor& nd);

  // Advance the pointer by one element,
  // Allow to cross the boundary and advance to the start of next node
  // Return whether the operation succeeds E.g,
  //  False if cross_boundary = true and cursor points the very last element
  //  False if cross_boundary = false and cursor points the node's last element
  bool Advance(bool cross_boundary);

  // Retreate the pointer by one element,
  // Allow to cross the boundary and advance to the end of preceding node
  // Return whether the operation succeeds E.g,
  //  False if cross_boundary = true and cursor points the very first element
  //  False if cross_boundary = false and cursor points the node's first element
  bool Retreat(bool cross_boundary);

  // return the data pointed by current cursor
  const byte* current() const;

  // return the number of bytes of pointed element
  const size_t num_current_bytes() const;

 private:
  // Init cursor given parent cursor
  // Internally use to create NodeCursor recursively
  NodeCursor(const NodeCursor* parent_cr, size_t idx);

  NodeCursor* parent_cr_;  // responsible to delete during destruction

  // the hash of pointed sequence
  // Subject to change when the cursor pointing to another chunk
  Hash chunk_hash_;

  ChunkLoader* chunk_loader_;

  size_t idx;  // the index of pointed elements

  // the byte offset of idx-th element relative the chunk data header.
  // Must be in sync with idx!
  size_t byte_offset;

};


}  // namespace ustore

#endif  // USTORE_TYPES_CURSOR_H_
