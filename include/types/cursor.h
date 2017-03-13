// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CURSOR_H_
#define USTORE_TYPES_CURSOR_H_

#include <cstddef>
#include "types/chunk_loader.h"
#include "types/node.h"
#include "types/orderedkey.h"
#include "types/type.h"

namespace ustore {

class NodeCursor {
 public:
  // Init Cursor to point at idx element at leaf in a tree
  // rooted at SeqNode with Hash
  NodeCursor(const Hash& hash, size_t idx, ChunkLoader* ch_loader_);
  // Init Cursor to point a element at leaf in a tree
  // The element has the smallest key larger than the parameter key
  // NodeCursor(const Hash& hash, const OrderedKey& key);
  // Copy constructor used to clone a NodeCursor
  // Need to recursively copy the parent NodeCursor
  NodeCursor(const NodeCursor& cursor);

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
  size_t numCurrentBytes() const;

 private:
  // Init cursor given parent cursor
  // Internally use to create NodeCursor recursively
  NodeCursor(const NodeCursor* parent_cr, size_t idx);
  // responsible to delete during destruction
  NodeCursor* parent_cr_ = nullptr;
  // the pointed sequence
  SeqNode* seq_node_;
  ChunkLoader* chunk_loader_;
  // the index of pointed elements
  size_t idx_;
  // the byte offset of idx-th element relative the chunk data header.
  // Must be in sync with idx!
  size_t byte_offset_;
};

}  // namespace ustore

#endif  // USTORE_TYPES_CURSOR_H_
