// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_CURSOR_H_
#define USTORE_NODE_CURSOR_H_

#include <cstddef>
#include "node/chunk_loader.h"
#include "node/orderedkey.h"
#include "node/node.h"
#include "types/type.h"

namespace ustore {

class NodeCursor {
 public:
  // Init Cursor to point at idx element at leaf in a tree
  // rooted at SeqNode with Hash
  static NodeCursor* GetCursorByIndex(const Hash& hash, size_t idx,
                                      ChunkLoader* ch_loader);

  // Init Cursor to point a element at leaf in a tree
  // The element has the smallest key larger than the parameter key
  static NodeCursor* GetCursorByKey(const Hash& hash, const OrderedKey& key,
                                    ChunkLoader* ch_loader);

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
  const byte_t* current() const;
  // return the number of bytes of pointed element
  size_t numCurrentBytes() const;

 private:
  // Init cursor given parent cursor
  // Internally use to create NodeCursor recursively
  NodeCursor(const Hash& hash, size_t idx, ChunkLoader chunk_loader,
             NodeCursor* parent_cr);

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

#endif  // USTORE_NODE_CURSOR_H_
