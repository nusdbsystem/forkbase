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
  // if idx >= total_num elemnets,
  //   cursor points to the end of sequence.
  static NodeCursor* GetCursorByIndex(const Hash& hash, size_t idx,
                                      ChunkLoader* ch_loader);

  // Init Cursor to point a element at leaf in a tree
  // The element has the smallest key larger than the parameter key
  static NodeCursor* GetCursorByKey(const Hash& hash, const OrderedKey& key,
                                    ChunkLoader* ch_loader);

  // Copy constructor used to clone a NodeCursor
  // Need to recursively copy the parent NodeCursor
  NodeCursor(const NodeCursor& cursor);

  ~NodeCursor();

  // Advance the pointer by one element,
  // Allow to cross the boundary and advance to the start of next node
  // @return Return whether the cursor has already reached the end after the
  // operation
  //   1. True if cross_boundary = true and idx = numElements()
  //     of the last leaf node of the entire tree
  //   2. True if cross_boundary = false and idx = numElements()
  //     of the pointed leaf node
  bool Advance(bool cross_boundary);

  // Retreate the pointer by one element,
  // Allow to cross the boundary and point to the last element of preceding node
  // @return Return whether the cursor has already reached the first element
  //   1. True if cross_boundary = true and cursor points the very first element
  //   2. True if cross_boundary = false and cursor points the node's first
  //   element
  bool Retreat(bool cross_boundary);

  // return the data pointed by current cursor
  const byte_t* current() const;
  // return the number of bytes of pointed element
  size_t numCurrentBytes() const;

  // cursor places at seq end
  inline bool isEnd() const { return idx_ == seq_node_->numEntries(); }
  // cursor places at seq start
  inline bool isBegin() const { return idx_ == -1; }

  inline NodeCursor* parent() const { return parent_cr_; }

  inline int32_t idx() const { return idx_; }

 private:
  // Init cursor given parent cursor
  // Internally use to create NodeCursor recursively
  NodeCursor(const SeqNode* seq_node, size_t idx, ChunkLoader* chunk_loader,
             NodeCursor* parent_cr);

  // responsible to delete during destruction
  NodeCursor* parent_cr_ = nullptr;
  // the pointed sequence
  const SeqNode* seq_node_;
  ChunkLoader* chunk_loader_;
  // the index of pointed elements
  //   can be -1 when pointing to seq start
  int32_t idx_;
};

}  // namespace ustore

#endif  // USTORE_NODE_CURSOR_H_