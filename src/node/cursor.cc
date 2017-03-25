// Copyright (c) 2017 The Ustore Authors.

#include <cstring>  // for memcpy

#include "hash/hash.h"

#include "node/blob_node.h"
#include "node/cursor.h"
#include "node/node.h"
#include "node/orderedkey.h"

#include "utils/logging.h"

namespace ustore {

// utility function for cursor usage
const SeqNode* CreateSeqNodeFromChunk(const Chunk* chunk) {
  CHECK_NE(chunk, nullptr);

  switch (chunk->type()) {
    case kMetaChunk:
      return new MetaNode(chunk);
    case kBlobChunk:
      return new BlobNode(chunk);
    default:
      LOG(FATAL) << "Other Non-chunkable Node Not Supported!";
      return nullptr;
  }
}

NodeCursor* NodeCursor::GetCursorByIndex(const Hash& hash, size_t idx,
                                         ChunkLoader* ch_loader) {
  const SeqNode* seq_node = nullptr;
  NodeCursor* parent_cursor = nullptr;
  size_t element_idx = idx;
  size_t entry_idx = 0;

  const Chunk* chunk = ch_loader->Load(hash);
  seq_node = CreateSeqNodeFromChunk(chunk);

  while (!seq_node->isLeaf()) {
    const MetaNode* mnode = dynamic_cast<const MetaNode*>(seq_node);
    Hash child_hash = mnode->GetChildHashByIndex(element_idx, &entry_idx);

    if (child_hash.empty()) {
      CHECK_EQ(entry_idx, mnode->numEntries());
      // idx exceeds the number of total elements
      //  child_hash = child hash of last entry
      //  entry_idx = idx of the last entry
      child_hash = mnode->GetChildHashByEntry(mnode->numEntries() - 1);
      entry_idx = mnode->numEntries() - 1;
    }

    parent_cursor =
        new NodeCursor(seq_node, entry_idx, ch_loader, parent_cursor);

    element_idx -= mnode->numElementsUntilEntry(entry_idx);
    chunk = ch_loader->Load(child_hash);
    seq_node = CreateSeqNodeFromChunk(chunk);
  }

  // if the element_idx > num of elements at leaf
  //   make cursor point to the end of leaf
  //   entry_idx = numEntries()
  const LeafNode* lnode = dynamic_cast<const LeafNode*>(seq_node);
  if (element_idx > lnode->numEntries()) {
    entry_idx = lnode->numElements();
  } else {
    entry_idx = element_idx;
  }

  return new NodeCursor(seq_node, entry_idx, ch_loader, parent_cursor);
}

NodeCursor* NodeCursor::GetCursorByKey(const Hash& hash, const OrderedKey& key,
                                       ChunkLoader* ch_loader) {
  const SeqNode* seq_node = nullptr;
  NodeCursor* parent_cursor = nullptr;
  size_t entry_idx = 0;

  const Chunk* chunk = ch_loader->Load(hash);
  seq_node = CreateSeqNodeFromChunk(chunk);

  while (!seq_node->isLeaf()) {
    const MetaNode* mnode = dynamic_cast<const MetaNode*>(seq_node);
    Hash child_hash = mnode->GetChildHashByKey(key, &entry_idx);

    if (child_hash.empty()) {
      CHECK_EQ(entry_idx, mnode->numEntries());
      // idx exceeds the number of total elements
      //  child_hash = child hash of last entry
      //  entry_idx = idx of the last entry
      child_hash = mnode->GetChildHashByEntry(mnode->numEntries() - 1);
      entry_idx = mnode->numEntries() - 1;
    }

    parent_cursor =
        new NodeCursor(seq_node, entry_idx, ch_loader, parent_cursor);

    chunk = ch_loader->Load(child_hash);
    seq_node = CreateSeqNodeFromChunk(chunk);
  }

  // if the element_idx > num of elements at leaf
  //   make cursor point to the end of leaf
  //   entry_idx = numEntries()
  const LeafNode* lnode = dynamic_cast<const LeafNode*>(seq_node);

  bool found;
  entry_idx = lnode->GetIdxForKey(key, &found);
  return new NodeCursor(seq_node, entry_idx, ch_loader, parent_cursor);
}
// copy cosntructor
NodeCursor::NodeCursor(const NodeCursor& cursor)
    : parent_cr_(nullptr),
      seq_node_(cursor.seq_node_),
      chunk_loader_(cursor.chunk_loader_),
      idx_(cursor.idx_) {
  if (cursor.parent_cr_ != nullptr) {
    this->parent_cr_ = new NodeCursor(*(cursor.parent_cr_));
  }
}

NodeCursor::~NodeCursor() {
  // delete parent cursor recursively
  delete parent_cr_;
}

NodeCursor::NodeCursor(const SeqNode* seq_node, size_t idx,
                       ChunkLoader* chunk_loader, NodeCursor* parent_cr)
    : parent_cr_(parent_cr),
      seq_node_(seq_node),
      chunk_loader_(chunk_loader),
      idx_(idx) {
  // do nothing
}

bool NodeCursor::Advance(bool cross_boundary) {
  // InitLogging("");
  // LOG(INFO) << "Before Advance: idx = " << idx_;
  // LOG(INFO) << "  numEntries = " << seq_node_->numEntries();
  // LOG(INFO) << "  isLeaf = " << seq_node_->isLeaf();

  // when idx == -1, idx_ < seq_node->numEntries is false.
  //  This is because lhs is signed and rhs is not signed.
  //  Hence, add extra test on idx_ == -1
  if (idx_ == -1 || idx_ < seq_node_->numEntries()) {
    ++idx_;
  }
  if (idx_ < seq_node_->numEntries()) return false;

  CHECK_EQ(idx_, seq_node_->numEntries());
  // not allow to cross boundary,
  //   remain idx = numEntries()
  if (!cross_boundary) return true;

  // current cursor points to the seq end (idx = numEntries())
  //   will be retreated by child cursor
  if (parent_cr_ == nullptr) return true;

  if (parent_cr_->Advance(true)) {
    // curent parent cursor now points the seq end, (idx = numEntries())
    //   must retreat to point to the last element (idx = numEntries() - 1)
    parent_cr_->Retreat(false);
    return true;
  } else {
    MetaEntry me(parent_cr_->current());
    const Chunk* chunk = chunk_loader_->Load(me.targetHash());
    seq_node_ = CreateSeqNodeFromChunk(chunk);
    CHECK_GT(seq_node_->numEntries(), 0);
    idx_ = 0;  // point the first element
    return false;
  }
}

bool NodeCursor::Retreat(bool cross_boundary) {
  if (idx_ >= 0) {
    --idx_;
  }
  if (idx_ >= 0) return false;

  CHECK_EQ(idx_, -1);
  // not allow to cross boundary,
  //   remain idx = -1
  if (!cross_boundary) return true;

  // remain idx = -1, to be advanced by child cursor.
  if (parent_cr_ == nullptr) return true;

  if (parent_cr_->Retreat(true)) {
    // parent cursor now points the seq start, (idx = -1)
    //   must advance to point to the frist element (idx = 0)
    parent_cr_->Advance(false);
    return true;
  } else {
    MetaEntry me(parent_cr_->current());
    const Chunk* chunk = chunk_loader_->Load(me.targetHash());
    seq_node_ = CreateSeqNodeFromChunk(chunk);
    CHECK_GT(seq_node_->numEntries(), 0);
    idx_ = seq_node_->numEntries() - 1;  // point to the last element
    return false;
  }
}
const byte_t* NodeCursor::current() const {
  if (idx_ == -1) {
    LOG(WARNING) << "Cursor points to Seq Head. Return nullptr.";
    return nullptr;
  }

  if (idx_ == seq_node_->numEntries()) {
    LOG(WARNING) << "Cursor points to Seq End. Return nullptr.";
    return nullptr;
  }

  return seq_node_->data(idx_);
}

size_t NodeCursor::numCurrentBytes() const {
  if (idx_ == -1) {
    LOG(WARNING) << "Cursor points to Seq Head. Return 0.";
    return 0;
  }

  if (idx_ == seq_node_->numEntries()) {
    LOG(WARNING) << "Cursor points to Seq End. Return 0.";
    return 0;
  }

  return seq_node_->len(idx_);
}
}  // namespace ustore