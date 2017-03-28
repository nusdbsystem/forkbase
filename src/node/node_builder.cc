// Copyright (c) 2017 The Ustore Authors.

#include "node/node_builder.h"

#include <cstring>  // for memcpy
#include "hash/hash.h"
#include "node/cursor.h"
#include "node/node.h"
#include "node/orderedkey.h"
#include "store/chunk_store.h"
#include "utils/logging.h"

namespace ustore {

NodeBuilder* NodeBuilder::NewNodeBuilderAtIndex(const Hash& root_hash,
                                                size_t idx,
                                                ChunkLoader* chunk_loader) {
  NodeCursor* cursor =
      NodeCursor::GetCursorByIndex(root_hash, idx, chunk_loader);
  // LOG(INFO) << "\n\n\n";
  NodeBuilder* builder = new NodeBuilder(cursor, 0);
  return builder;
}

NodeBuilder* NodeBuilder::NewNodeBuilderAtKey(const Hash& root_hash,
                                              const OrderedKey& key,
                                              ChunkLoader* chunk_loader) {
  NodeCursor* cursor = NodeCursor::GetCursorByKey(root_hash, key, chunk_loader);
  // cursor now points to the leaf node
  NodeBuilder* builder = new NodeBuilder(cursor, 0);
  return builder;
}

NodeBuilder::NodeBuilder(size_t level)
    : cursor_(nullptr),
      parent_builder_(nullptr),
      entries_data_(),
      entries_num_bytes_(),
#ifdef TEST_NODEBUILDER
      rhasher_(RollingHasher::TestHasher()),
#else
      rhasher_(new RollingHasher()),
#endif
      commited_(true),
      num_appended_entries_(0),
      num_skip_entries_(0),
      level_(level) {
  // do nothing
}

NodeBuilder::NodeBuilder() : NodeBuilder(0) {}

NodeBuilder::~NodeBuilder() {
  delete rhasher_;
  delete cursor_;
  delete parent_builder_;
}

NodeBuilder::NodeBuilder(NodeCursor* cursor, size_t level)
    : cursor_(new NodeCursor(*cursor)),
      parent_builder_(nullptr),
      entries_data_(),
      entries_num_bytes_(),
#ifdef TEST_NODEBUILDER
      rhasher_(RollingHasher::TestHasher()),
#else
      rhasher_(new RollingHasher()),
#endif
      commited_(true),
      num_appended_entries_(0),
      num_skip_entries_(0),
      level_(level) {
  if (cursor_->parent() != nullptr) {
    parent_builder_ = new NodeBuilder(cursor_->parent(), level + 1);
  }
  Resume();
}

void NodeBuilder::Resume() {
  int32_t original_idx = cursor_->idx();
  // num of cursor's retreat
  size_t num_retreat = 0;
  // copy the entries from node start until cursor (exclusive)
  while (!cursor_->Retreat(false)) {
    size_t entry_num_bytes = cursor_->numCurrentBytes();
    byte_t* entry_data = new byte_t[entry_num_bytes];
    // TODO(pingcheng): avoid memcpy
    std::memcpy(entry_data, cursor_->current(), entry_num_bytes);
    entries_data_.push_front(entry_data);
    entries_num_bytes_.push_front(entry_num_bytes);
    ++num_retreat;
  }
  // LOG(INFO) << "Resume: # entries in current Chunk before the original
  // cursor: "
  //           << num_retreat;
  // this is possible if cursor points at seq begin
  if (cursor_->isBegin()) {
    cursor_->Advance(true);
  }
  // from then on, advance the cursor back to the original place
  // previous entries in the same chunk will be hashed during commit phase
  for (size_t i = 0; i < num_retreat; i++) cursor_->Advance(true);
  CHECK_EQ(original_idx, cursor_->idx());
  // LOG(INFO) << "Level " << level_
  //           << "\n# Entry in the current Chunk before the cursor: "
  //           << entries_data_.size() << "\n# Retreat: " << num_retreat <<
  //           "\n";
}

void NodeBuilder::AppendEntry(const byte_t* data, size_t num_bytes) {
  commited_ = false;
  ++num_appended_entries_;
  // LOG(INFO) << "Append Entry Byte: " << num_bytes;
  entries_data_.push_back(data);
  entries_num_bytes_.push_back(num_bytes);
}

size_t NodeBuilder::SkipEntries(size_t num_elements) {
  commited_ = false;
  // possible if this node builder is created during build process
  if (cursor_ == nullptr) return 0;
  // LOG(INFO) << "Level: " << level_ << " Before Skip Idx: " << cursor_->idx()
  //           << " Skip # Entry: " << num_elements;
  for (size_t i = 0; i < num_elements; i++) {
    if (!cursor_->Advance(false)) continue;
    // Here, cursor has already reached this chunk end
    //   we need to skip and remove the parent metaentry
    //     which points to current chunk
    if (parent_builder_ == nullptr || parent_builder()->SkipEntries(1) == 0) {
      // parent builder's cursor has also reached the seq end
      // therefore, current builder's cursor has also reached the seq end
      //   we can only skip i elements
      // Attempt to advance cursor to check
      // cursor indeed points the seq end and cannot advance
      //   even allowing crossing the boundary
      CHECK(cursor_->Advance(true));
      num_skip_entries_ += i;
      return i;
    } else {
      // LOG(INFO) << "Level: " << level_ << " Recursive Skipping Parents. ";
      bool AtEnd = cursor_->Advance(true);
      CHECK(!AtEnd);
    }  // end if parent_chunker
  }    // end for
  num_skip_entries_ += num_elements;
  return num_elements;
}

void NodeBuilder::SpliceEntries(size_t num_delete,
                                const std::vector<const byte_t*>& element_data,
                                const std::vector<size_t>& element_num_bytes) {
  if (!commited_) {
    LOG(FATAL) << "There exists some uncommited_ operations. "
               << " Commit them first before doing any operation. ";
  }
  commited_ = false;
  num_appended_entries_ = 0;
  size_t actual_delete = SkipEntries(num_delete);
  if (actual_delete < num_delete) {
    LOG(WARNING) << "Actual Remove " << actual_delete << " elements instead of "
                 << num_delete;
  }
  CHECK_EQ(element_data.size(), element_num_bytes.size());
  size_t num_elements = element_data.size();
  for (size_t i = 0; i < num_elements; ++i) {
    // LOG(INFO) << "Element Byte: " << elementsnum_bytes.at(i);
    AppendEntry(element_data.at(i), element_num_bytes.at(i));
  }
}

NodeBuilder* NodeBuilder::parent_builder() {
  if (parent_builder_ == nullptr) {
    parent_builder_ = new NodeBuilder();
  }
  return parent_builder_;
}

const Chunk* NodeBuilder::HandleBoundary(
    std::vector<const byte_t*>* chunk_entries_data,
    std::vector<size_t>* chunk_entries_num_bytes, MakeChunkFunc make_chunk) {
  // LOG(INFO) << "Start Handing Boundary. ";
  // LOG(INFO) << " Entry Count: " << chunk_entries_num_bytes->size();
  ChunkInfo chunk_info =
      make_chunk(*chunk_entries_data, *chunk_entries_num_bytes);
  const Chunk* chunk = chunk_info.first;
  const byte_t* metaentry_data = chunk_info.second.first;
  size_t metaentry_num_bytes = chunk_info.second.second;
  // LOG(INFO) << "me_num_bytes : " << metaentry_num_bytes;
  // Dump chunk into storage here
  store::GetChunkStore()->Put(chunk->hash(), *chunk);
  parent_builder()->AppendEntry(metaentry_data, metaentry_num_bytes);
  // LOG(INFO) << "Finish Appending Entry for parent builder.";
  // CHECK(rhasher_->CrossedBoundary());
  rhasher_->ClearLastBoundary();
  for (const byte_t* chunk_entry_data : *chunk_entries_data) {
    // LOG(INFO) << "Delete Data.";
    delete[] chunk_entry_data;
  }
  // LOG(INFO) << "Finish clearing appended data.";
  chunk_entries_data->clear();
  chunk_entries_num_bytes->clear();
  return chunk;
}

const Chunk* NodeBuilder::Commit(MakeChunkFunc make_chunk) {
  CHECK(!commited_);
  CHECK_EQ(entries_data_.size(), entries_num_bytes_.size());
  // As we are about to make new chunk,
  // parent metaentry that points the old chunk
  // shall be invalid and replaced by the created metaentry
  // of the new chunk.
  parent_builder()->SkipEntries(1);
  // size_t skip_p = parent_builder()->SkipEntries(1);
  // if (skip_p > 0) {
  //   LOG(INFO) << "Level: " << level_ << " Skipping Parents At Start. ";
  // }
  // First thing to do:
  // Detect Boundary and Make Chunk
  //   Pass the MetaEntry to upper level builder
  const Chunk* last_created_chunk = nullptr;
  // Entries data and size to make chunk
  std::vector<const byte_t*> chunk_entries_data;
  std::vector<size_t> chunk_entries_num_bytes;
  // iterate newly appended entries for making chunk
  //   only detecing a boundary, make a chunk
  size_t num_entries = entries_data_.size();
  // LOG(INFO) << "Level: " << level_
  //           << " # Entries to commit: " << entries_num_bytes_.size()
  //           << " # Append: " << num_appended_entries_
  //           << " # Skip: " << num_skip_entries_;
  for (size_t i = 0; i < num_entries; i++) {
    const byte_t* entry_data = entries_data_.front();
    size_t entry_num_bytes = entries_num_bytes_.front();
    entries_data_.pop_front();
    entries_num_bytes_.pop_front();
    chunk_entries_data.push_back(entry_data);
    chunk_entries_num_bytes.push_back(entry_num_bytes);
    rhasher_->HashBytes(entry_data, entry_num_bytes);
    if (rhasher_->CrossedBoundary()) {
      // LOG(INFO) << "Handle Boundary Here";
      last_created_chunk = HandleBoundary(&chunk_entries_data,
                                          &chunk_entries_num_bytes, make_chunk);
    }  // end if
  }    // end for
  // LOG(INFO) << "Append " << num_entries << " entries. ";
  // Second, combining original entries pointed by current cursor
  //   to make chunk
  if (cursor_ != nullptr && !cursor_->isEnd()) {
    bool advanced = 0;
    do {
      if (cursor_->idx() == 0 && advanced) {
        if (rhasher_->byte_hashed() == 0) break;
        parent_builder()->SkipEntries(1);
        // LOG(INFO) << "Level: " << level_ << "Commit Skipping Parents. ";
      }
      size_t entry_num_bytes = cursor_->numCurrentBytes();
      // XXX(wangji): again really need to copy?
      byte_t* entry_data = new byte_t[entry_num_bytes];
      std::memcpy(entry_data, cursor_->current(), entry_num_bytes);
      chunk_entries_data.push_back(entry_data);
      chunk_entries_num_bytes.push_back(entry_num_bytes);
      rhasher_->HashBytes(cursor_->current(), entry_num_bytes);
      // Create Chunk and append metaentries to upper builders
      //   if detecing boundary
      if (rhasher_->CrossedBoundary()) {
        last_created_chunk = HandleBoundary(
            &chunk_entries_data, &chunk_entries_num_bytes, make_chunk);
      }  // end if
      advanced = true;
    } while (!cursor_->Advance(true));
  }  // end if
  // LOG(INFO) << "Finish detecting boundary. ";
  if (chunk_entries_data.size() > 0) {
    // this could happen if the last entry of sequence
    // cannot form a boundary, we still need to make a explicit chunk
    // and append a metaentry to upper builder
    CHECK(cursor_ == nullptr ||
          (cursor_ != nullptr && cursor_->isEnd() && cursor_->Advance(true)));
    last_created_chunk = HandleBoundary(&chunk_entries_data,
                                        &chunk_entries_num_bytes, make_chunk);
  }  // end if
  // Comment the following line out
  //   because current NodeBuilder is allowed to commit for once.
  // commited_ = true;
  num_appended_entries_ = 0;
  CHECK_EQ(chunk_entries_data.size(), 0);
  CHECK(last_created_chunk != nullptr);
  // upper node builder would build a tree node with a single metaentry
  //   This node will be excluded from final prolley tree
  // LOG(INFO) << "Finish one level commiting.\n";
  if (parent_builder()->isInvalidNode()) {
    return last_created_chunk;
  } else {
    return parent_builder()->Commit(MetaNode::MakeChunk);
  }
}

}  // namespace ustore
