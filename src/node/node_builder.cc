// Copyright (c) 2017 The Ustore Authors.

#include <cstring>  // for memcpy

#include "hash/hash.h"

#include "node/cursor.h"
#include "node/node.h"
#include "node/node_builder.h"
#include "node/orderedkey.h"

#include "utils/logging.h"

#ifdef USE_LEVELDB
#include "utils/singleton.h"
#include "store/ldb_store.h"
/// wrong here. ldb should be global Singleton not an in file Singleton
static ustore::Singleton<ustore::LDBStore> ldb;
#endif  // USE_LEVELDB

namespace ustore {
NodeBuilder* NodeBuilder::NewNodeBuilderAtIndex(
                                            const Hash& root_hash,
                                            size_t idx,
                                            ChunkLoader* chunk_loader) {
  NodeCursor* cursor = NodeCursor::GetCursorByIndex(root_hash, idx,
                                                    chunk_loader);
  // LOG(INFO) << "\n\n\n";
  NodeBuilder* builder = new NodeBuilder(cursor, 0);
  return builder;
}

NodeBuilder* NodeBuilder::NewNodeBuilderAtKey(
                                            const Hash& root_hash,
                                            const OrderedKey& key,
                                            ChunkLoader* chunk_loader) {
  NodeCursor* cursor = NodeCursor::GetCursorByKey(root_hash, key,
                                                  chunk_loader);

  // cursor now points to the leaf node
  NodeBuilder* builder = new NodeBuilder(cursor, 0);
  return builder;
}


NodeBuilder::NodeBuilder(size_t level):
                          cursor_(nullptr),
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
                          level_(level) {
  // do nothing
}

NodeBuilder::NodeBuilder() : NodeBuilder(0) {}

NodeBuilder::NodeBuilder(NodeCursor* cursor, size_t level):
                                  cursor_(new NodeCursor(*cursor)),
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
                                  level_(level) {
  if (cursor_->parent() != nullptr) {
    parent_builder_  = new NodeBuilder(cursor_->parent(), level + 1);
  }
  Resume();
}

void NodeBuilder::Resume() {
  int32_t original_idx = cursor_->idx();
  size_t window_size = rhasher_->window_size();

  size_t num_bytes_sum = 0;

  // num of cursor's retreat
  size_t num_retreat = 0;

  // copy the entries from node start until cursor (exclusive)
  while (!cursor_->Retreat(false)) {
    size_t entry_num_bytes = cursor_->numCurrentBytes();
    byte_t* entry_data = new byte_t[entry_num_bytes];
    /*XXX(wangji): really need to copy?*/
    memcpy(entry_data, cursor_->current(), entry_num_bytes);
    entries_data_.push_front(entry_data);
    entries_num_bytes_.push_front(entry_num_bytes);

    num_bytes_sum += entry_num_bytes;
    ++num_retreat;
  }
  // LOG(INFO) << "Num Pre Chunk: " << num_retreat;
  // We may need to go back to entries in previous chunk
  // in order to populate the rolling hash window size

  // num of entries in previous chunk
  //   that need to be hashed into rolling hasher window
  size_t num_pre_entries = 0;
  while (num_bytes_sum <= window_size && !cursor_->Retreat(true)) {
    size_t entry_num_bytes = cursor_->numCurrentBytes();
    num_bytes_sum += entry_num_bytes;
    ++num_retreat;
    ++num_pre_entries;
  }

  // this is possible if cursor points at seq begin
  if (cursor_->isBegin()) { cursor_->Advance(true); }

  // from then on, advance the cursor back to the original place
  // while filling entries in previous chunk into window
  for (size_t i = 0; i < num_retreat; i++) {
    if ( i < num_pre_entries ) {
      rhasher_->HashBytes(cursor_->current(), cursor_->numCurrentBytes());
      bool atChunkEnd = cursor_->Advance(false);

      if (atChunkEnd) {
        CHECK_EQ(i, num_pre_entries - 1);
      // place the cursor at start of next chunk
      // next chunk should contains the entry that
      //   the cursor originally points to
        cursor_->Advance(true);
      }
    } else {
      // previous entries in the same chunk will be
      //   hashed during commit phase
      cursor_->Advance(true);
    }  // end if
  }  // end of for

  CHECK_EQ(original_idx, cursor_->idx());
  // LOG(INFO) << "Level " << level_
  //           << "\nPre Entry in Chunk: " << entries_data_.size()
  //           << "\n# Retreat: " << num_retreat
  //           << "\nEntry in Prev Chunk: " << num_pre_entries
  //           << "\n";

  // Keep the number of bytes hashed
  rhasher_->ResetBoundary();
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

  // LOG(INFO) << "Before Skip Idx: " << cursor_->idx()
  //           << " Skip # Entry: " << num_elements;

  for (size_t i = 0; i < num_elements; i++) {
    if (!cursor_->Advance(false)) continue;

    // Here, cursor has already reached this chunk end
    //   we need to skip and remove the parent metaentry
    //     which points to current chunk
    if (parent_builder_ == nullptr ||
        parent_builder()->SkipEntries(1) == 0) {
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
      // LOG(INFO) << "Level: " << level_
      //           << " Recursive Skipping Parents. ";
      bool AtEnd = cursor_->Advance(true);
      CHECK(!AtEnd);
    }  // end if parent_chunker
  }  // end for
  num_skip_entries_ += num_elements;
  return num_elements;
}

NodeBuilder:: ~NodeBuilder() {
  delete rhasher_;
  delete cursor_;
  delete parent_builder_;
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
    LOG(WARNING) << "Actual Remove " << actual_delete
                 << " elements instead of " << num_delete;
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
                    std::vector<size_t>* chunk_entries_num_bytes,
                    MakeChunkFunc make_chunk) {
  // LOG(INFO) << "Start Handing Boundary. ";
  // LOG(INFO) << " Entry Count: " << chunk_entries_num_bytes->size();
  ChunkInfo chunk_info = make_chunk(*chunk_entries_data,
                                    *chunk_entries_num_bytes);

  const Chunk* chunk = chunk_info.first;
  const byte_t* metaentry_data = chunk_info.second.first;
  size_t metaentry_num_bytes = chunk_info.second.second;
  // LOG(INFO) << "me_num_bytes : " << metaentry_num_bytes;

  // Dump chunk into storage here
  #ifdef USE_LEVELDB
  ldb.Instance()->Put(chunk->hash(), *chunk);
  // LOG(INFO) << "Finish Dumping Chunk.";
  #endif  // USE_LEVELDB

  parent_builder()->AppendEntry(metaentry_data, metaentry_num_bytes);
  // LOG(INFO) << "Finish Appending Entry for parent builder.";
  // CHECK(rhasher_->CrossedBoundary());
  rhasher_->ClearLastBoundary();

  for ( const byte_t* chunk_entry_data : *chunk_entries_data ) {
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
  //         LOG(INFO) << "Level: " << level_
  //                   << " Skipping Parents At Start. ";
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

  size_t window_size = rhasher_->window_size();
  for (size_t i = 0; i < num_entries; i++) {
    const byte_t* entry_data = entries_data_.front();
    size_t entry_num_bytes = entries_num_bytes_.front();

    entries_data_.pop_front();
    entries_num_bytes_.pop_front();

    chunk_entries_data.push_back(entry_data);
    chunk_entries_num_bytes.push_back(entry_num_bytes);

    rhasher_->HashBytes(entry_data, entry_num_bytes);
    // if we are hashing previous entries in the same chunk
    //   we don't bother detecting boundary pattern
    if (rhasher_->byte_hashed() < window_size) {
      rhasher_->ResetBoundary();
    }

    if (rhasher_->CrossedBoundary()) {
      // LOG(INFO) << "Handle Boundary Here";
      last_created_chunk = HandleBoundary(&chunk_entries_data,
                                          &chunk_entries_num_bytes,
                                          make_chunk);
    }  // end if
  }  // end for
  // LOG(INFO) << "Append " << num_entries << " entries. ";
  // Second, combining original entries pointed by current cursor
  //   to make chunk

  // number of bytes hashed into rhaser since the last appended entry

  if (cursor_ != nullptr && !cursor_->isEnd()) {
    size_t num_hashed_bytes = 0;
    size_t num_advance = 0;
    do {
      if (num_hashed_bytes < window_size - 1) {
        // the rolling hash of current entry
        //   may also still be affected by appended entries

        if (num_advance > 0 && cursor_->idx() == 0) {
          // cursor has advanced to the next chunk
          //  skip to replace the parent builder's metaentry
          // LOG(INFO) << "Level: " << level_
          //           << " Commit Skipping Parents. ";
          parent_builder()->SkipEntries(1);
          // CHECK_EQ(actual, 1);
        }
      } else {
        // cursor has reached the start of next chunk outside
        //   the influence window of last appended entries
        // Hence this is the common boundary between the new and old sequence
        //   break the do-while loop
        if (num_advance > 0 && cursor_->idx() == 0)  break;
      }

      size_t entry_num_bytes = cursor_->numCurrentBytes();

      // XXX(wangji): again really need to copy?
      byte_t* entry_data = new byte_t[entry_num_bytes];
      memcpy(entry_data, cursor_->current(), entry_num_bytes);

      chunk_entries_data.push_back(entry_data);
      chunk_entries_num_bytes.push_back(entry_num_bytes);

      rhasher_->HashBytes(cursor_->current(),
                          entry_num_bytes);

      num_hashed_bytes += entry_num_bytes;

      // Create Chunk and append metaentries to upper builders
      //   if detecing boundary
      if (rhasher_->CrossedBoundary()) {
        last_created_chunk = HandleBoundary(&chunk_entries_data,
                                            &chunk_entries_num_bytes,
                                            make_chunk);
      }  // end if
      ++num_advance;
    } while (!cursor_->Advance(true));
  }  // end if
  // LOG(INFO) << "Finish detecting boundary. ";

  if (chunk_entries_data.size() > 0) {
    // this could happen if the last entry of sequence
    // cannot form a boundary, we still need to make a explicit chunk
    // and append a metaentry to upper builder
    CHECK(cursor_ == nullptr || (cursor_ != nullptr &&
                                 cursor_->isEnd() &&
                                 cursor_->Advance(true)));

    last_created_chunk = HandleBoundary(&chunk_entries_data,
                                        &chunk_entries_num_bytes,
                                        make_chunk);
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


