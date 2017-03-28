

#include "types/ublob.h"

#include <cstring>
#include "node/node_builder.h"
#include "node/blob_node.h"
#include "node/chunker.h"
#include "utils/logging.h"
#include "utils/singleton.h"

namespace ustore {
const UBlob* UBlob::Load(const Hash& root_hash) {
  std::shared_ptr<ChunkLoader> loader(new ChunkLoader());
  const Chunk* root_chunk = loader->Load(root_hash);
  return new UBlob(root_chunk, loader);
}

const UBlob* UBlob::Create(const byte_t* data, size_t num_bytes) {
  std::shared_ptr<ChunkLoader> loader(new ChunkLoader());
  NodeBuilder nb = NodeBuilder(Singleton<BlobChunker>::Instance(), true);

  FixedSegment seg(data, num_bytes, 1);
  nb.SpliceElements(0, &seg);
  const Chunk* root_chunk = nb.Commit();
  return new UBlob(root_chunk, loader);
}

UBlob::UBlob(const Chunk* chunk, std::shared_ptr<ChunkLoader> loader)
    : chunk_loader_(loader) {
  if (chunk->type() == ChunkType::kMeta) {
    root_node_ = new MetaNode(chunk);
  } else if (chunk->type() == ChunkType::kBlob) {
    root_node_ = new BlobNode(chunk);
  } else {
    LOG(FATAL) << "Cannot be other chunk type for Ublob.";
  }
}

const UBlob* UBlob::Splice(size_t pos, size_t num_delete, const byte_t* data,
                           size_t num_append) const {
  NodeBuilder* nb = NodeBuilder::NewNodeBuilderAtIndex(
      root_node_->hash(), pos, chunk_loader_.get(),
      Singleton<BlobChunker>::Instance(), true);

  FixedSegment seg(data, num_append, 1);
  nb->SpliceElements(num_delete, &seg);

  const Chunk* result_chunk = nb->Commit();
  return new UBlob(result_chunk, chunk_loader_);
}

const UBlob* UBlob::Insert(size_t pos, const byte_t* data,
                           size_t num_insert) const {
  return Splice(pos, 0, data, num_insert);
}

const UBlob* UBlob::Delete(size_t pos, size_t num_delete) const {
  return Splice(pos, num_delete, nullptr, 0);
}

const UBlob* UBlob::Append(byte_t* data, size_t num_append) const {
  return Splice(size(), 0, data, num_append);
}

size_t UBlob::Read(size_t pos, size_t len, byte_t* buffer) const {
  if (pos >= size()) {
    LOG(WARNING) << "Read Pos exceeds Blob Size. ";
    return 0;
  }
  NodeCursor* cursor = NodeCursor::GetCursorByIndex(root_node_->hash(), pos,
                                                    chunk_loader_.get());
  size_t total_copy_byte = 0;
  do {
    size_t pre_copy_byte = total_copy_byte;
    size_t chunk_copy_byte = 0;
    const byte_t* chunk_copy_start = cursor->current();
    do {
      chunk_copy_byte += cursor->numCurrentBytes();
      total_copy_byte += cursor->numCurrentBytes();
      if (total_copy_byte == len) break;
    } while (cursor->Advance(false));
    std::memcpy(buffer + pre_copy_byte, chunk_copy_start, chunk_copy_byte);
    if (total_copy_byte == len) break;
  } while (cursor->Advance(true));

  delete cursor;
  return total_copy_byte;
}

}  // namespace ustore
