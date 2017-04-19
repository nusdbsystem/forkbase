// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_ITERATOR_H_
#define USTORE_TYPES_ITERATOR_H_

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "chunk/chunk.h"
#include "hash/hash.h"
#include "node/cursor.h"
#include "node/node.h"
#include "node/map_node.h"
#include "store/chunk_loader.h"
#include "spec/slice.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {

class Iterator : private Noncopyable {
 public:
  explicit Iterator(std::unique_ptr<NodeCursor> cursor) :
      cursor_(std::move(cursor)) {}
  inline bool Advance() {return cursor_->Advance(true); }
  inline bool Retreat() {return cursor_->Retreat(true); }
  inline bool end() const {return cursor_->isEnd(); }

 protected:
  inline const Slice raw_data() const {
    CHECK(!end());
    return Slice(reinterpret_cast<const char*>(cursor_->current()),
                 cursor_->numCurrentBytes());
  }

  std::unique_ptr<NodeCursor> cursor_;
};

}  // ustore
#endif  // USTORE_TYPES_ITERATOR_H_
