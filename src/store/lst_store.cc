// Copyright (c) 2017 The Ustore Authors.

#include <experimental/filesystem>

#include "utils/logging.h"
#include "store/lst_store.h"

namespace ustore {
namespace lst_store {

const Chunk* LSTStore::Get(const Hash& key) {
    LSTHash hash(key.value());
    CHECK(this->chunk_map_.count(hash) == 1);
    return chunk_map_[hash].toChunk();
}
    
} /* namespace lst_store */
} /* namespace ustore */
