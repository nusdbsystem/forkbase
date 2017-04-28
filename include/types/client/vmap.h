// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VMAP_H_
#define USTORE_TYPES_CLIENT_VMAP_H_

#include "types/umap.h"

namespace ustore {
class VMap : public UMap, private Noncopyable {
// UMap for server side
 public:
  // // Create an existing map using hash
  // explicit VMap(const Hash& root_hash) noexcept;

  // create an empty map
  // construct chunk loader for client
  VMap() {}

  ~VMap() = default

  // // Send message to server
  // VMap Set(const byte_t* key, size_t key_size,
  //          const byte_t* value, size_t value_size);
};
}  // namespace ustore

#endif  // USTORE_TYPES_CLIENT_VMAP_H_
