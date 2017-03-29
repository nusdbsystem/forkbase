// Copyright (c) 2017 The Ustore Authors.

#include "worker/head_version.h"

namespace ustore {

const Hash& HeadVersion::Get(const Slice& key, const Slice& branch) const {
  static Hash hash;
  return hash;
}

void HeadVersion::Put(const Slice& key, const Slice& branch,
                      const Hash& version) {
}

}  // namespace ustore
