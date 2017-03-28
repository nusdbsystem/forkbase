// Copyright (c) 2017 The Ustore Authors.

#include "worker/head_version.h"

namespace ustore {

HeadVersion*
HeadVersion::Instance() {
  static auto ins(new HeadVersion());

  return ins;
}

HeadVersion::HeadVersion() {}

HeadVersion::~HeadVersion() {}

const Hash&
HeadVersion::Get(const Slice& key,
                 const Slice& branch) const {
  return NULL;
}

void
HeadVersion::Put(const Slice& key,
                 const Slice& branch,
                 const Hash& version) {
}

} // namespace ustore
