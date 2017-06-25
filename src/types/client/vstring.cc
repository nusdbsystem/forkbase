// Copyright (c) 2017 The Ustore Authors.

#include "types/client/vstring.h"

namespace ustore {

VString::VString(const Slice& data) noexcept {
  buffer_ = {UType::kString, {}, 0, 0, {data}, {}};
}

VString::VString(const UCell& cell) noexcept : UString(cell) {
  buffer_ = {UType::kString, {}, 0, 0, {slice()}, {}};
}

}  // namespace ustore
