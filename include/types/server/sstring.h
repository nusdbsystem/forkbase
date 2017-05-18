// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_SSTRING_H_
#define USTORE_TYPES_SERVER_SSTRING_H_

#include "types/ustring.h"

namespace ustore {

class SString : public UString {
 public:
  // Load an existing sstring
  explicit SString(const Hash& hash) noexcept;

  // Creata a new sstring
  explicit SString(const Slice& data) noexcept;

  ~SString() = default;

  SString() = default;

  SString(SString&& rhs) noexcept :
      UString(std::move(rhs)) {}

  SString& operator=(SString&& rhs) noexcept {
    UString::operator=(std::move(rhs));
    return *this;
  }
};

}  // namespace ustore

#endif  // USTORE_TYPES_SERVER_SSTRING_H_
