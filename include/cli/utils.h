// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLI_UTILS_H_
#define USTORE_CLI_UTILS_H_

#include "utils/utils.h"

namespace ustore {
namespace cli {

class Utils : public ::ustore::Utils {
 public:
  template<class T>
  static inline std::string ToStringWithQuote(const std::vector<T>& vec) {
    return ToStringSeq(vec.cbegin(), vec.cend(), "[", "]", ", ", true);
  }

 private:
};

}  // namespace cli
}  // namespace ustore

#endif  // USTORE_CLI_UTILS_H_
