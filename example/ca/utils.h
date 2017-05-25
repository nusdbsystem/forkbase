// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_UTILS_H_
#define USTORE_EXAMPLE_CA_UTILS_H_

#include <iomanip>
#include <iostream>
#include <string>
#include "types/uiterator.h"
#include "types/ulist.h"
#include "utils/utils.h"

namespace ustore {
namespace example {
namespace ca {

class Utils : public ::ustore::Utils {
 public:
  static void PrintList(const UList& list);

  static void PrintListDiff(DuallyDiffIndexIterator& it);

  static inline void PrintListDiff(DuallyDiffIndexIterator&& it) {
    PrintListDiff(it);
  }

  static void Print(const std::string& table_name,
                    const std::string& branch_name,
                    const std::string& list_name, const UList& list);

 private:
  static const int kKeyPrintWidth;
  static const int kBrnachPrintWidth;
};

}  // namespace ca
}  // namespace example
}  // namespace ustore

#endif  // USTORE_EXAMPLE_CA_UTILS_H_
