// Copyright (c) 2017 The Ustore Authors.

#include "ca/utils.h"

namespace ustore {
namespace example {
namespace ca {

const int Utils::kKeyPrintWidth = 16;
const int Utils::kBrnachPrintWidth = 8;

void Utils::PrintListDiff(DuallyDiffIndexIterator& it) {
  auto f_print_entry = [&it]() {
    std::cout << it.index() << ":(";
    auto lhs = it.lhs_value();
    std::cout << (lhs.empty() ? "_" : lhs.ToString()) << ',';
    auto rhs = it.rhs_value();
    std::cout << (rhs.empty() ? "_" : rhs.ToString()) << ')';
  };
  std::cout << "[";
  if (!it.end()) {
    f_print_entry();
    for (it.next(); !it.end(); it.next()) {
      std::cout << ", ";
      f_print_entry();
    }
  }
  std::cout << "]";
}

void Utils::Print(const std::string& table_name,
                  const std::string& branch_name,
                  const std::string& list_name, const UList& list) {
  const std::string list_key(table_name + "::" + list_name);
  std::cout << std::left << std::setw(kKeyPrintWidth) << list_key
            << " @" << std::setw(kBrnachPrintWidth) << branch_name << ": ";
  ::ustore::Utils::Print(list);
  std::cout << std::endl;
}

}  // namespace ca
}  // namespace example
}  // namespace ustore
