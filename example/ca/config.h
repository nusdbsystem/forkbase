// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_EXAMPLE_CA_CONFIG_H_
#define USTORE_EXAMPLE_CA_CONFIG_H_

#include <list>
#include <map>
#include <string>
#include "worker/worker.h"

namespace ustore {
namespace example {
namespace ca {

#define MAP_TYPE std::map
#define LIST_TYPE std::list

using KeyType = std::string;
using DataType = std::string;
using ColumnType = LIST_TYPE<DataType>;
using TableType = MAP_TYPE<KeyType, ColumnType>;

class Config {
 public:
  static const WorkerID kWorkID;
  static const size_t kNumSimpleColumns;
  static const size_t kNumSimpleRecords;
};

} // namespace ca
} // namespace example
} // namespace ustore

#endif // USTORE_EXAMPLE_CA_CONFIG_H_
