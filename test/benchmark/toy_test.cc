// Copyright (c) 2017 The Ustore Authors.

// Please add your header file here
#include <cstring>
#include <string>
#include <sstream>
#include <utility>
#include "gtest/gtest.h"
#include "hash/hash.h"
#include "benchmark/bench_utils.h"

// Please add your global variables here
// The following line is an example
const ustore::byte_t raw_str[] = "The quick brown fox jumps over the lazy dog";

// Please add namespace here
// The following line is an example
using namespace ustore;

int main(int argc, char* argv[]) {
// Please add your own codes here

// The following block is an example
/*
  Timer t_; 
  RandomGenerator rg_;
  int n = 1000000;
  auto t = 0;
  std::string str = rg_.FixedString(5000);
  t_.Reset();
  for(int i = 0; i < n; ++i)
    // ustore::Hash::ComputeFrom(raw_str, 43);  
    ustore::Hash::ComputeFrom(str);
  t = t_.Elapse();
  std::cout << "raw_str: " << raw_str << std::endl;
  std::cout << "Throughput: " << (double) n / t * 1000 << std::endl;
*/
}
