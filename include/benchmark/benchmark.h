// Copyright (c) 2017 The Ustore Authors.
#ifndef USTORE_BENCHMARK_BENCHMARK_H_
#define USTORE_BENCHMARK_BENCHMARK_H_

#include <string>
#include <thread>
#include <vector>
#include "benchmark/bench_config.h"
#include "benchmark/bench_utils.h"
#include "spec/object_db.h"
#include "spec/slice.h"

namespace ustore {

struct BenchParam {
  size_t ops;
  size_t length;
  size_t elements;
  std::string prefix;
};

class Benchmark {
 public:
  explicit Benchmark(const std::vector<ObjectDB*>& dbs)
    : dbs_(dbs) {
  //    kNumValidations(BenchmarkConfig::num_validations),
  //    kNumStrings(BenchmarkConfig::num_strings),
  //    kNumBlobs(BenchmarkConfig::num_blobs),
  //    kStringLength(BenchmarkConfig::string_len),
  //    kBlobSize(BenchmarkConfig::blob_size) {
    LoadParameters();
    num_threads_ = dbs.size();
  }
  ~Benchmark() = default;

  void RunAll();
  void Put(UType type, bool validate);
  void Get(UType type);
  void Branch();
  void Merge();

 private:
  using StrVec = std::vector<std::string>;
  using StrVecVec = std::vector<std::vector<std::string>>;
  using SliceVec = std::vector<Slice>;
  using SliceVecVec = std::vector<std::vector<Slice>>;

  void LoadParameters();
  void ExecPut(UType type, const StrVec& keys, const std::string& branch,
               const StrVecVec& values, bool validate);
  void ExecGet(UType type, const StrVec& keys, const std::string& branch,
               bool scan);
  void ExecBranch(const StrVec& keys, const std::string& ref_branch,
                  const StrVec& branches);
  void ExecMerge(const StrVec& keys, const std::string& ref_branch,
                 const StrVec& branches);
  void ThreadPut(ObjectDB* db, UType type, const SliceVec& keys,
                 const Slice& branch, const SliceVecVec& values, bool validate);
  void ThreadGet(ObjectDB* db, UType type, const SliceVec& keys,
                 const Slice& branch, bool scan);
  void ThreadBranch(ObjectDB* db, const SliceVec& keys,
                    const Slice& ref_branch, const SliceVec& branches);
  void ThreadMerge(ObjectDB* db, const SliceVec& keys,
                   const Slice& ref_branch, const SliceVec& branches);

  size_t kValidateOps = 10;
  size_t kBranchOps = 1000;
  size_t kMergeOps = 1000;
  bool kSuffix = true;
  size_t kSuffixRange = 100;
  std::string kDefaultBranch;
  std::string kBranchPrefix;
  std::string kMergePrefix;

  std::map<UType, BenchParam> params_;
  std::vector<ObjectDB*> dbs_;
  std::vector<std::string> subkeys_;
  std::vector<Slice> subkeys_slice_;
  Timer timer_;
  RandomGenerator rg_;
  size_t num_threads_;
};
}  // namespace ustore

#endif  // USTORE_BENCHMARK_BENCHMARK_H_
