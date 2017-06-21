// Copyright (c) 2017 The Ustore Authors.
#ifndef USTORE_BENCHMARK_BENCHMARK_H_
#define USTORE_BENCHMARK_BENCHMARK_H_

#include <map>
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
  std::string key;
};

class Benchmark {
 public:
  explicit Benchmark(const std::vector<ObjectDB*>& dbs)
      : dbs_(dbs), num_threads_(dbs.size()), profiler_(num_threads_) {
    LoadParameters();
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
  void HeaderInfo(const std::string& cmd, UType type, size_t ops, size_t length,
      size_t elements, const std::string& key);
  void FooterInfo(const std::string& cmd, UType type, size_t total_time,
      size_t pk_tp, size_t avg_tp);
  void ExecPut(UType type, const StrVec& keys, const std::string& branch,
               const StrVecVec& values, bool validate);
  void ExecGet(UType type, const StrVec& keys, const std::string& branch,
               bool scan);
  void ExecBranch(const StrVec& keys, const std::string& ref_branch,
                  const StrVec& branches);
  void ExecMerge(const StrVec& keys, const std::string& ref_branch,
                 const StrVec& branches);
  void ThreadPut(ObjectDB* db, UType type, const SliceVec& keys,
                 const Slice& branch, const SliceVecVec& values, bool validate,
                 size_t tid);
  void ThreadGet(ObjectDB* db, UType type, const SliceVec& keys,
                 const Slice& branch, bool scan, size_t tid);
  void ThreadBranch(ObjectDB* db, const SliceVec& keys, const Slice& ref_branch,
                    const SliceVec& branches, size_t tid);
  void ThreadMerge(ObjectDB* db, const SliceVec& keys, const Slice& ref_branch,
                   const SliceVec& branches, size_t tid);

  size_t kValidateOps = 10;
  size_t kBranchOps = 1000;
  size_t kMergeOps = 1000;
  bool kSuffix = true;
  size_t kSuffixRange = 100;
  std::string kDefaultBranch;
  std::string kBranchKey;
  std::string kMergeKey;

  std::map<UType, BenchParam> params_;
  std::vector<ObjectDB*> dbs_;
  std::vector<std::string> subkeys_;
  std::vector<Slice> subkeys_slice_;
  Timer timer_;
  RandomGenerator rg_;
  size_t num_threads_;
  Profiler profiler_;
};
}  // namespace ustore

#endif  // USTORE_BENCHMARK_BENCHMARK_H_
