// Copyright (c) 2017 The Ustore Authors.

#include <future>
#include <iterator>
#include <iostream>
#include <iomanip>
#include <vector>
#include "benchmark/benchmark.h"
#include "types/client/vblob.h"
#include "types/client/vstring.h"
#include "utils/logging.h"

namespace ustore {

inline std::vector<Slice> ToSlice(std::vector<std::string> vec) {
  std::vector<Slice> res(vec.size());
  for (size_t i = 0; i < vec.size(); ++i) res[i] = Slice(vec[i]);
  return res;
}

inline std::vector<std::vector<Slice>> ToSlice(
    std::vector<std::vector<std::string>> vec) {
  std::vector<std::vector<Slice>> res(vec.size());
  for (size_t i = 0; i < vec.size(); ++i) res[i] = ToSlice(vec[i]);
  return res;
}

template <typename T>
inline std::vector<std::vector<T>> vec_split(const std::vector<T>& vec,
                                             size_t n) {
  size_t s_subvec = vec.size() / n;
  std::vector<std::vector<T>> ret;
  for (size_t i = 0; i < n - 1; ++i) {
    ret.emplace_back(vec.begin() + i * s_subvec,
                     vec.begin() + (i + 1) * s_subvec);
  }
  ret.emplace_back(vec.begin() + (n - 1) * s_subvec, vec.end());
  return ret;
}

void Benchmark::LoadParameters() {
  BenchParam param;
  // Common
  kValidateOps = 10;
  kDefaultBranch = "master";
  kSuffix = true;
  kSuffixRange = 100;
  // String
  param.ops = 5000;
  param.length = 128;
  param.elements = 1;
  param.prefix = "String";
  params_.emplace(UType::kString, param);
  // Blob
  param.ops = 100;
  param.length = 16 * 1024;
  param.elements = 1;
  param.prefix = "Blob";
  params_.emplace(UType::kBlob, param);
  // List
  param.ops = 100;
  param.length = 64;
  param.elements = 256;
  param.prefix = "List";
  params_.emplace(UType::kList, param);
  // Map
  param.ops = 100;
  param.length = 64;
  param.elements = 256;
  param.prefix = "Map";
  params_.emplace(UType::kMap, param);
  // Branch
  kBranchOps = 1000;
  kBranchPrefix = "Blob";
  // Merge
  kMergeOps = 1000;
  kMergePrefix = "Blob";

  // Init subkeys for Map
  subkeys_ = rg_.PrefixSeqString("k", params_[UType::kMap].elements, 100000000);
  std::sort(subkeys_.begin(), subkeys_.end());
  subkeys_slice_ = ToSlice(subkeys_);
}

void Benchmark::RunAll() {
  // Validate
  Put(UType::kString, true);
  Put(UType::kBlob, true);
  Put(UType::kList, true);
  Put(UType::kMap, true);
  // String
  Put(UType::kString, false);
  Get(UType::kString);
  // Blob
  Put(UType::kBlob, false);
  Get(UType::kBlob);
  // List
  Put(UType::kList, false);
  Get(UType::kList);
  // Map
  Put(UType::kMap, false);
  Get(UType::kMap);
  // Branch
  Branch();
  Merge();
}

void Benchmark::Put(UType type, bool validate) {
  auto ops = params_[type].ops;
  auto length = params_[type].length;
  auto elements = params_[type].elements;
  auto prefix = params_[type].prefix;
  if (validate) ops = kValidateOps;
  std::cout << GREEN((validate ? "[Validate]" : "[Put]"))
            << " type: " << GREEN(type) << " ops: " << ops << " bytes: ("
            << length << "*" << elements << ") key: \"" << GREEN(prefix)
            << (kSuffix ? "[0-" + std::to_string(kSuffixRange) + ")" : "")
            << "\"" << std::endl;
  // generate key
  auto keys = kSuffix ? rg_.PrefixSeqString(prefix, ops, kSuffixRange)
                      : std::vector<std::string>(ops, prefix);
  auto branch = validate ? "validate" : kDefaultBranch;
  StrVecVec values(ops);
  // generate value
  for (size_t i = 0; i < ops; ++i)
    values[i] = rg_.NFixedString(elements, length);
  ExecPut(type, keys, branch, values, validate);
}

void Benchmark::Get(UType type) {
  auto ops = params_[type].ops;
  auto prefix = params_[type].prefix;
  std::cout << GREEN("[Get]") << " type: " << GREEN(type) << " ops: " << ops
            << " key: \"" << GREEN(prefix)
            << (kSuffix ? "[0-" + std::to_string(kSuffixRange) + ")" : "")
            << "\"" << std::endl;
  // generate key
  auto keys = kSuffix ? rg_.PrefixSeqString(prefix, ops, kSuffixRange)
                      : std::vector<std::string>(ops, prefix);
  auto branch = kDefaultBranch;
  ExecGet(type, keys, branch, false);
  ExecGet(type, keys, branch, true);
}

void Benchmark::Branch() {
  auto ops = kBranchOps;
  auto prefix = kBranchPrefix;
  std::cout << GREEN("[Branch]") << " ops: " << ops << " key: \""
            << GREEN(prefix)
            << (kSuffix ? "[0-" + std::to_string(kSuffixRange) + ")" : "")
            << "\"" << std::endl;
  // generate key
  auto keys = kSuffix ? rg_.PrefixSeqString(prefix, ops, kSuffixRange)
                      : std::vector<std::string>(ops, prefix);
  auto branch = kDefaultBranch;
  // generate branches
  auto branches = rg_.PrefixSeqString(branch, ops, 100000000);
  ExecBranch(keys, branch, branches);
}

void Benchmark::Merge() {
  auto ops = kMergeOps;
  auto prefix = kMergePrefix;
  std::cout << GREEN("[Merge]") << " ops: " << ops << " key: \""
            << GREEN(prefix)
            << (kSuffix ? "[0-" + std::to_string(kSuffixRange) + ")" : "")
            << "\"" << std::endl;
  // generate key
  auto keys = kSuffix ? rg_.PrefixSeqString(prefix, ops, kSuffixRange)
                      : std::vector<std::string>(ops, prefix);
  auto branch = kDefaultBranch;
  // generate branches
  auto branches = rg_.PrefixSeqString(branch, ops, 100000000);
  ExecMerge(keys, branch, branches);
}

void Benchmark::ExecPut(UType type, const StrVec& keys,
                        const std::string& branch, const StrVecVec& values,
                        bool validate) {
  Timer timer;
  auto split_keys = vec_split(ToSlice(keys), num_threads_);
  auto split_branch = Slice(branch);
  auto split_vals = vec_split(ToSlice(values), num_threads_);
  profiler_.Clear();
  std::thread profiler_th(&Profiler::SamplerThread, &profiler_);
  timer.Reset();
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads_; ++i)
    threads.emplace_back(&Benchmark::ThreadPut, this, dbs_[i], type,
                         split_keys[i], split_branch, split_vals[i], validate,
                         i);
  for (auto& t : threads) t.join();
  auto total_time = timer.Elapse();
  profiler_.Terminate();
  profiler_th.join();
  double avg_tp = keys.size() * 1000.0 / total_time;
  auto pk_tp = profiler_.PeakThroughput();
  std::cout << YELLOW((validate ? "[Validate] " : "[Put] ")) << type
            << " Time: " << YELLOW(total_time) << YELLOW(" ms") << std::endl
            << YELLOW("\tPeak Throughput: ") << static_cast<unsigned>(pk_tp)
            << YELLOW(" ops/s") << std::endl
            << YELLOW("\tAverage Throughput: ") << static_cast<unsigned>(avg_tp)
            << YELLOW(" ops/s") << std::endl;
}

void Benchmark::ExecGet(UType type, const StrVec& keys,
                        const std::string& branch, bool scan) {
  Timer timer;
  auto split_keys = vec_split(ToSlice(keys), num_threads_);
  auto split_branch = Slice(branch);
  profiler_.Clear();
  std::thread profiler_th(&Profiler::SamplerThread, &profiler_);
  timer.Reset();
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads_; ++i)
    threads.emplace_back(&Benchmark::ThreadGet, this, dbs_[i], type,
                         split_keys[i], split_branch, scan, i);
  for (auto& t : threads) t.join();
  auto total_time = timer.Elapse();
  profiler_.Terminate();
  profiler_th.join();
  double avg_tp = keys.size() * 1000.0 / total_time;
  auto pk_tp = profiler_.PeakThroughput();
  std::cout << YELLOW("[Get") << YELLOW((scan ? "] " : " Meta] ")) << type
            << " Time: " << YELLOW(total_time) << YELLOW(" ms") << std::endl
            << YELLOW("\tPeak Throughput: ") << static_cast<unsigned>(pk_tp)
            << YELLOW(" ops/s") << std::endl
            << YELLOW("\tAverage Throughput: ") << static_cast<unsigned>(avg_tp)
            << YELLOW(" ops/s") << std::endl;
}

void Benchmark::ExecBranch(const StrVec& keys, const std::string& ref_branch,
                           const StrVec& branches) {
  Timer timer;
  auto split_keys = vec_split(ToSlice(keys), num_threads_);
  auto split_branch = Slice(ref_branch);
  auto split_branches = vec_split(ToSlice(branches), num_threads_);
  profiler_.Clear();
  std::vector<std::thread> threads;
  std::thread profiler_th(&Profiler::SamplerThread, &profiler_);
  timer.Reset();
  for (size_t i = 0; i < num_threads_; ++i)
    threads.emplace_back(&Benchmark::ThreadBranch, this, dbs_[i], split_keys[i],
                         split_branch, split_branches[i], i);
  for (auto& t : threads) t.join();
  auto total_time = timer.Elapse();
  profiler_.Terminate();
  profiler_th.join();
  double avg_tp = keys.size() * 1000.0 / total_time;
  auto pk_tp = profiler_.PeakThroughput();
  std::cout << YELLOW("[Branch]") << " Time: " << YELLOW(total_time)
            << YELLOW(" ms") << std::endl
            << YELLOW("\tPeak Throughput: ") << static_cast<unsigned>(pk_tp)
            << YELLOW(" ops/s") << std::endl
            << YELLOW("\tAverage Throughput: ") << static_cast<unsigned>(avg_tp)
            << YELLOW(" ops/s") << std::endl;
}

void Benchmark::ExecMerge(const StrVec& keys, const std::string& ref_branch,
                          const StrVec& branches) {
  Timer timer;
  auto split_keys = vec_split(ToSlice(keys), num_threads_);
  auto split_branch = Slice(ref_branch);
  auto split_branches = vec_split(ToSlice(branches), num_threads_);
  profiler_.Clear();
  std::vector<std::thread> threads;
  std::thread profiler_th(&Profiler::SamplerThread, &profiler_);
  timer.Reset();
  for (size_t i = 0; i < num_threads_; ++i)
    threads.emplace_back(&Benchmark::ThreadMerge, this, dbs_[i], split_keys[i],
                         split_branch, split_branches[i], i);
  for (auto& t : threads) t.join();
  auto total_time = timer.Elapse();
  profiler_.Terminate();
  profiler_th.join();
  double avg_tp = keys.size() * 1000.0 / total_time;
  auto pk_tp = profiler_.PeakThroughput();
  std::cout << YELLOW("[Merge]") << " Time: " << YELLOW(total_time)
            << YELLOW(" ms") << std::endl
            << YELLOW("\tPeak Throughput: ") << static_cast<unsigned>(pk_tp)
            << YELLOW(" ops/s") << std::endl
            << YELLOW("\tAverage Throughput: ") << static_cast<unsigned>(avg_tp)
            << YELLOW(" ops/s") << std::endl;
}

void Benchmark::ThreadPut(ObjectDB* db, UType type, const SliceVec& keys,
                          const Slice& branch, const SliceVecVec& values,
                          bool validate, size_t tid) {
  switch (type) {
    case UType::kString:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto ver = db->Put(keys[i], VString(values[i][0]), branch);
        profiler_.IncCounter(tid);
        if (!validate) continue;
        auto val = db->Get(keys[i], ver.value).value.String();
        CHECK_EQ(values[i][0], val.slice());
      }
      break;
    case UType::kBlob:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto ver = db->Put(keys[i], VBlob(values[i][0]), branch);
        profiler_.IncCounter(tid);
        if (!validate) continue;
        auto val = db->Get(keys[i], ver.value).value.Blob();
        byte_t* buf = new byte_t[val.size()];
        val.Read(0, val.size(), buf);
        CHECK_EQ(values[i][0], Slice(buf, val.size()));
        delete[] buf;
      }
      break;
    case UType::kList:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto ver = db->Put(keys[i], VList(values[i]), branch);
        profiler_.IncCounter(tid);
        if (!validate) continue;
        auto val = db->Get(keys[i], ver.value).value.List();
        auto it = val.Scan();
        for (auto& slice : values[i]) {
          CHECK_EQ(slice, it.value());
          it.next();
        }
      }
      break;
    case UType::kMap:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto ver = db->Put(keys[i], VMap(subkeys_slice_, values[i]), branch);
        profiler_.IncCounter(tid);
        if (!validate) continue;
        auto val = db->Get(keys[i], ver.value).value.Map();
        auto it = val.Scan();
        for (auto& slice : values[i]) {
          CHECK_EQ(slice, it.value());
          it.next();
        }
      }
      break;
    default:
      LOG(FATAL) << "Unsupported type: " << type;
  }
}

void Benchmark::ThreadGet(ObjectDB* db, UType type, const SliceVec& keys,
                          const Slice& branch, bool scan, size_t tid) {
  switch (type) {
    case UType::kString:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto meta = db->Get(keys[i], branch);
        if (scan) {
          auto val = meta.value.String();
        }
        profiler_.IncCounter(tid);
      }
      break;
    case UType::kBlob:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto meta = db->Get(keys[i], branch);
        if (scan) {
          auto val = meta.value.Blob();
          for (auto it = val.ScanChunk(); !it.end(); it.next()) it.value();
        }
        profiler_.IncCounter(tid);
      }
      break;
    case UType::kList:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto meta = db->Get(keys[i], branch);
        if (scan) {
          auto val = meta.value.List();
          for (auto it = val.Scan(); !it.end(); it.next()) it.value();
        }
        profiler_.IncCounter(tid);
      }
      break;
    case UType::kMap:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto meta = db->Get(keys[i], branch);
        if (scan) {
          auto val = meta.value.Map();
          for (auto it = val.Scan(); !it.end(); it.next()) it.value();
        }
        profiler_.IncCounter(tid);
      }
      break;
    default:
      LOG(FATAL) << "Unsupported type: " << type;
  }
}

void Benchmark::ThreadBranch(ObjectDB* db, const SliceVec& keys,
                             const Slice& ref_branch, const SliceVec& branches,
                             size_t tid) {
  for (size_t i = 0; i < keys.size(); ++i) {
    auto res = db->Branch(keys[i], ref_branch, branches[i]);
    profiler_.IncCounter(tid);
  }
}

void Benchmark::ThreadMerge(ObjectDB* db, const SliceVec& keys,
                            const Slice& ref_branch, const SliceVec& branches,
                            size_t tid) {
  for (size_t i = 0; i < keys.size(); ++i) {
    auto meta = db->Get(keys[i], branches[i]);
    switch (meta.value.type()) {
      case UType::kString:
        db->Merge(keys[i], meta.value.String(), branches[i], ref_branch);
        break;
      case UType::kBlob:
        db->Merge(keys[i], meta.value.Blob(), branches[i], ref_branch);
        break;
      case UType::kList:
        db->Merge(keys[i], meta.value.List(), branches[i], ref_branch);
        break;
      case UType::kMap:
        db->Merge(keys[i], meta.value.Map(), branches[i], ref_branch);
        break;
      default:
        LOG(FATAL) << "Unsupported type: " << meta.value.type();
    }
    profiler_.IncCounter(tid);
  }
}

}  // namespace ustore
