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
#include "utils/utils.h"

namespace ustore {

Benchmark::Benchmark(const std::vector<ObjectDB*>& dbs)
  : dbs_(dbs), num_threads_(dbs.size()), profiler_(num_threads_) {
  LoadParameters();
  BENCHMARK_HANDLER("ALL", RunAll());
  BENCHMARK_HANDLER("PUT", Put(Utils::ToUType(BenchmarkConfig::type)));
  BENCHMARK_HANDLER("GET", Get(Utils::ToUType(BenchmarkConfig::type)));
  BENCHMARK_HANDLER("BRANCH", Branch());
  BENCHMARK_HANDLER("MERGE", Merge());
}

void Benchmark::Run() {
  auto& cmd = BenchmarkConfig::command;
  auto it_cmd_exec = cmd_exec_.find(cmd);
  if (it_cmd_exec == cmd_exec_.end()) {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Unknown command: " << cmd << std::endl;
  } else {
    it_cmd_exec->second();
  }
}

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

void Benchmark::HeaderInfo(const std::string& cmd, UType type, size_t ops,
                           size_t length, size_t elements, const std::string& key) {
  std::cout << BOLD_RED("[" << cmd << "]");
  if (type != UType::kUnknown) std::cout << " type=" << BOLD_RED(type);
  std::cout << " ops=" << ops;
  if (length) std::cout << " bytes=(" << length << "*" << elements << ")";
  std::cout << " key=\"" << BOLD_RED(key)
            << (kSuffix ? "[0-" + std::to_string(kSuffixRange) + ")" : "")
            << "\"" << std::endl;
}

void Benchmark::FooterInfo(const std::string& cmd, UType type,
                           size_t total_time, size_t pk_tp, size_t avg_tp) {
  std::cout << BOLD_GREEN("[" << cmd);
  if (type != UType::kUnknown) std::cout << BOLD_GREEN(" " << type);
  std::cout << BOLD_GREEN("]")
            << " Elapsed Time: " << BOLD_GREEN(total_time << " ms")
            << std::endl
            << "\tPeak Throughput: " << BOLD_BLUE(pk_tp << " ops/s")
            << std::endl
            << "\tAverage Throughput: " << BOLD_BLUE(avg_tp << " ops/s")
            << std::endl;
}

void Benchmark::LoadParameters() {
  BenchParam param;
  // Common
  kValidateOps = BenchmarkConfig::validate_ops;
  kDefaultBranch = BenchmarkConfig::default_branch;
  kSuffix = BenchmarkConfig::suffix;
  kSuffixRange = BenchmarkConfig::suffix_range;
  // String
  param.ops = BenchmarkConfig::string_ops;
  param.length = BenchmarkConfig::string_length;
  param.elements = 1;
  param.key = BenchmarkConfig::string_key;
  params_.emplace(UType::kString, param);
  // Blob
  param.ops = BenchmarkConfig::blob_ops;
  param.length = BenchmarkConfig::blob_length;
  param.elements = 1;
  param.key = BenchmarkConfig::blob_key;
  params_.emplace(UType::kBlob, param);
  // List
  param.ops = BenchmarkConfig::list_ops;
  param.length = BenchmarkConfig::list_length;
  param.elements = BenchmarkConfig::list_elements;
  param.key = BenchmarkConfig::list_key;
  params_.emplace(UType::kList, param);
  // Map
  param.ops = BenchmarkConfig::map_ops;
  param.length = BenchmarkConfig::map_length;
  param.elements = BenchmarkConfig::map_elements;
  param.key = BenchmarkConfig::map_key;
  params_.emplace(UType::kMap, param);
  // Branch
  kBranchOps = BenchmarkConfig::branch_ops;
  kBranchKey = BenchmarkConfig::branch_key;
  // Merge
  kMergeOps = BenchmarkConfig::merge_ops;
  kMergeKey = BenchmarkConfig::merge_key;

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
  auto key = params_[type].key;
  if (validate) ops = kValidateOps;
  if (validate && !ops) return;
  HeaderInfo(validate ? "Validate" : "Put", type, ops, length, elements,
             key);
  // generate key
  auto keys = kSuffix ? rg_.PrefixSeqString(key, ops, kSuffixRange)
              : std::vector<std::string>(ops, key);
  auto branch = validate ? "validate" : kDefaultBranch;
  StrVecVec values(ops);
  // generate value
  for (size_t i = 0; i < ops; ++i)
    values[i] = rg_.NFixedString(elements, length);
  ExecPut(type, keys, branch, values, validate);
}

void Benchmark::Get(UType type) {
  auto ops = params_[type].ops;
  auto key = params_[type].key;
  HeaderInfo("Get", type, ops, 0, 0, key);
  // generate key
  auto keys = kSuffix ? rg_.PrefixSeqString(key, ops, kSuffixRange)
              : std::vector<std::string>(ops, key);
  auto branch = kDefaultBranch;
  ExecGet(type, keys, branch, false);
  ExecGet(type, keys, branch, true);
}

void Benchmark::Branch() {
  auto ops = kBranchOps;
  auto key = kBranchKey;
  HeaderInfo("Branch", UType::kUnknown, ops, 0, 0, key);
  // generate key
  auto keys = kSuffix ? rg_.PrefixSeqString(key, ops, kSuffixRange)
              : std::vector<std::string>(ops, key);
  auto branch = kDefaultBranch;
  // generate branches
  auto branches = rg_.PrefixSeqString(branch, ops, 100000000);
  ExecBranch(keys, branch, branches);
}

void Benchmark::Merge() {
  auto ops = kMergeOps;
  auto key = kMergeKey;
  HeaderInfo("Merge", UType::kUnknown, ops, 0, 0, key);
  // generate key
  auto keys = kSuffix ? rg_.PrefixSeqString(key, ops, kSuffixRange)
              : std::vector<std::string>(ops, key);
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
  if (!validate) FooterInfo("Put", type, total_time, pk_tp, avg_tp);
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
  FooterInfo((scan ? "Get" : "Get Meta"), type, total_time, pk_tp, avg_tp);
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
  FooterInfo("Branch", UType::kUnknown, total_time, pk_tp, avg_tp);
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
  FooterInfo("Merge", UType::kUnknown, total_time, pk_tp, avg_tp);
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
