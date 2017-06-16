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
  Get(UType::kString, false);
  Get(UType::kString, true);
  // Blob
  Put(UType::kBlob, false);
  Get(UType::kBlob, false);
  Get(UType::kBlob, true);
  // List
  Put(UType::kList, false);
  Get(UType::kList, false);
  Get(UType::kList, true);
  // Map
  Put(UType::kMap, false);
  Get(UType::kMap, false);
  Get(UType::kMap, true);
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
  std::cout << (validate ? "Validating " : "Putting ") << ops << " "
            << type << " with size (" << length << " * "<< elements
            << ") | key = \"" << prefix
            << (kSuffix ? "[0-" + std::to_string(kSuffixRange) + ")" : "")
            << "\" ......" << std::endl;
  // generate key
  auto keys = kSuffix
            ? rg_.PrefixSeqString(prefix, ops, kSuffixRange)
            : std::vector<std::string>(ops, prefix);
  auto branch = validate ? "validate" : kDefaultBranch;
  StrVecVec values(ops);
  // generate value
  for (size_t i = 0; i < ops; ++i)
    values[i] = rg_.NFixedString(elements, length);
  ExecPut(type, keys, branch, values, validate);
}

void Benchmark::Get(UType type, bool scan) {
  auto ops = params_[type].ops;
  auto prefix = params_[type].prefix;
  std::cout << "Getting " << ops << " " << type << (scan ? "" : " Meta")
            << " | key = \"" << prefix
            << (kSuffix ? "[0-" + std::to_string(kSuffixRange) + ")" : "")
            << "\" ......" << std::endl;
  // generate key
  auto keys = kSuffix
            ? rg_.PrefixSeqString(prefix, ops, kSuffixRange)
            : std::vector<std::string>(ops, prefix);
  auto branch = kDefaultBranch;
  ExecGet(type, keys, branch, scan);
}

void Benchmark::Branch() {
  auto ops = kBranchOps;
  auto prefix = kBranchPrefix;
  std::cout << "Branching " << ops << " branches | key = \"" << prefix
            << (kSuffix ? "[0-" + std::to_string(kSuffixRange) + ")" : "")
            << "\" ......" << std::endl;
  // generate key
  auto keys = kSuffix
            ? rg_.PrefixSeqString(prefix, ops, kSuffixRange)
            : std::vector<std::string>(ops, prefix);
  auto branch = kDefaultBranch;
  // generate branches
  auto branches = rg_.PrefixSeqString(branch, ops, 100000000);
  ExecBranch(keys, branch, branches);
}

void Benchmark::Merge() {
  auto ops = kMergeOps;
  auto prefix = kMergePrefix;
  std::cout << "Merging " << ops << " branches | key = \"" << prefix
            << (kSuffix ? "[0-" + std::to_string(kSuffixRange) + ")" : "")
            << "\" ......" << std::endl;
  // generate key
  auto keys = kSuffix
            ? rg_.PrefixSeqString(prefix, ops, kSuffixRange)
            : std::vector<std::string>(ops, prefix);
  auto branch = kDefaultBranch;
  // generate branches
  auto branches = rg_.PrefixSeqString(branch, ops, 100000000);
  ExecMerge(keys, branch, branches);
}

void Benchmark::ExecPut(UType type, const StrVec& keys,
    const std::string& branch, const StrVecVec& values, bool validate) {
  Timer timer;
  auto split_keys = vec_split(ToSlice(keys), num_threads_);
  auto split_branch = Slice(branch);
  auto split_vals = vec_split(ToSlice(values), num_threads_);
  timer.Reset();
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads_; ++i)
    threads.emplace_back(&Benchmark::ThreadPut, this, dbs_[i], type,
        split_keys[i], split_branch, split_vals[i], validate);
  for (auto& t : threads) t.join();
  std::cout << (validate ? "Validate " : "Put ") << type << " Time: "
            << timer.Elapse() << " ms" << std::endl;
}

void Benchmark::ExecGet(UType type, const StrVec& keys,
    const std::string& branch, bool scan) {
  Timer timer;
  auto split_keys = vec_split(ToSlice(keys), num_threads_);
  auto split_branch = Slice(branch);
  timer.Reset();
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads_; ++i)
    threads.emplace_back(&Benchmark::ThreadGet, this, dbs_[i], type,
        split_keys[i], split_branch, scan);
  for (auto& t : threads) t.join();
  std::cout << "Get " << type << (scan ? "" : " Meta") << " Time: "
            << timer.Elapse() << " ms" << std::endl;
}

void Benchmark::ExecBranch(const StrVec& keys, const std::string& ref_branch,
                           const StrVec& branches) {
  Timer timer;
  auto split_keys = vec_split(ToSlice(keys), num_threads_);
  auto split_branch = Slice(ref_branch);
  auto split_branches = vec_split(ToSlice(branches), num_threads_);
  timer.Reset();
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads_; ++i)
    threads.emplace_back(&Benchmark::ThreadBranch, this, dbs_[i], split_keys[i],
        split_branch, split_branches[i]);
  for (auto& t : threads) t.join();
  std::cout << "Branch Time: " << timer.Elapse() << " ms" << std::endl;
}

void Benchmark::ExecMerge(const StrVec& keys, const std::string& ref_branch,
                          const StrVec& branches) {
  Timer timer;
  auto split_keys = vec_split(ToSlice(keys), num_threads_);
  auto split_branch = Slice(ref_branch);
  auto split_branches = vec_split(ToSlice(branches), num_threads_);
  timer.Reset();
  std::vector<std::thread> threads;
  for (size_t i = 0; i < num_threads_; ++i)
    threads.emplace_back(&Benchmark::ThreadMerge, this, dbs_[i], split_keys[i],
        split_branch, split_branches[i]);
  for (auto& t : threads) t.join();
  std::cout << "Merge Time: " << timer.Elapse() << " ms" << std::endl;
}

void Benchmark::ThreadPut(ObjectDB* db, UType type, const SliceVec& keys,
    const Slice& branch, const SliceVecVec& values, bool validate) {
  switch (type) {
    case UType::kString:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto ver = db->Put(keys[i], VString(values[i][0]), branch);
        if (!validate) continue;
        auto val = db->Get(keys[i], ver.value).value.String();
        CHECK_EQ(values[i][0], val.slice());
      }
      break;
    case UType::kBlob:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto ver = db->Put(keys[i], VBlob(values[i][0]), branch);
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
    const Slice& branch, bool scan) {
  switch (type) {
    case UType::kString:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto meta = db->Get(keys[i], branch);
        if (!scan) continue;
        auto val = meta.value.String();
      }
      break;
    case UType::kBlob:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto meta = db->Get(keys[i], branch);
        if (!scan) continue;
        auto val = meta.value.Blob();
        for (auto it = val.ScanChunk(); !it.end(); it.next()) it.value();
      }
      break;
    case UType::kList:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto meta = db->Get(keys[i], branch);
        if (!scan) continue;
        auto val = meta.value.List();
        for (auto it = val.Scan(); !it.end(); it.next()) it.value();
      }
      break;
    case UType::kMap:
      for (size_t i = 0; i < keys.size(); ++i) {
        auto meta = db->Get(keys[i], branch);
        if (!scan) continue;
        auto val = meta.value.Map();
        for (auto it = val.Scan(); !it.end(); it.next()) it.value();
      }
      break;
    default:
      LOG(FATAL) << "Unsupported type: " << type;
  }
}

void Benchmark::ThreadBranch(ObjectDB* db, const SliceVec& keys,
    const Slice& ref_branch, const SliceVec& branches) {
  for (size_t i = 0; i < keys.size(); ++i)
    auto res = db->Branch(keys[i], ref_branch, branches[i]);
}

void Benchmark::ThreadMerge(ObjectDB* db, const SliceVec& keys,
    const Slice& ref_branch, const SliceVec& branches) {
  for (size_t i = 0; i < keys.size(); ++i) {
    auto meta = db->Get(keys[i], branches[i]);
    switch(meta.value.type()) {
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
  }
}

// template <typename T>
// inline static std::vector<std::vector<T>> vec_split(const std::vector<T>& vec,
//                                                     size_t n) {
//   size_t s_subvec = vec.size() / n;
//   std::vector<std::vector<T>> ret;
//   for (size_t i = 0; i < n - 1; ++i) {
//     ret.emplace_back(vec.begin() + i * s_subvec,
//                      vec.begin() + (i + 1) * s_subvec);
//   }
//   ret.emplace_back(vec.begin() + (n - 1) * s_subvec, vec.end());
//   return ret;
// }
//
// template <typename T>
// inline static std::vector<T> vec_merge(const std::vector<std::vector<T>*>& vecs,
//                                        size_t size) {
//   std::vector<T> ret;
//   ret.reserve(size);
//   for (const auto& p : vecs) ret.insert(ret.end(), p->begin(), p->end());
//   return ret;
// }
//
// inline static std::vector<Hash> vec_merge(
//     const std::vector<std::vector<Hash>*>& vecs) {
//   std::vector<Hash> ret;
//   for (const auto& p : vecs) {
//     for (const auto& h : *p) {
//       ret.push_back(h.Clone());
//     }
//   }
//   return ret;
// }
//
// template <typename T>
// static std::vector<Hash>* PutThread(ObjectDB* db,
//                                     const std::vector<std::string>& keys,
//                                     const Slice& branch,
//                                     const std::vector<std::string>& values,
//                                     Profiler* profiler, size_t tid) {
//   std::vector<Hash>* versions = new std::vector<Hash>();
//   for (size_t i = 0; i < keys.size(); ++i) {
//     versions->push_back(
//         db->Put(Slice(keys[i]), T(Slice(values[i])), branch).value);
//     profiler->IncCounter(tid);
//   }
//   return versions;
// }
//
// static void GetStringThread(ObjectDB* db, const std::vector<std::string>& keys,
//                             const std::vector<Hash>& versions,
//                             Profiler* profiler, size_t tid) {
//   for (size_t i = 0; i < keys.size(); ++i) {
//     auto s = db->Get(Slice(keys[i]), versions[i]).value.String();
//     profiler->IncCounter(tid);
//     // std::cout << tid << " : " << i << std::endl;
//   }
// }
//
// static void ValidateStringThread(ObjectDB* db,
//                                  const std::vector<std::string>& keys,
//                                  const std::vector<Hash>& versions,
//                                  const std::vector<std::string>& values) {
//   CHECK_EQ(keys.size(), versions.size());
//   CHECK_EQ(keys.size(), values.size());
//   for (size_t i = 0; i < keys.size(); ++i) {
//     auto s = db->Get(Slice(keys[i]), versions[i]).value.String();
//     CHECK(Slice(values[i]) == s.slice());
//   }
// }
//
// static void GetBlobMetaThread(ObjectDB* db,
//                               const std::vector<std::string>& keys,
//                               const std::vector<Hash>& versions,
//                               Profiler* profiler, size_t tid) {
//   for (size_t i = 0; i < keys.size(); ++i) {
//     auto s = db->Get(Slice(keys[i]), versions[i]).value.Blob();
//     profiler->IncCounter(tid);
//   }
// }
//
// static void GetBlobThread(ObjectDB* db, const std::vector<std::string>& keys,
//                           const std::vector<Hash>& versions, Profiler* profiler,
//                           size_t tid) {
//   for (size_t i = 0; i < keys.size(); ++i) {
//     auto b = db->Get(Slice(keys[i]), versions[i]).value.Blob();
//     for (auto it = b.ScanChunk(); !it.end(); it.next()) it.value();
//     profiler->IncCounter(tid);
//   }
// }
//
// static void ValidateBlobThread(ObjectDB* db,
//                                const std::vector<std::string>& keys,
//                                const std::vector<Hash>& versions,
//                                const std::vector<std::string>& values) {
//   CHECK_EQ(keys.size(), versions.size());
//   CHECK_EQ(keys.size(), values.size());
//   for (size_t i = 0; i < keys.size(); ++i) {
//     auto b = db->Get(Slice(keys[i]), versions[i]).value.Blob();
//     byte_t* buf = new byte_t[b.size()];
//     b.Read(0, b.size(), buf);
//     CHECK(Slice(values[i]) == Slice(buf, b.size()));
//     delete[] buf;
//   }
// }
//
// void Benchmark::RunAll() {
//   // Validation
//   StringValidation(kNumValidations, kStringLength);
//   BlobValidation(kNumValidations, kBlobSize);
//   // String
//   FixedString(kNumStrings, kStringLength);
//   RandomString(kNumStrings, kStringLength);
//   // Blob
//   FixedBlob(kNumBlobs, kBlobSize);
//   RandomBlob(kNumBlobs, kBlobSize);
// }
//
// // String Benchmarks
//
// std::vector<Hash> Benchmark::PutString(const std::vector<std::string>& keys,
//                                        const Slice& branch,
//                                        const std::vector<std::string>& values) {
//   Timer timer;
//   std::vector<std::vector<Hash>*> vers;
//   for (size_t i = 0; i < n_threads_; ++i)
//     vers.push_back(new std::vector<Hash>());
//   auto splited_keys = vec_split(keys, n_threads_);
//   auto splited_vals = vec_split(values, n_threads_);
//   std::vector<std::future<std::vector<Hash>*>> ops;
//   Profiler p(n_threads_);
//   std::thread profiler_th(&Profiler::SamplerThread, &p);
//   timer.Reset();
//   for (size_t i = 0; i < n_threads_; ++i) {
//     ops.emplace_back(std::async(std::launch::async, PutThread<VString>, dbs_[i],
//                                 splited_keys[i], branch, splited_vals[i], &p,
//                                 i));
//   }
//   CHECK_EQ(n_threads_, ops.size());
//
//   for (auto& t : ops) {
//     CHECK(t.valid());
//     vers.push_back(t.get());
//   }
//
//   auto elapsed_time = timer.Elapse();
//   p.Terminate();
//   profiler_th.join();
//   double avg_tp = keys.size() * 1000.0 / elapsed_time;
//   auto pk_tp = p.PeakThroughput();
//   if (avg_tp > pk_tp) pk_tp = avg_tp;
//   std::cout << "Put String Time: " << elapsed_time << " ms" << std::endl
//             << "\tPeak Throughput: " << static_cast<unsigned>(pk_tp) << " ops/s"
//             << std::endl;
//
//   auto ret = vec_merge(vers);
//   for (auto& p : vers) {
//     delete p;
//   }
//   return ret;
// }
//
// void Benchmark::GetString(const std::vector<std::string>& keys,
//                           const std::vector<Hash>& versions) {
//   Timer timer;
//   auto splited_keys = vec_split(keys, n_threads_);
//   auto splited_vers = vec_split(versions, n_threads_);
//
//   Profiler p(n_threads_);
//   std::thread profiler_th(&Profiler::SamplerThread, &p);
//   timer.Reset();
//   std::vector<std::thread> ths;
//   for (size_t i = 0; i < n_threads_; ++i)
//     ths.emplace_back(GetStringThread, dbs_[i], splited_keys[i], splited_vers[i],
//                      &p, i);
//   for (auto& th : ths) th.join();
//
//   auto elapsed_time = timer.Elapse();
//   p.Terminate();
//   profiler_th.join();
//   double avg_tp = keys.size() * 1000.0 / elapsed_time;
//   auto pk_tp = p.PeakThroughput();
//   if (avg_tp > pk_tp) pk_tp = avg_tp;
//   std::cout << "Get String Time: " << elapsed_time << " ms" << std::endl
//             << "\tPeak Throughput: " << static_cast<unsigned>(pk_tp) << " ops/s"
//             << std::endl;
// }
//
// void Benchmark::ValidateString(const std::vector<std::string>& keys,
//                                const std::vector<Hash>& versions,
//                                const std::vector<std::string>& values) {
//   Timer timer;
//   auto splited_keys = vec_split(keys, n_threads_);
//   auto splited_vers = vec_split(versions, n_threads_);
//   auto splited_vals = vec_split(values, n_threads_);
//   timer.Reset();
//
//   std::vector<std::thread> ths;
//   for (size_t i = 0; i < n_threads_; ++i)
//     ths.emplace_back(ValidateStringThread, dbs_[i], splited_keys[i],
//                      splited_vers[i], splited_vals[i]);
//   for (auto& th : ths) th.join();
//   std::cout << "Validate String Time: " << timer.Elapse() << " ms" << std::endl;
// }
//
// void Benchmark::StringValidation(int n, int len) {
//   std::cout << "Validating " << n << " Strings with Fixed Length (" << len
//             << ")......" << std::endl;
//   std::vector<std::string> keys = rg_.SequentialNumString("String", n);
//   Slice branch("Validation");
//   std::vector<std::string> values = rg_.NRandomString(n, len);
//   auto versions = PutString(keys, branch, values);
//   ValidateString(keys, versions, values);
// }
//
// void Benchmark::FixedString(int n, int len) {
//   std::cout << "Benchmarking " << n << " Strings with Fixed Length (" << len
//             << ")......" << std::endl;
//   std::vector<std::string> keys = rg_.SequentialNumString("String", n);
//   Slice branch("Fixed");
//   std::vector<std::string> values = rg_.NFixedString(n, len);
//   auto versions = PutString(keys, branch, values);
//   GetString(keys, versions);
// }
//
// void Benchmark::RandomString(int n, int len) {
//   std::cout << "Benchmarking " << n
//             << " Strings with Random Length (max=" << len << ")......"
//             << std::endl;
//   std::vector<std::string> keys = rg_.SequentialNumString("String", n);
//   Slice branch("Random");
//   std::vector<std::string> values = rg_.NRandomString(n, len);
//   auto versions = PutString(keys, branch, values);
//   GetString(keys, versions);
// }
//
// // Blob Benchmarks
//
// std::vector<Hash> Benchmark::PutBlob(const std::vector<std::string>& keys,
//                                      const Slice& branch,
//                                      const std::vector<std::string>& values) {
//   Timer timer;
//   std::vector<std::vector<Hash>*> vers;
//   for (size_t i = 0; i < n_threads_; ++i)
//     vers.push_back(new std::vector<Hash>());
//   auto splited_keys = vec_split(keys, n_threads_);
//   auto splited_vals = vec_split(values, n_threads_);
//   std::vector<std::future<std::vector<Hash>*>> ops;
//   Profiler p(n_threads_);
//   std::thread profiler_th(&Profiler::SamplerThread, &p);
//   timer.Reset();
//   for (size_t i = 0; i < n_threads_; ++i) {
//     ops.emplace_back(std::async(std::launch::async, PutThread<VBlob>, dbs_[i],
//                                 splited_keys[i], branch, splited_vals[i], &p,
//                                 i));
//   }
//   CHECK_EQ(n_threads_, ops.size());
//
//   for (auto& t : ops) {
//     CHECK(t.valid());
//     vers.push_back(t.get());
//   }
//
//   auto elapsed_time = timer.Elapse();
//   p.Terminate();
//   profiler_th.join();
//   double avg_tp = keys.size() * 1000.0 / elapsed_time;
//   auto pk_tp = p.PeakThroughput();
//   if (avg_tp > pk_tp) pk_tp = avg_tp;
//   std::cout << "Put Blob Time: " << elapsed_time << " ms" << std::endl
//             << "\tPeak Throughput: " << static_cast<unsigned>(pk_tp) << " ops/s"
//             << std::endl;
//
//   auto ret = vec_merge(vers);
//   for (auto& p : vers) {
//     delete p;
//   }
//   return ret;
// }
//
// void Benchmark::GetBlobMeta(const std::vector<std::string>& keys,
//                             const std::vector<Hash>& versions) {
//   Timer timer;
//   auto splited_keys = vec_split(keys, n_threads_);
//   auto splited_vers = vec_split(versions, n_threads_);
//   Profiler p(n_threads_);
//   std::thread profiler_th(&Profiler::SamplerThread, &p);
//   timer.Reset();
//
//   std::vector<std::thread> ths;
//   for (size_t i = 0; i < n_threads_; ++i)
//     ths.emplace_back(GetBlobMetaThread, dbs_[i], splited_keys[i],
//                      splited_vers[i], &p, i);
//   for (auto& th : ths) th.join();
//
//   auto elapsed_time = timer.Elapse();
//   p.Terminate();
//   profiler_th.join();
//   double avg_tp = keys.size() * 1000.0 / elapsed_time;
//   auto pk_tp = p.PeakThroughput();
//   if (avg_tp > pk_tp) pk_tp = avg_tp;
//   std::cout << "Get Blob Meta Time: " << elapsed_time << " ms" << std::endl
//             << "\tPeak Throughput: " << static_cast<unsigned>(pk_tp) << " ops/s"
//             << std::endl;
// }
//
// void Benchmark::GetBlob(const std::vector<std::string>& keys,
//                         const std::vector<Hash>& versions) {
//   Timer timer;
//   auto splited_keys = vec_split(keys, n_threads_);
//   auto splited_vers = vec_split(versions, n_threads_);
//   Profiler p(n_threads_);
//   std::thread profiler_th(&Profiler::SamplerThread, &p);
//   timer.Reset();
//
//   std::vector<std::thread> ths;
//   for (size_t i = 0; i < n_threads_; ++i)
//     ths.emplace_back(GetBlobThread, dbs_[i], splited_keys[i], splited_vers[i],
//                      &p, i);
//   for (auto& th : ths) th.join();
//
//   auto elapsed_time = timer.Elapse();
//   p.Terminate();
//   profiler_th.join();
//   double avg_tp = keys.size() * 1000.0 / elapsed_time;
//   auto pk_tp = p.PeakThroughput();
//   if (avg_tp > pk_tp) pk_tp = avg_tp;
//   std::cout << "Get Blob Time: " << elapsed_time << " ms" << std::endl
//             << "\tPeak Throughput: " << static_cast<unsigned>(pk_tp) << " ops/s"
//             << std::endl;
// }
//
// void Benchmark::ValidateBlob(const std::vector<std::string>& keys,
//                              const std::vector<Hash>& versions,
//                              const std::vector<std::string>& values) {
//   Timer timer;
//   auto splited_keys = vec_split(keys, n_threads_);
//   auto splited_vers = vec_split(versions, n_threads_);
//   auto splited_vals = vec_split(values, n_threads_);
//   timer.Reset();
//
//   std::vector<std::thread> ths;
//   for (size_t i = 0; i < n_threads_; ++i)
//     ths.emplace_back(ValidateBlobThread, dbs_[i], splited_keys[i],
//                      splited_vers[i], splited_vals[i]);
//   for (auto& th : ths) th.join();
//   std::cout << "Validate Blob Time: " << timer.Elapse() << " ms\n";
// }
//
// void Benchmark::BlobValidation(int n, int size) {
//   std::cout << "Validating " << n << " Blobs with Fixed Size (" << size
//             << ")......" << std::endl;
//   std::vector<std::string> keys = rg_.SequentialNumString("Blob", n);
//   Slice branch("Validation");
//   std::vector<std::string> values = rg_.NRandomString(n, size);
//   auto versions = PutBlob(keys, branch, values);
//   ValidateBlob(keys, versions, values);
// }
//
// void Benchmark::FixedBlob(int n, int size) {
//   std::cout << "Benchmarking " << n << " Blobs with Fixed Size (" << size
//             << ")......" << std::endl;
//   std::vector<std::string> keys = rg_.SequentialNumString("Blob", n);
//   Slice branch("Fixed");
//   std::vector<std::string> values = rg_.NFixedString(n, size);
//   auto versions = PutBlob(keys, branch, values);
//   GetBlobMeta(keys, versions);
//   GetBlob(keys, versions);
// }
//
// void Benchmark::RandomBlob(int n, int size) {
//   std::cout << "Benchmarking " << n << " Blobs with Random Size (max=" << size
//             << ")......" << std::endl;
//   std::vector<std::string> keys = rg_.SequentialNumString("Blob", n);
//   Slice branch("Random");
//   std::vector<std::string> values = rg_.NRandomString(n, size);
//   auto versions = PutBlob(keys, branch, values);
//   GetBlobMeta(keys, versions);
//   GetBlob(keys, versions);
// }
}  // namespace ustore
