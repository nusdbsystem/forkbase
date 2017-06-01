// Copyright (c) 2017 The Ustore Authors.

#include <future>
#include <iterator>
#include <iostream>
#include <iomanip>
#include <vector>
#include "benchmark/benchmark.h"
#include "store/chunk_store.h"
#include "types/client/vblob.h"
#include "types/client/vstring.h"
#include "utils/logging.h"

namespace ustore {

const unsigned Profiler::kSamplingInterval = 500;

Profiler::Profiler(size_t n_th) : n_thread_(n_th) {
  counters_ = new std::atomic<unsigned>[n_th];
  for (size_t i = 0; i < n_th; ++i) {
    samples_.emplace_back();
    counters_[i].store(0);
  }
  finished_.store(false, std::memory_order_release);
}

Profiler::~Profiler() { delete[] counters_; }

double Profiler::PeakThroughput() {
  std::vector<unsigned> ops;
  unsigned prev_sum = 0;
  double ret = -1;
  for (size_t i = 0; i < samples_[0].size(); ++i) {
    unsigned sum = 0;
    // std::cout << i << "-th: " << std::endl;
    for (size_t t = 0; t < n_thread_; ++t) {
      //  std::cout << " " << samples_[t][i];
      sum += samples_[t][i];
    }
    // std::cout << std::endl
    //           << "\tsum: " << sum << "\tprev_sum: " << prev_sum << std::endl;
    double tp = 1000.0 * (sum - prev_sum) / kSamplingInterval;
    prev_sum = sum;
    if (tp > ret) ret = tp;
  }
  return ret;
}

void Profiler::SamplerThread() {
  while (!finished_.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(kSamplingInterval));
    for (size_t t = 0; t < n_thread_; ++t)
      samples_[t].push_back(counters_[t].load(std::memory_order_acquire));
  }
}

template <typename T>
inline static std::vector<std::vector<T>> vec_split(const std::vector<T>& vec,
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

template <typename T>
inline static std::vector<T> vec_merge(const std::vector<std::vector<T>*>& vecs,
                                       size_t size) {
  std::vector<T> ret;
  ret.reserve(size);
  for (const auto& p : vecs) ret.insert(ret.end(), p->begin(), p->end());
  return ret;
}

inline static std::vector<Hash> vec_merge(
    const std::vector<std::vector<Hash>*>& vecs) {
  std::vector<Hash> ret;
  for (const auto& p : vecs) {
    for (const auto& h : *p) {
      ret.push_back(h.Clone());
    }
  }
  return ret;
}

template <typename T>
static std::vector<Hash>* PutThread(ObjectDB* db,
                                    const std::vector<std::string>& keys,
                                    const Slice& branch,
                                    const std::vector<std::string>& values,
                                    Profiler* profiler, size_t tid) {
  std::vector<Hash>* versions = new std::vector<Hash>();
  for (size_t i = 0; i < keys.size(); ++i) {
    versions->push_back(
        db->Put(Slice(keys[i]), T(Slice(values[i])), branch).value);
    profiler->IncCounter(tid);
  }
  return versions;
}

static void GetStringThread(ObjectDB* db, const std::vector<std::string>& keys,
                            const std::vector<Hash>& versions,
                            Profiler* profiler, size_t tid) {
  for (size_t i = 0; i < keys.size(); ++i) {
    auto s = db->Get(Slice(keys[i]), versions[i]).value.String();
    profiler->IncCounter(tid);
    // std::cout << tid << " : " << i << std::endl;
  }
}

static void ValidateStringThread(ObjectDB* db,
                                 const std::vector<std::string>& keys,
                                 const std::vector<Hash>& versions,
                                 const std::vector<std::string>& values) {
  CHECK_EQ(keys.size(), versions.size());
  CHECK_EQ(keys.size(), values.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    auto s = db->Get(Slice(keys[i]), versions[i]).value.String();
    CHECK(Slice(values[i]) == s.slice());
  }
}

static void GetBlobMetaThread(ObjectDB* db,
                              const std::vector<std::string>& keys,
                              const std::vector<Hash>& versions,
                              Profiler* profiler, size_t tid) {
  for (size_t i = 0; i < keys.size(); ++i) {
    auto s = db->Get(Slice(keys[i]), versions[i]).value.Blob();
    profiler->IncCounter(tid);
  }
}

static void GetBlobThread(ObjectDB* db, const std::vector<std::string>& keys,
                          const std::vector<Hash>& versions, Profiler* profiler,
                          size_t tid) {
  for (size_t i = 0; i < keys.size(); ++i) {
    auto b = db->Get(Slice(keys[i]), versions[i]).value.Blob();
    for (auto it = b.ScanChunk(); !it.end(); it.next()) it.value();
    profiler->IncCounter(tid);
  }
}

static void ValidateBlobThread(ObjectDB* db,
                               const std::vector<std::string>& keys,
                               const std::vector<Hash>& versions,
                               const std::vector<std::string>& values) {
  CHECK_EQ(keys.size(), versions.size());
  CHECK_EQ(keys.size(), values.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    auto b = db->Get(Slice(keys[i]), versions[i]).value.Blob();
    byte_t* buf = new byte_t[b.size()];
    b.Read(0, b.size(), buf);
    CHECK(Slice(values[i]) == Slice(buf, b.size()));
    delete[] buf;
  }
}

void Benchmark::RunAll() {
  // Validation
  StringValidation(kNumValidations, kStringLength);
  BlobValidation(kNumValidations, kBlobSize);
  // String
  FixedString(kNumStrings, kStringLength);
  RandomString(kNumStrings, kStringLength);
  // Blob
  FixedBlob(kNumBlobs, kBlobSize);
  RandomBlob(kNumBlobs, kBlobSize);
}

// String Benchmarks

std::vector<Hash> Benchmark::PutString(const std::vector<std::string>& keys,
                                       const Slice& branch,
                                       const std::vector<std::string>& values) {
  Timer timer;
  std::vector<std::vector<Hash>*> vers;
  for (size_t i = 0; i < n_threads_; ++i)
    vers.push_back(new std::vector<Hash>());
  auto splited_keys = vec_split(keys, n_threads_);
  auto splited_vals = vec_split(values, n_threads_);
  std::vector<std::future<std::vector<Hash>*>> ops;
  Profiler p(n_threads_);
  std::thread profiler_th(&Profiler::SamplerThread, &p);
  timer.Reset();
  for (size_t i = 0; i < n_threads_; ++i) {
    ops.emplace_back(std::async(std::launch::async, PutThread<VString>, dbs_[i],
                                splited_keys[i], branch, splited_vals[i], &p,
                                i));
  }
  CHECK_EQ(n_threads_, ops.size());

  for (auto& t : ops) {
    CHECK(t.valid());
    vers.push_back(t.get());
  }

  auto elapsed_time = timer.Elapse();
  p.Terminate();
  profiler_th.join();
  double avg_tp = keys.size() * 1000.0 / elapsed_time;
  auto pk_tp = p.PeakThroughput();
  if (avg_tp > pk_tp) pk_tp = avg_tp;
  std::cout << "Put String Time: " << elapsed_time << " ms" << std::endl
            << "\tPeak Throughput: " << static_cast<unsigned>(pk_tp) << " ops/s"
            << std::endl;

  auto ret = vec_merge(vers);
  for (auto& p : vers) {
    delete p;
  }
  return ret;
}

void Benchmark::GetString(const std::vector<std::string>& keys,
                          const std::vector<Hash>& versions) {
  Timer timer;
  auto splited_keys = vec_split(keys, n_threads_);
  auto splited_vers = vec_split(versions, n_threads_);

  Profiler p(n_threads_);
  std::thread profiler_th(&Profiler::SamplerThread, &p);
  timer.Reset();
  std::vector<std::thread> ths;
  for (size_t i = 0; i < n_threads_; ++i)
    ths.emplace_back(GetStringThread, dbs_[i], splited_keys[i], splited_vers[i],
                     &p, i);
  for (auto& th : ths) th.join();

  auto elapsed_time = timer.Elapse();
  p.Terminate();
  profiler_th.join();
  double avg_tp = keys.size() * 1000.0 / elapsed_time;
  auto pk_tp = p.PeakThroughput();
  if (avg_tp > pk_tp) pk_tp = avg_tp;
  std::cout << "Get String Time: " << elapsed_time << " ms" << std::endl
            << "\tPeak Throughput: " << static_cast<unsigned>(pk_tp) << " ops/s"
            << std::endl;
}

void Benchmark::ValidateString(const std::vector<std::string>& keys,
                               const std::vector<Hash>& versions,
                               const std::vector<std::string>& values) {
  Timer timer;
  auto splited_keys = vec_split(keys, n_threads_);
  auto splited_vers = vec_split(versions, n_threads_);
  auto splited_vals = vec_split(values, n_threads_);
  timer.Reset();

  std::vector<std::thread> ths;
  for (size_t i = 0; i < n_threads_; ++i)
    ths.emplace_back(ValidateStringThread, dbs_[i], splited_keys[i],
                     splited_vers[i], splited_vals[i]);
  for (auto& th : ths) th.join();
  std::cout << "Validate String Time: " << timer.Elapse() << " ms" << std::endl;
}

void Benchmark::StringValidation(int n, int len) {
  std::cout << "Validating " << n << " Strings with Fixed Length (" << len
            << ")......" << std::endl;
  std::vector<std::string> keys = rg_.SequentialNumString("String", n);
  Slice branch("Validation");
  std::vector<std::string> values = rg_.NRandomString(n, len);
  auto versions = PutString(keys, branch, values);
  ValidateString(keys, versions, values);
}

void Benchmark::FixedString(int n, int len) {
  std::cout << "Benchmarking " << n << " Strings with Fixed Length (" << len
            << ")......" << std::endl;
  std::vector<std::string> keys = rg_.SequentialNumString("String", n);
  Slice branch("Fixed");
  std::vector<std::string> values = rg_.NFixedString(n, len);
  auto versions = PutString(keys, branch, values);
  GetString(keys, versions);
}

void Benchmark::RandomString(int n, int len) {
  std::cout << "Benchmarking " << n
            << " Strings with Random Length (max=" << len << ")......"
            << std::endl;
  std::vector<std::string> keys = rg_.SequentialNumString("String", n);
  Slice branch("Random");
  std::vector<std::string> values = rg_.NRandomString(n, len);
  auto versions = PutString(keys, branch, values);
  GetString(keys, versions);
}

// Blob Benchmarks

std::vector<Hash> Benchmark::PutBlob(const std::vector<std::string>& keys,
                                     const Slice& branch,
                                     const std::vector<std::string>& values) {
  Timer timer;
  std::vector<std::vector<Hash>*> vers;
  for (size_t i = 0; i < n_threads_; ++i)
    vers.push_back(new std::vector<Hash>());
  auto splited_keys = vec_split(keys, n_threads_);
  auto splited_vals = vec_split(values, n_threads_);
  std::vector<std::future<std::vector<Hash>*>> ops;
  Profiler p(n_threads_);
  std::thread profiler_th(&Profiler::SamplerThread, &p);
  timer.Reset();
  for (size_t i = 0; i < n_threads_; ++i) {
    ops.emplace_back(std::async(std::launch::async, PutThread<VBlob>, dbs_[i],
                                splited_keys[i], branch, splited_vals[i], &p,
                                i));
  }
  CHECK_EQ(n_threads_, ops.size());

  for (auto& t : ops) {
    CHECK(t.valid());
    vers.push_back(t.get());
  }

  auto elapsed_time = timer.Elapse();
  p.Terminate();
  profiler_th.join();
  double avg_tp = keys.size() * 1000.0 / elapsed_time;
  auto pk_tp = p.PeakThroughput();
  if (avg_tp > pk_tp) pk_tp = avg_tp;
  std::cout << "Put Blob Time: " << elapsed_time << " ms" << std::endl
            << "\tPeak Throughput: " << static_cast<unsigned>(pk_tp) << " ops/s"
            << std::endl;

  auto ret = vec_merge(vers);
  for (auto& p : vers) {
    delete p;
  }
  return ret;
}

void Benchmark::GetBlobMeta(const std::vector<std::string>& keys,
                            const std::vector<Hash>& versions) {
  Timer timer;
  auto splited_keys = vec_split(keys, n_threads_);
  auto splited_vers = vec_split(versions, n_threads_);
  Profiler p(n_threads_);
  std::thread profiler_th(&Profiler::SamplerThread, &p);
  timer.Reset();

  std::vector<std::thread> ths;
  for (size_t i = 0; i < n_threads_; ++i)
    ths.emplace_back(GetBlobMetaThread, dbs_[i], splited_keys[i],
                     splited_vers[i], &p, i);
  for (auto& th : ths) th.join();

  auto elapsed_time = timer.Elapse();
  p.Terminate();
  profiler_th.join();
  double avg_tp = keys.size() * 1000.0 / elapsed_time;
  auto pk_tp = p.PeakThroughput();
  if (avg_tp > pk_tp) pk_tp = avg_tp;
  std::cout << "Get Blob Meta Time: " << elapsed_time << " ms" << std::endl
            << "\tPeak Throughput: " << static_cast<unsigned>(pk_tp) << " ops/s"
            << std::endl;
}

void Benchmark::GetBlob(const std::vector<std::string>& keys,
                        const std::vector<Hash>& versions) {
  Timer timer;
  auto splited_keys = vec_split(keys, n_threads_);
  auto splited_vers = vec_split(versions, n_threads_);
  Profiler p(n_threads_);
  std::thread profiler_th(&Profiler::SamplerThread, &p);
  timer.Reset();

  std::vector<std::thread> ths;
  for (size_t i = 0; i < n_threads_; ++i)
    ths.emplace_back(GetBlobThread, dbs_[i], splited_keys[i], splited_vers[i],
                     &p, i);
  for (auto& th : ths) th.join();

  auto elapsed_time = timer.Elapse();
  p.Terminate();
  profiler_th.join();
  double avg_tp = keys.size() * 1000.0 / elapsed_time;
  auto pk_tp = p.PeakThroughput();
  if (avg_tp > pk_tp) pk_tp = avg_tp;
  std::cout << "Get Blob Time: " << elapsed_time << " ms" << std::endl
            << "\tPeak Throughput: " << static_cast<unsigned>(pk_tp) << " ops/s"
            << std::endl;
}

void Benchmark::ValidateBlob(const std::vector<std::string>& keys,
                             const std::vector<Hash>& versions,
                             const std::vector<std::string>& values) {
  Timer timer;
  auto splited_keys = vec_split(keys, n_threads_);
  auto splited_vers = vec_split(versions, n_threads_);
  auto splited_vals = vec_split(values, n_threads_);
  timer.Reset();

  std::vector<std::thread> ths;
  for (size_t i = 0; i < n_threads_; ++i)
    ths.emplace_back(ValidateBlobThread, dbs_[i], splited_keys[i],
                     splited_vers[i], splited_vals[i]);
  for (auto& th : ths) th.join();
  std::cout << "Validate Blob Time: " << timer.Elapse() << " ms\n";
}

void Benchmark::BlobValidation(int n, int size) {
  std::cout << "Validating " << n << " Blobs with Fixed Size (" << size
            << ")......" << std::endl;
  std::vector<std::string> keys = rg_.SequentialNumString("Blob", n);
  Slice branch("Validation");
  std::vector<std::string> values = rg_.NRandomString(n, size);
  auto versions = PutBlob(keys, branch, values);
  ValidateBlob(keys, versions, values);
}

void Benchmark::FixedBlob(int n, int size) {
  std::cout << "Benchmarking " << n << " Blobs with Fixed Size (" << size
            << ")......" << std::endl;
  std::vector<std::string> keys = rg_.SequentialNumString("Blob", n);
  Slice branch("Fixed");
  std::vector<std::string> values = rg_.NFixedString(n, size);
  auto versions = PutBlob(keys, branch, values);
  GetBlobMeta(keys, versions);
  GetBlob(keys, versions);
}

void Benchmark::RandomBlob(int n, int size) {
  std::cout << "Benchmarking " << n << " Blobs with Random Size (max=" << size
            << ")......" << std::endl;
  std::vector<std::string> keys = rg_.SequentialNumString("Blob", n);
  Slice branch("Random");
  std::vector<std::string> values = rg_.NRandomString(n, size);
  auto versions = PutBlob(keys, branch, values);
  GetBlobMeta(keys, versions);
  GetBlob(keys, versions);
}
}  // namespace ustore
