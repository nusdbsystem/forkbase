// Copyright (c) 2017 The Ustore Authors.

#include <thread>
#include "benchmark/bench_utils.h"

namespace ustore {

RandomGenerator::RandomGenerator() {
  std::random_device device;
  engine_ = std::default_random_engine(device());
  alph_dist_ = std::uniform_int_distribution<>(0,
    sizeof(alphabet)/sizeof(*alphabet)-2);
}

std::string RandomGenerator::FixedString(int length) {
  std::string str;
  std::generate_n(std::back_inserter(str), length, [&]() {
    return alphabet[alph_dist_(engine_)]; });
  return str;
}

std::vector<std::string> RandomGenerator::NFixedString(
  int size, int length) {
  std::vector<std::string> strs;
  strs.reserve(size);
  std::generate_n(std::back_inserter(strs), (strs).capacity(), [&]() {
    std::string str;
    str.reserve(length);
    std::generate_n(std::back_inserter(str), length, [&]() {
      return alphabet[alph_dist_(engine_)];});
    return str; });
  return strs;
}

std::string RandomGenerator::RandomString(int maxLength) {
  std::uniform_int_distribution<> len_dist_(1, maxLength);
  std::string str;

  int length = len_dist_(engine_);
  std::generate_n(std::back_inserter(str), length, [&]() {
    return alphabet[alph_dist_(engine_)]; });
  return str;
}

std::vector<std::string> RandomGenerator::NRandomString(
  int size, int maxLength) {
  std::uniform_int_distribution<> len_dist_(1, maxLength);
  std::vector<std::string> strs;

  strs.reserve(size);
  std::generate_n(std::back_inserter(strs), (strs).capacity(), [&]() {
    std::string str;
    int length = len_dist_(engine_);
    str.reserve(length);
    std::generate_n(std::back_inserter(str), length, [&]() {
      return alphabet[alph_dist_(engine_)];});
    return str; });
  return strs;
}

std::vector<std::string> RandomGenerator::SequentialNumString(
    const std::string& prefix, int size) {
  std::vector<std::string> keys;

  keys.reserve(size);
  int k = 0;
  std::generate_n(std::back_inserter(keys), (keys).capacity(), [&k, &prefix]() {
    return prefix + std::to_string(++k); });
  return keys;
}

std::vector<std::string> RandomGenerator::PrefixSeqString(
    const std::string& prefix, int size, int mod) {
  std::vector<std::string> res(size);
  for (size_t i = 0; i < size; ++i)
    res[i] = prefix + std::to_string((i)%mod);
  return res;
}

std::vector<std::string> RandomGenerator::PrefixRandString(
    const std::string& prefix, int size, int mod) {
  std::vector<std::string> res(size);
  for (size_t i = 0; i < size; ++i)
    res[i] = prefix + std::to_string(alph_dist_(engine_)%mod);
  return res;
}


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

}  // namespace ustore
