#include "utils/uniform_random.h"

namespace uio {
namespace utils {

static const char alphabet[] =
  "0123456789"
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  "abcdefghijklmnopqrstuvwxyz";  // ends with '\0'

UniformRandom::UniformRandom() {
  std::random_device device;
  engine_ = std::default_random_engine(device());
  const auto sz_alphabet = sizeof(alphabet) / sizeof(*alphabet);
  alph_dist_ = std::uniform_int_distribution<>(0, sz_alphabet - 2);
}

std::string UniformRandom::FixedString(int length) {
  std::string str;
  std::generate_n(std::back_inserter(str), length, [&]() {
    return alphabet[alph_dist_(engine_)];
  });
  return str;
}

std::vector<std::string>
UniformRandom::NFixedString(int size,
                            int length) {
  std::vector<std::string> strs;
  strs.reserve(size);
  std::generate_n(std::back_inserter(strs), (strs).capacity(), [&]() {
    std::string str;
    str.reserve(length);
    std::generate_n(std::back_inserter(str), length, [&]() {
      return alphabet[alph_dist_(engine_)];
    });
    return str;
  });
  return strs;
}

std::string UniformRandom::RandomString(int max_length) {
  std::uniform_int_distribution<> len_dist_(1, max_length);
  std::string str;

  int length = len_dist_(engine_);
  std::generate_n(std::back_inserter(str), length, [&]() {
    return alphabet[alph_dist_(engine_)];
  });
  return str;
}

std::vector<std::string>
UniformRandom::NRandomString(int size,
                             int max_length) {
  std::uniform_int_distribution<> len_dist_(1, max_length);
  std::vector<std::string> strs;

  strs.reserve(size);
  std::generate_n(std::back_inserter(strs), (strs).capacity(), [&]() {
    std::string str;
    int length = len_dist_(engine_);
    str.reserve(length);
    std::generate_n(std::back_inserter(str), length, [&]() {
      return alphabet[alph_dist_(engine_)];
    });
    return str;
  });
  return strs;
}

std::vector<std::string> UniformRandom::SequentialNumString(
  const std::string& prefix, int size) {
  std::vector<std::string> keys;
  keys.reserve(size);
  int k = 0;
  std::generate_n(std::back_inserter(keys), (keys).capacity(), [&k, &prefix]() {
    return prefix + std::to_string(++k);
  });
  return keys;
}

std::vector<std::string> UniformRandom::PrefixSeqString(
  const std::string& prefix, int size, int mod) {
  std::vector<std::string> res(size);
  for (int i = 0; i < size; ++i)
    res[i] = prefix + std::to_string((i) % mod);
  return res;
}

std::vector<std::string> UniformRandom::PrefixRandString(
  const std::string& prefix, int size, int mod) {
  std::vector<std::string> res(size);
  for (int i = 0; i < size; ++i)
    res[i] = prefix + std::to_string(alph_dist_(engine_) % mod);
  return res;
}

const char* UniformRandom::RandomWeekDay() {
  static const char* weekdays[] = {
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
  };
  static std::uniform_int_distribution<> dist(0, 6);
  return weekdays[dist(engine_)];
}

const char* UniformRandom::RandomWeekDayAbbr() {
  static const char* weekdays_abbr[] = {
    "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"
  };
  static std::uniform_int_distribution<> dist(0, 6);
  return weekdays_abbr[dist(engine_)];
}

}  // namespace utils
}  // namespace uio
