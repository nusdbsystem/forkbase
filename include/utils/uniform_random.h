#ifndef UTILS_UNIFORM_RANDOM_H_
#define UTILS_UNIFORM_RANDOM_H_

#include <algorithm>
#include <string>
#include <random>
#include <vector>

namespace uio {
namespace utils {

class UniformRandom {
 public:
  UniformRandom();
  ~UniformRandom() = default;

  std::string FixedString(int length);

  std::vector<std::string> NFixedString(int size, int length);

  std::string RandomString(int max_length);

  std::vector<std::string> NRandomString(int size, int max_length);

  std::vector<std::string> SequentialNumString(
    const std::string& prefix, int size);

  std::vector<std::string>
  PrefixSeqString(const std::string& prefix,
                  int size,
                  int mod);

  std::vector<std::string>
  PrefixRandString(const std::string& prefix,
                   int size,
                   int mod);

  inline int RandomInt(const int min,
                       const int max) {
    std::uniform_int_distribution<> dist(min, max);
    return dist(engine_);
  }

  inline bool RandomBool() {
    static std::uniform_int_distribution<> dist(0, 1);
    return (dist(engine_) == 0);
  }

  template<typename T>
  inline void Shuffle(std::vector<T>* elems) {
    std::shuffle(elems->begin(), elems->end(), engine_);
  }

  const char* RandomWeekDay();
  const char* RandomWeekDayAbbr();

  inline const char* RandomGender() {
    return (RandomBool() ? "Male" : "Female");
  }

  inline const char* RandomGenderAbbr() {
    return (RandomBool() ? "M" : "F");
  }

 private:
  std::default_random_engine engine_;
  std::uniform_int_distribution<> alph_dist_;
};

}  // namespace utils

// pull names to the uio namespace
using utils::UniformRandom;

}  // namespace uio

#endif  // UTILS_UNIFORM_RANDOM_H_
