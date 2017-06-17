// Copyright (c) 2017 The Ustore Authors.

#include <boost/tokenizer.hpp>
#include "utils/utils.h"

namespace ustore {

static std::unordered_map<std::string, UType> str2type = {
  {"Bool", UType::kBool},
  {"Num", UType::kNum},
  {"String", UType::kString},
  {"Blob", UType::kBlob},
  {"List", UType::kList},
  {"Set", UType::kSet},
  {"Map", UType::kMap}
};

UType Utils::ToUType(const std::string& str) {
  auto it = str2type.find(str);
  return it == str2type.end() ? UType::kUnknown : it->second;
}

static std::unordered_map<UType, std::string> type2str = {
  {UType::kBool, "Bool"},
  {UType::kNum, "Num"},
  {UType::kString, "String"},
  {UType::kBlob, "Blob"},
  {UType::kList, "List"},
  {UType::kSet, "Set"},
  {UType::kMap, "Map"}
};

std::string Utils::ToString(const UType& type) {
  auto it = type2str.find(type);
  return it == type2str.end() ? "<Unknown>" : it->second;
}

static std::unordered_map<ErrorCode, std::string> ec2str = {
  {ErrorCode::kOK, "success"},
  {ErrorCode::kUnknownOp, "unknown operation"},
  {ErrorCode::kInvalidRange, "invalid value range"},
  {ErrorCode::kBranchExists, "branch already exists"},
  {ErrorCode::kBranchNotExists, "branch does not exist"},
  {ErrorCode::kReferringVersionNotExist, "referring version does not exist"},
  {ErrorCode::kUCellNotfound, "UCell is not found"},
  {ErrorCode::kChunkNotExists, "chunk does not exist"},
  {ErrorCode::kTypeUnsupported, "unsupported data type"},
  {ErrorCode::kFailedCreateUCell, "failed to create UCell"},
  {ErrorCode::kFailedCreateSBlob, "failed to create SBlob"},
  {ErrorCode::kFailedCreateSString, "failed to create SString"},
  {ErrorCode::kFailedCreateSList, "failed to create SList"},
  {ErrorCode::kFailedCreateSMap, "failed to create SMap"},
  {ErrorCode::kInconsistentKey, "inconsistent values of key"},
  {ErrorCode::kInvalidValue, "invalid value"},
  {ErrorCode::kFailedModifySBlob, "failed to modify SBlob"},
  {ErrorCode::kFailedModifySList, "failed to modify SList"},
  {ErrorCode::kFailedModifySMap, "failed to modify SMap"},
  {ErrorCode::kIndexOutOfRange, "index out of range"},
  {ErrorCode::kTypeMismatch, "data types mismatch"},
  {ErrorCode::kKeyNotExists, "key does not exist"},
  {ErrorCode::kKeyExists, "key already exists"},
  {ErrorCode::kTableNotExists, "table does not exist"},
  {ErrorCode::kEmptyTable, "table is empty"},
  {ErrorCode::kNotEmptyTable, "table is not empty"},
  {ErrorCode::kColumnNotExists, "column does not exist"},
  {ErrorCode::kRowNotExists, "row does not exist"},
  {ErrorCode::kFailedOpenFile, "failed to open file"},
  {ErrorCode::kInvalidCommandArgument, "invalid command-line argument"},
  {ErrorCode::kUnknownCommand, "unrecognized command"}
};

std::string Utils::ToString(const ErrorCode& ec) {
  auto it = ec2str.find(ec);
  return it == ec2str.end() ? "<Unknown>" : it->second;
}

std::vector<std::string> Utils::Tokenize(
  const std::string& str, const char* sep_chars) {
  using Tokenizer = boost::tokenizer<boost::char_separator<char>>;
  using CharSep = boost::char_separator<char>;
  std::vector<std::string> vec;
  for (const auto& t : Tokenizer(str, CharSep(sep_chars))) {
    vec.push_back(std::move(t));
  }
  return vec;
}

bool Utils::TokenizeArgs(const std::string& line,
                         std::vector<std::string>* args) {
  args->clear();
  std::stringstream ss;
  bool in_quote = false;
  for (auto it = line.begin(); it != line.end(); ++it) {
    if (in_quote) {
      if (*it == '\"') {
        args->emplace_back(ss.str());
        ss.str("");
        in_quote = false;
      } else {
        ss << *it;
      }
    } else if (*it == '\"') {
      in_quote = true;
    } else if (*it == ' ' || *it == '\t') {
      auto elem = ss.str();
      if (!elem.empty()) {
        args->push_back(std::move(elem));
        ss.str("");
      }
    } else {
      ss << *it;
    }
  }
  args->emplace_back(ss.str());
  return !in_quote;
}

std::vector<int> Utils::ToIntVector(
  const std::string& str, const char* sep_chars) {
  static auto f = [](const std::string & str) { return std::stoi(str); };
  return ToVector<int>(str, f, sep_chars);
}

std::vector<double> Utils::ToDoubleVector(
  const std::string& str, const char* sep_chars) {
  static auto f = [](const std::string & str) { return std::stod(str); };
  return ToVector<double>(str, f, sep_chars);
}

std::vector<long> Utils::ToLongVector(
  const std::string& str, const char* sep_chars) {
  static auto f = [](const std::string & str) { return std::stol(str); };
  return ToVector<long>(str, f, sep_chars);
}

ErrorCode Utils::CheckIndex(size_t idx, const SList& list) {
  if (idx >= list.numElements()) {
    LOG(WARNING) << "Index out of range: [Actual] " << idx
                 << ", [Expected] <" << list.numElements();
    return ErrorCode::kIndexOutOfRange;
  }
  return ErrorCode::kOK;
}

void Utils::Print(const UList& list, const std::string& lsymbol,
                  const std::string& rsymbol, const std::string& sep,
                  bool elem_in_quote, std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto it = list.Scan();
  os << lsymbol;
  if (!it.end()) {
    os << quote << it.value() << quote;
    for (it.next(); !it.end(); it.next()) {
      os << sep << quote << it.value() << quote;
    }
  }
  os << rsymbol;
}

void Utils::Print(const UMap& map, const std::string& lsymbol,
                  const std::string& rsymbol, const std::string& sep,
                  const std::string& lentry, const std::string& rentry,
                  const std::string& entry_sep, bool elem_in_quote,
                  std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto it = map.Scan();
  auto f_print_it = [&]() {
    os << lentry << quote << it.key() << quote << entry_sep << quote
       << it.value() << quote << rentry;
  };
  os << lsymbol;
  if (!it.end()) {
    f_print_it();
    for (it.next(); !it.end(); it.next()) {
      os << sep;
      f_print_it();
    }
  }
  os << rsymbol;
}

void Utils::PrintKeys(const UMap& map, const std::string& lsymbol,
                      const std::string& rsymbol, const std::string& sep,
                      bool elem_in_quote, std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto it = map.Scan();
  os << lsymbol;
  if (!it.end()) {
    os << quote << it.key() << quote;
    for (it.next(); !it.end(); it.next()) {
      os << sep << quote << it.key() << quote;
    }
  }
  os << rsymbol;
}

// TODO(ruanpc): make it_diff.key() of DuallyDiffIndexIterator refer to
//               it_diff.index() so that the following print function for
//               diff of UList be the template function of PrintDiff.
void Utils::PrintListDiff(DuallyDiffIndexIterator& it_diff,
                          bool show_diff,
                          bool elem_in_quote, std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto f_print_diff_key = [&os, &it_diff, &quote]() {
    os << quote << it_diff.index() << quote;
  };
  auto f_print_diff = [&os, &it_diff, &quote]() {
    os << quote << it_diff.index() << quote << ":(";
    auto lhs = it_diff.lhs_value();
    if (lhs.empty()) { os << "_"; } else { os << quote << lhs << quote; }
    os << ',';
    auto rhs = it_diff.rhs_value();
    if (rhs.empty()) { os << "_"; } else { os << quote << rhs << quote; }
    os << ')';
  };

  os << "[";
  if (!it_diff.end()) {
    show_diff ? f_print_diff() : f_print_diff_key();
    for (it_diff.next(); !it_diff.end(); it_diff.next()) {
      os << ", ";
      show_diff ? f_print_diff() : f_print_diff_key();
    }
  }
  os << "]";
}

}  // namespace ustore
