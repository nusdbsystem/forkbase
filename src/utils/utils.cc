// Copyright (c) 2017 The Ustore Authors.

#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <limits>
#include <regex>
#include "utils/utils.h"

namespace ustore {

namespace boost_fs = boost::filesystem;
namespace boost_sys = boost::system;

const size_t Utils::max_size_t(std::numeric_limits<size_t>::max());

static const std::unordered_map<std::string, UType> str2type = {
  {"bool", UType::kBool},
  {"num", UType::kNum},
  {"string", UType::kString},
  {"blob", UType::kBlob},
  {"list", UType::kList},
  {"set", UType::kSet},
  {"map", UType::kMap}
};

UType Utils::ToUType(const std::string& str) {
  auto it = str2type.find(str);
  return it == str2type.end() ? UType::kUnknown : it->second;
}

static const std::unordered_map<UType, std::string> type2str = {
  {UType::kBool, "Bool"},
  {UType::kNum, "Num"},
  {UType::kString, "String"},
  {UType::kBlob, "Blob"},
  {UType::kList, "List"},
  {UType::kSet, "Set"},
  {UType::kMap, "Map"}
};

const std::string& Utils::ToString(const UType& type) {
  auto it = type2str.find(type);
  static const std::string unknown("<Unknown>");
  return it == type2str.end() ? unknown : it->second;
}

static const std::unordered_map<ErrorCode, std::string> ec2str = {
  {ErrorCode::kOK, "success"},
  {ErrorCode::kUnknownOp, "unknown operation"},
  {ErrorCode::kIOFault, "I/O fault"},
  {ErrorCode::kInvalidPath, "invalid path"},
  {ErrorCode::kInvalidRange, "invalid value range"},
  {ErrorCode::kBranchExists, "branch already exists"},
  {ErrorCode::kBranchNotExists, "branch does not exist"},
  {ErrorCode::kReferringVersionNotExist, "referring version does not exist"},
  {ErrorCode::kUCellNotExists, "UCell does not exist"},
  {ErrorCode::kChunkNotExists, "chunk does not exist"},
  {ErrorCode::kStoreInfoUnavailable, "storage information is unavailable"},
  {ErrorCode::kTypeUnsupported, "unsupported data type"},
  {ErrorCode::kFailedCreateUCell, "failed to create UCell"},
  {ErrorCode::kFailedCreateSBlob, "failed to create SBlob"},
  {ErrorCode::kFailedCreateSString, "failed to create SString"},
  {ErrorCode::kFailedCreateSList, "failed to create SList"},
  {ErrorCode::kFailedCreateSMap, "failed to create SMap"},
  {ErrorCode::kFailedCreateSSet, "failed to create SSet"},
  {ErrorCode::kInconsistentKey, "inconsistent values of key"},
  {ErrorCode::kInvalidValue, "invalid value"},
  {ErrorCode::kFailedModifySBlob, "failed to modify SBlob"},
  {ErrorCode::kFailedModifySList, "failed to modify SList"},
  {ErrorCode::kFailedModifySMap, "failed to modify SMap"},
  {ErrorCode::kFailedModifySSet, "failed to modify SSet"},
  {ErrorCode::kIndexOutOfRange, "index out of range"},
  {ErrorCode::kObjectMetaError, "object meta error"},
  {ErrorCode::kObjectMetaNotExists, "object meta does not exist"},
  {ErrorCode::kObjectMetaIndicesMismatch, "indices mismatch against object meta"}, // NOLINT
  {ErrorCode::kTypeMismatch, "data types mismatch"},
  {ErrorCode::kKeyNotExists, "key does not exist"},
  {ErrorCode::kKeyExists, "key already exists"},
  {ErrorCode::kTableNotExists, "table or branch does not exist"},
  {ErrorCode::kEmptyTable, "table is empty"},
  {ErrorCode::kNotEmptyTable, "table is not empty"},
  {ErrorCode::kColumnNotExists, "column does not exist"},
  {ErrorCode::kRowNotExists, "row does not exist"},
  {ErrorCode::kRowExists, "row already exists"},
  {ErrorCode::kFailedOpenFile, "failed to open file"},
  {ErrorCode::kInvalidCommandArgument, "invalid command-line argument"},
  {ErrorCode::kUnknownCommand, "unrecognized command"},
  {ErrorCode::kInvalidSchema, "invalid schema"},
  {ErrorCode::kInconsistentType, "inconsistent data types"},
  {ErrorCode::kInvalidParameter, "invalid parameter"},
  {ErrorCode::kMapKeyNotExists, "key of map entry does not exist"},
  {ErrorCode::kMapKeyExists, "key of map entry already exists"},
  {ErrorCode::kElementExists, "element already exists"},
  {ErrorCode::kUnexpectedSuccess, "unexpected success of command execution"},
  {ErrorCode::kDatasetNotExists, "dataset or branch does not exist"},
  {ErrorCode::kDataEntryNotExists, "data entry does not exist"},
  {ErrorCode::kDatasetSchemaMismatch, "schema of dataset mismatch"},
  {ErrorCode::kDatasetSchemaNotFound, "schema of dataset is missing"},
  {ErrorCode::kIllegalDataEntryNameAttr, "illegal data entry name attribute"},
  {ErrorCode::kDataEntryNameIndicesUnknown, "indices of data entry name attributes are missing"}, // NOLINT
  {ErrorCode::kDataEntryNameIndicesMismatch, "indices of data entry name attributes mismatch"} // NOLINT
};

const std::string& Utils::ToString(const ErrorCode& ec) {
  auto it = ec2str.find(ec);
  static std::string unknown("<Unknown>");
  return it == ec2str.end() ? unknown : it->second;
}

std::vector<std::string> Utils::Tokenize(
  const std::string& str, const char* sep_chars, size_t hint_size) {
  using Tokenizer = boost::tokenizer<boost::char_separator<char>>;
  using CharSep = boost::char_separator<char>;
  std::vector<std::string> elems;
  elems.reserve(hint_size);
  for (const auto& t : Tokenizer(str, CharSep(sep_chars))) {
    elems.push_back(std::move(t));
  }
  return elems;
}

std::vector<std::string> Utils::Split(const std::string& str, char delim,
                                      size_t hint_size) {
  std::vector<std::string> elems;
  elems.reserve(hint_size);
  Split(str, delim, std::back_inserter(elems));
  return elems;
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
                  bool elem_in_quote, size_t limit, std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto it = list.Scan();
  os << lsymbol;
  if (!it.end()) {
    os << quote << it.value() << quote;
    size_t cnt(1);
    for (it.next(); !it.end() && cnt++ < limit; it.next()) {
      os << sep << quote << it.value() << quote;
    }
    size_t list_size = list.numElements();
    if (list_size > limit) {
      os << sep << "...(and " << (list_size - limit) << " more)";
    }
  }
  os << rsymbol;
}

void Utils::Print(const UMap& map, const std::string& lsymbol,
                  const std::string& rsymbol, const std::string& sep,
                  const std::string& lentry, const std::string& rentry,
                  const std::string& entry_sep, bool elem_in_quote,
                  size_t limit, std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto it = map.Scan();
  auto f_print_it = [&]() {
    os << lentry << quote << it.key() << quote << entry_sep << quote
       << it.value() << quote << rentry;
  };
  os << lsymbol;
  if (!it.end()) {
    f_print_it();
    size_t cnt(1);
    for (it.next(); !it.end() && cnt++ < limit; it.next()) {
      os << sep;
      f_print_it();
    }
    size_t map_size = map.numElements();
    if (map_size > limit) {
      os << sep << "...(and " << (map_size - limit) << " more)";
    }
  }
  os << rsymbol;
}

void Utils::Print(const USet& set, const std::string& lsymbol,
                  const std::string& rsymbol, const std::string& sep,
                  bool elem_in_quote, size_t limit, std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto it = set.Scan();
  auto f_print_it = [&]() {
    os << quote << it.key() << quote;
  };
  os << lsymbol;
  if (!it.end()) {
    f_print_it();
    size_t cnt(1);
    for (it.next(); !it.end() && cnt++ < limit; it.next()) {
      os << sep;
      f_print_it();
    }
    size_t set_size = set.numElements();
    if (set_size > limit) {
      os << sep << "...(and " << (set_size - limit) << " more)";
    }
  }
  os << rsymbol;
}

void Utils::PrintKeys(const UMap& map, const std::string& lsymbol,
                      const std::string& rsymbol, const std::string& sep,
                      bool elem_in_quote, size_t limit, std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto it = map.Scan();
  os << lsymbol;
  if (!it.end()) {
    os << quote << it.key() << quote;
    size_t cnt(1);
    for (it.next(); !it.end() && cnt++ < limit; it.next()) {
      os << sep << quote << it.key() << quote;
    }
    size_t n_elems = map.numElements();
    if (n_elems > limit) {
      os << sep << "...(and " << (n_elems - limit) << " more)";
    }
  }
  os << rsymbol;
}

void Utils::PrintKeysTransform(
  const UMap& map,
  const std::function<std::string(const Slice&)>& f_trans,
  const std::string& lsymbol,
  const std::string& rsymbol, const std::string& sep,
  bool elem_in_quote, size_t limit, std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto it = map.Scan();
  os << lsymbol;
  if (!it.end()) {
    os << quote << f_trans(it.key()) << quote;
    size_t cnt(1);
    for (it.next(); !it.end() && cnt++ < limit; it.next()) {
      os << sep << quote << f_trans(it.key()) << quote;
    }
    size_t n_elems = map.numElements();
    if (n_elems > limit) {
      os << sep << "...(and " << (n_elems - limit) << " more)";
    }
  }
  os << rsymbol;
}

void Utils::PrintKeys(const USet& set, const std::string& lsymbol,
                      const std::string& rsymbol, const std::string& sep,
                      bool elem_in_quote, std::ostream& os) {
  const auto quote = elem_in_quote ? "\"" : "";
  auto it = set.Scan();
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

void Utils::PrintPercentBar(double fraction, const std::string& front_symbol,
                            size_t width, const std::string& lsymbol,
                            const std::string& rsymbol, char progress_symbol,
                            std::ostream& os) {
  os << lsymbol;
  size_t progress_width =
    width - lsymbol.size() - rsymbol.size() - front_symbol.size();
  size_t n_progress_symbols = std::lround(fraction * progress_width);
  size_t i = 0;
  for (; i < n_progress_symbols; ++i) os << progress_symbol;
  os << front_symbol;
  for (i += front_symbol.size() - 1; i < progress_width; ++i) os << ' ';
  os << rsymbol;
}

std::string Utils::TimeString(double ms) {
  std::stringstream ss;
  if (ms < 60000.0) {
    double seconds = ms / 1000.0;
    ss << std::fixed << std::setprecision(3) << seconds << "s";
  } else if (ms < 3600000.0) {
    int minutes = ms / 60000.0;
    int minutes_ms = minutes * 60000.0;
    double seconds = (ms - minutes_ms) / 1000.0;
    ss << minutes << 'm'
       << std::fixed << std::setprecision(3) << seconds << "s";
  } else if (ms < 86400000.0) {
    int hours = ms / 3600000.0;
    double hours_ms = hours * 3600000.0;
    int minutes = (ms - hours_ms) / 60000.0;
    int minutes_ms = minutes * 60000.0;
    double seconds = (ms - hours_ms - minutes_ms) / 1000.0;
    ss << hours << 'h' << minutes << 'm'
       << std::fixed << std::setprecision(1) << seconds << "s";
  } else {
    int days = ms / 86400000.0;
    double days_ms = days * 86400000.0;
    int hours = (ms - days_ms) / 3600000.0;
    double hours_ms = hours * 3600000.0;
    int minutes = (ms - days_ms - hours_ms) / 60000.0;
    int minutes_ms = minutes * 60000.0;
    int seconds = (ms - days_ms - hours_ms - minutes_ms) / 1000.0;
    ss << days << 'd' << hours << 'h' << minutes << 'm' << seconds << "s";
  }
  return ss.str();
}

std::string Utils::StorageSizeString(size_t n_bytes) {
  static const size_t bytes_per_kb = 0x400;
  static const size_t bytes_per_mb = 0x100000;
  static const size_t bytes_per_gb = 0x40000000;
  static const size_t bytes_per_tb = 0x10000000000;
  static const size_t bytes_per_pb = 0x4000000000000;

  std::stringstream ss;
  if (n_bytes < bytes_per_kb) {
    ss << n_bytes << "B";
  } else if (n_bytes < bytes_per_mb) {
    ss << std::fixed << std::setprecision(1)
       << (static_cast<double>(n_bytes) / bytes_per_kb) << "KB";
  } else if (n_bytes < bytes_per_gb) {
    ss << std::fixed << std::setprecision(2)
       << (static_cast<double>(n_bytes) / bytes_per_mb) << "MB";
  } else if (n_bytes < bytes_per_tb) {
    ss << std::fixed << std::setprecision(3)
       << (static_cast<double>(n_bytes) / bytes_per_gb) << "GB";
  } else if (n_bytes < bytes_per_pb) {
    ss << std::fixed << std::setprecision(3)
       << (static_cast<double>(n_bytes) / bytes_per_tb) << "TB";
  } else {
    ss << std::fixed << std::setprecision(3)
       << (static_cast<double>(n_bytes) / bytes_per_pb) << "PB";
  }
  return ss.str();
}

ErrorCode Utils::GetFileContents(const std::string& file_path,
                                 std::string* buf) {
  std::ifstream ifs(file_path, std::ios::in | std::ios::binary);
  if (!ifs) return ErrorCode::kFailedOpenFile;

  ifs.seekg(0, std::ios::end);
  size_t sz = ifs.tellg();
  buf->resize(sz);
  ifs.seekg(0, std::ios::beg);
  ifs.read(&(buf->at(0)), sz);
  ifs.close();
  return ErrorCode::kOK;
}

ErrorCode Utils::IterateDirectory(
  const boost_fs::path& dir_path,
  const std::function<ErrorCode(
    const boost_fs::path& file_path,
    const boost_fs::path& rlt_path)>& f_manip_file,
  const boost_fs::path& init_rlt_path) {
  // define recursion
  const std::function<ErrorCode(
    const boost_fs::path&, const boost_fs::path&)> f_manip =
  [&](const boost_fs::path & path, const boost_fs::path & rlt_path) {
    if (boost_fs::is_symlink(path)) {
      USTORE_GUARD(
        f_manip(boost_fs::canonical(path), rlt_path));
    } else if (boost_fs::is_directory(path)) {
      // iterate sub-directory
      for (auto && file_entry : boost_fs::directory_iterator(path)) {
        const auto file_path = file_entry.path();
        USTORE_GUARD(
          f_manip(file_path, rlt_path / file_path.filename()));
      }
    } else if (boost_fs::is_regular_file(path)) {
      USTORE_GUARD(
        f_manip_file(path, rlt_path));
    } else {
      LOG(WARNING) << path
                   << " is not a directory, regular file or symbolic link";
    }
    return ErrorCode::kOK;
  };
  // launch recursion
  try {
    if (boost_fs::exists(dir_path) && boost_fs::is_directory(dir_path)) {
      return f_manip(dir_path, init_rlt_path);
    } else {
      LOG(ERROR) << "Invalid directory: " << dir_path;
      return ErrorCode::kInvalidPath;
    }
  } catch (const boost_fs::filesystem_error& e) {
    LOG(ERROR) << e.what();
    return ErrorCode::kIOFault;
  }
}

ErrorCode Utils::IterateFileByLine(
  const boost_fs::path& file_path,
  const std::function<ErrorCode(std::string& line)>& f_manip_line) {
  USTORE_GUARD(ValidateFilePath(file_path));
  std::ifstream ifs(file_path.native());
  USTORE_GUARD(ifs ? ErrorCode::kOK : ErrorCode::kFailedOpenFile);
  USTORE_GUARD(IterateInputStreamByLine(ifs, f_manip_line));
  ifs.close();
  return ErrorCode::kOK;
}

ErrorCode Utils::ValidateFilePath(const boost_fs::path& file_path) {
  try {
    if (boost_fs::exists(file_path) && boost_fs::is_regular_file(file_path)) {
      return ErrorCode::kOK;
    } else {
      LOG(ERROR) << "Invalid file: " << file_path;
      return ErrorCode::kInvalidPath;
    }
  } catch (const boost_fs::filesystem_error& e) {
    LOG(ERROR) << e.what();
    return ErrorCode::kIOFault;
  }
}

ErrorCode Utils::IterateInputStreamByLine(
  std::istream& is,
  const std::function<ErrorCode(std::string& line)>& f_manip_line) {
  std::string line;
  while (std::getline(is, line)) {
    USTORE_GUARD(
      f_manip_line(line));
  }
  return ErrorCode::kOK;
}

ErrorCode Utils::ExtractElementsWithCheck(
  const std::string& str,
  const std::vector<size_t>& idxs_elem,
  const std::function<ErrorCode(const std::string&)>& f_check,
  std::vector<std::string>* elems,
  const char delim) {
  elems->clear();
  auto tokens = Split(str, delim);
  for (auto& i : idxs_elem) {
    if (i >= tokens.size()) {
      LOG(ERROR) << "Failed to extract element from string: \"" << str << "\"";
      return ErrorCode::kIndexOutOfRange;
    }
    auto& e = tokens[i];
    USTORE_GUARD(f_check(e));
    elems->push_back(std::move(e));
  }
  return ErrorCode::kOK;
  // TODO(linqian): Replace the above approach by scanning the string and
  //                extract elements in the target positions.
}

ErrorCode Utils::CreateDirectories(const boost_fs::path& dir_path) {
  if (!dir_path.empty()) {
    try {
      boost_fs::create_directories(dir_path);
    } catch (const boost_fs::filesystem_error& e) {
      LOG(ERROR) << "Failed to create directories for path: "
                 << dir_path.native();
      return ErrorCode::kIOFault;
    }
  }
  return ErrorCode::kOK;
}

ErrorCode Utils::DeleteFile(const std::string& file_path) {
  if (std::remove(file_path.c_str()) != 0) {
    LOG(ERROR) << "Failed to delete file: " << file_path;
    return ErrorCode::kIOFault;
  }
  return ErrorCode::kOK;
}

ErrorCode Utils::CopyFile(const boost::filesystem::path& from,
                          const boost::filesystem::path& to,
                          boost_fs::copy_option opt) {
  boost_sys::error_code ec;
  boost_fs::copy_file(from, to, opt, ec);
  if (ec.value() != boost_sys::errc::success) {
    LOG(ERROR) << ec.message();
    return ErrorCode::kIOFault;
  }
  return ErrorCode::kOK;
}

ErrorCode Utils::ToIndices(const std::string& str,
                           std::vector<size_t>* indices) {
  indices->clear();
  for (auto& idx_str : Tokenize(str, "{}[]()|,;: ")) {
    auto i = std::stoi(idx_str);
    if (i < 0) {
      LOG(ERROR) << "Invalid index: " << i;
      return ErrorCode::kIndexOutOfRange;
    }
    indices->emplace_back(i);
  }
  return ErrorCode::kOK;
}

std::string Utils::RegularizeCSVLine(const std::string& line) {
  static const std::regex comma("\\s*(,)\\s*([^\\s]?)");
  return std::regex_replace(boost::trim_copy(line), comma, "$1$2");
}

std::string Utils::ReplaceSpace(const std::string& str,
                                const std::string& to_replace_space) {
  static const std::regex space("\\s+([^\\s]?|$)");
  return std::regex_replace(str, space, to_replace_space + "$1");
}

std::string Utils::RegularizeIntegers(const std::string& str) {
  static const std::regex trivial_int_zero("\\b[0]+([1-9]\\d*|0)\\b");
  return std::regex_replace(str, trivial_int_zero, "$1");
}

std::string Utils::RegularizeFloatNumbers(const std::string& str) {
  static const std::regex trivial_frac("\\b(\\d+)\\.[0]+\\b");
  static const std::regex trivial_frac_zero(
    "(\\b\\d+\\.|\\B\\.)(\\d*[1-9])[0]+\\b");
  return std::regex_replace(
           std::regex_replace(RegularizeIntegers(str), trivial_frac, "$1"),
           trivial_frac_zero, "$1$2");
}

const std::string& Utils::ErrorMessage(
  const std::regex_constants::error_type& ec) {
  static const std::string msg_error_collate("invalid collating element name");
  static const std::string msg_error_ctype("invalid character class name");
  static const std::string msg_error_escape("invalid escaped character or trailing escape"); // NOLINT
  static const std::string msg_error_backref("invalid back reference");
  static const std::string msg_error_brack("mismatched brackets ([ and ])");
  static const std::string msg_error_paren("mismatched parentheses (( and ))");
  static const std::string msg_error_brace("mismatched braces ({ and })");
  static const std::string msg_error_badbrace("invalid range between braces ({ and })"); // NOLINT
  static const std::string msg_error_range("invalid character range");
  static const std::string msg_error_space("insufficient memory to convert the expression info a finite state machine"); // NOLINT
  static const std::string msg_error_badrepeat("repeat specifier (one of *?+{) that was not preceded by a valid regular expression"); // NOLINT
  static const std::string msg_error_complexity("The complexity of an attempted match against a regular expression exceeded a pre-set level"); // NOLINT
  static const std::string msg_error_stack("insufficient memory to determine whether the regular expression could match the specified character sequence"); // NOLINT
  static const std::string unknown("<Unknown>");

  switch (ec) {
    case std::regex_constants::error_collate: return msg_error_collate;
    case std::regex_constants::error_ctype: return msg_error_ctype;
    case std::regex_constants::error_escape: return msg_error_escape;
    case std::regex_constants::error_backref: return msg_error_backref;
    case std::regex_constants::error_brack: return msg_error_brack;
    case std::regex_constants::error_paren: return msg_error_paren;
    case std::regex_constants::error_brace: return msg_error_brace;
    case std::regex_constants::error_badbrace: return msg_error_badbrace;
    case std::regex_constants::error_range: return msg_error_range;
    case std::regex_constants::error_space: return msg_error_space;
    case std::regex_constants::error_badrepeat: return msg_error_badrepeat;
    case std::regex_constants::error_complexity: return msg_error_complexity;
    case std::regex_constants::error_stack: return msg_error_stack;
    default: return unknown;
  }
}

}  // namespace ustore
