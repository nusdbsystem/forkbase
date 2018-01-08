// Copyright (c) 2017 The Ustore Authors.

#include <boost/algorithm/string.hpp>
#include <iomanip>
#include <utility>

#include "benchmark/bench_config.h"

namespace ustore {

std::string BenchmarkConfig::command;
std::string BenchmarkConfig::type;
bool BenchmarkConfig::is_help = false;
int BenchmarkConfig::num_clients;
// common
int BenchmarkConfig::validate_ops;
std::string BenchmarkConfig::default_branch;
bool BenchmarkConfig::suffix;
int BenchmarkConfig::suffix_range;
int BenchmarkConfig::ops_amplifier;
// string
int BenchmarkConfig::string_ops;
int BenchmarkConfig::string_length;
std::string BenchmarkConfig::string_key;
// blob
int BenchmarkConfig::blob_ops;
int BenchmarkConfig::blob_length;
std::string BenchmarkConfig::blob_key;
// list
int BenchmarkConfig::list_ops;
int BenchmarkConfig::list_length;
int BenchmarkConfig::list_elements;
std::string BenchmarkConfig::list_key;
// map
int BenchmarkConfig::map_ops;
int BenchmarkConfig::map_length;
int BenchmarkConfig::map_elements;
std::string BenchmarkConfig::map_key;
// branch
int BenchmarkConfig::branch_ops;
std::string BenchmarkConfig::branch_key;
// merge
int BenchmarkConfig::merge_ops;
std::string BenchmarkConfig::merge_key;

bool BenchmarkConfig::ParseCmdArgs(int argc, char* argv[]) {
  po::variables_map vm;
  GUARD(ParseCmdArgs(argc, argv, &vm));
  try {
    command = vm["command"].as<std::string>();
    boost::to_upper(command);
    type = vm["type"].as<std::string>();
    boost::to_lower(type);
    num_clients = vm["num-client"].as<int>();
    GUARD(CheckArgGT(num_clients, 0, "Number of clients"));
    // common
    validate_ops = vm["validate-ops"].as<int>();
    GUARD(CheckArgGE(validate_ops, 0, "Number of validate operations"));
    default_branch = vm["default-branch"].as<std::string>();
    GUARD(CheckArgGT(default_branch, "", "Default branch to operate on"));
    suffix = vm["suffix"].as<bool>();
    suffix_range = vm["suffix-range"].as<int>();
    GUARD(CheckArgGT(suffix_range, 0, "Key suffix range"));
    ops_amplifier = vm["ops-amplifier"].as<int>();
    GUARD(CheckArgGT(ops_amplifier, 0, "Operation number amplifier"));
    // string
    string_ops = vm["string-ops"].as<int>();
    GUARD(CheckArgGT(string_ops, 0, "Number of string related operations"));
    string_length = vm["string-length"].as<int>();
    GUARD(CheckArgGT(string_length, 0, "string length"));
    string_key = vm["string-key"].as<std::string>();
    GUARD(CheckArgGT(string_key, "", "Key of string related operations"));
    // blob
    blob_ops = vm["blob-ops"].as<int>();
    GUARD(CheckArgGT(blob_ops, 0, "Number of blob related operations"));
    blob_length = vm["blob-length"].as<int>();
    GUARD(CheckArgGT(blob_length, 0, "blob length"));
    blob_key = vm["blob-key"].as<std::string>();
    GUARD(CheckArgGT(blob_key, "", "Key of blob related operations"));
    // list
    list_ops = vm["list-ops"].as<int>();
    GUARD(CheckArgGT(list_ops, 0, "Number of list related operations"));
    list_length = vm["list-length"].as<int>();
    GUARD(CheckArgGT(list_length, 0, "Length of a list element"));
    list_elements = vm["list-elements"].as<int>();
    GUARD(CheckArgGT(list_elements, 0, "Number of elements in list"));
    list_key = vm["list-key"].as<std::string>();
    GUARD(CheckArgGT(list_key, "", "Key of list related operations"));
    // map
    map_ops = vm["map-ops"].as<int>();
    GUARD(CheckArgGT(map_ops, 0, "Number of map related operations"));
    map_length = vm["map-length"].as<int>();
    GUARD(CheckArgGT(map_length, 0, "Length of a map element"));
    map_elements = vm["map-elements"].as<int>();
    GUARD(CheckArgGT(map_elements, 0, "Number of elements in map"));
    map_key = vm["map-key"].as<std::string>();
    GUARD(CheckArgGT(map_key, "", "Key of map related operations"));
    // branch
    branch_ops = vm["branch-ops"].as<int>();
    GUARD(CheckArgGT(branch_ops, 0, "Number of branch related operations"));
    branch_key = vm["branch-key"].as<std::string>();
    GUARD(CheckArgGT(branch_key, "", "Key of branch related operations"));
    // merge
    merge_ops = vm["merge-ops"].as<int>();
    GUARD(CheckArgGT(merge_ops, 0, "Number of merge related operations"));
    merge_key = vm["merge-key"].as<std::string>();
    GUARD(CheckArgGT(merge_key, "", "Key of merge related operations"));
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl;
    return false;
  }
  return true;
}

#define FORMAT_BENCHMARK_CMD(cmd) \
  "* " << std::left << std::setw(8) << cmd << " "

bool BenchmarkConfig::ParseCmdArgs(int argc, char* argv[],
                                   po::variables_map* vm) {
  po::options_description desc(BLUE_STR("Options"), 120);
  desc.add_options()
  ("help,?", "print usage message")
  ("command", po::value<std::string>()->default_value("ALL"),
   "benchmark command")
  ("type,p", po::value<std::string>()->default_value("blob"),
   "data type")
  ("num-client,c", po::value<int>()->default_value(8),
   "number of clients")
  // common
  ("validate-ops", po::value<int>()->default_value(10),
   "number of validate operations")
  ("default-branch", po::value<std::string>()->default_value("master"),
   "default branch to operate on")
  ("suffix", po::value<bool>()->default_value(true),
   "add suffix to a key or not")
  ("suffix-range", po::value<int>()->default_value(100),
   "key suffix length")
  ("ops-amplifier", po::value<int>()->default_value(1),
   "operation number amplifier")
  // string
  ("string-ops", po::value<int>()->default_value(100000),
   "number of string related operations")
  ("string-length", po::value<int>()->default_value(256),
   "string length")
  ("string-key", po::value<std::string>()->default_value("STRING"),
   "key of string related operations")
  // blob
  ("blob-ops", po::value<int>()->default_value(5000),
   "number of blob related operations")
  ("blob-length", po::value<int>()->default_value(16 * 1024),
   "blob length")
  ("blob-key", po::value<std::string>()->default_value("BLOB"),
   "key of blob related operations")
  // list
  ("list-ops", po::value<int>()->default_value(5000),
   "number of list related operations")
  ("list-length", po::value<int>()->default_value(64),
   "length of a list element")
  ("list-elements", po::value<int>()->default_value(256),
   "number of elements in list")
  ("list-key", po::value<std::string>()->default_value("LIST"),
   "key of list related operations")
  // map
  ("map-ops", po::value<int>()->default_value(5000),
   "number of map related operations")
  ("map-length", po::value<int>()->default_value(64),
   "length of a map element")
  ("map-elements", po::value<int>()->default_value(256),
   "number of elements in map")
  ("map-key", po::value<std::string>()->default_value("MAP"),
   "key of map related operations")
  // branch
  ("branch-ops", po::value<int>()->default_value(100000),
   "number of branch related operations")
  ("branch-key", po::value<std::string>()->default_value("BLOB"),
   "key of branch related operations")
  // merge
  ("merge-ops", po::value<int>()->default_value(20000),
   "number of merge related operations")
  ("merge-key", po::value<std::string>()->default_value("BLOB"),
   "key of merge related operations");

  po::positional_options_description pos_opts;
  pos_opts.add("command", 1);
  pos_opts.add("type", 1);

  try {
    po::store(po::command_line_parser(argc, argv).options(desc)
              .style(po::command_line_style::unix_style)
              .positional(pos_opts).run(), *vm);
    if (vm->count("help")) {
      is_help = true;
      auto& os = std::cout;
      os << BLUE("Benchmark Commands") << ":" << std::endl
         << FORMAT_BENCHMARK_CMD("ALL") << std::endl
         << FORMAT_BENCHMARK_CMD("PUT") << "-p <type>" << std::endl
         << FORMAT_BENCHMARK_CMD("GET") << "-p <type>" << std::endl
         << FORMAT_BENCHMARK_CMD("BRANCH") << std::endl
         << FORMAT_BENCHMARK_CMD("MERGE") << std::endl
         << std::endl
         << desc << std::endl;
    }
    po::notify(*vm);
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl << std::endl;
    return false;
  }
  return true;
}

}  // namespace ustore
