// Copyright (c) 2017 The Ustore Authors.

#include <utility>

#include "benchmark/bench_config.h"

namespace ustore {

bool BenchmarkConfig::is_help = false;
int BenchmarkConfig::num_clients = 2;
  // common
int BenchmarkConfig::validate_ops = 10;
std::string BenchmarkConfig::default_branch = "master";
bool BenchmarkConfig::suffix = true;
int BenchmarkConfig::suffix_range = 100;
// string
int BenchmarkConfig::string_ops = 5000;
int BenchmarkConfig::string_length = 128;
std::string BenchmarkConfig::string_prefix = "String";
// blob
int BenchmarkConfig::blob_ops = 100;
int BenchmarkConfig::blob_length = 16 * 1024;
std::string BenchmarkConfig::blob_prefix = "Blob";
// list
int BenchmarkConfig::list_ops = 100;
int BenchmarkConfig::list_length = 64;
int BenchmarkConfig::list_elements = 256;
std::string BenchmarkConfig::list_prefix = "List";
// map
int BenchmarkConfig::map_ops = 100;
int BenchmarkConfig::map_length = 64;
int BenchmarkConfig::map_elements = 256;
std::string BenchmarkConfig::map_prefix = "Map";
// branch
int BenchmarkConfig::branch_ops = 1000;
std::string BenchmarkConfig::branch_prefix = "Blob";
// merge
int BenchmarkConfig::merge_ops = 1000;
std::string BenchmarkConfig::merge_prefix = "Blob";

bool BenchmarkConfig::ParseCmdArgs(int argc, char* argv[]) {
  po::variables_map vm;
  GUARD(ParseCmdArgs(argc, argv, &vm));
  try {
    num_clients = vm["num-client"].as<int>();
    GUARD(CheckArgGT(num_clients, 0, "Number of clients"));

    // validate_ops = vm["validate-ops"].as<int>();
    // GUARD(CheckArgGT(validate_ops, 0, "Number of validate operations"));

  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl;
    return false;
  }
  return true;
}

bool BenchmarkConfig::ParseCmdArgs(int argc, char* argv[],
                                   po::variables_map* vm) {
  po::options_description desc(BLUE_STR("Options"), 120);
  desc.add_options()
  ("help,?", "print usage message")
  ("num-client,c", po::value<int>()->default_value(2),
   "number of clients")
  // ("num-validate,v", po::value<int>()->default_value(100),
  //  "number of validations")
  // ("num-string,s", po::value<int>()->default_value(100000),
  //  "number of strings")
  // ("num-blob,b", po::value<int>()->default_value(5000),
  //  "number of blobs")
  // ("string-len,l", po::value<int>()->default_value(64),
  //  "string length")
  // ("blob-size,z", po::value<int>()->default_value(8192));
  ;

  try {
    po::store(po::command_line_parser(argc, argv).options(desc)
              .style(po::command_line_style::unix_style).run(), *vm);
    if (vm->count("help")) {
      is_help = true;
      std::cout << desc << std::endl;
    }
    po::notify(*vm);
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl << std::endl;
    return false;
  }
  return true;
}

}  // namespace ustore
