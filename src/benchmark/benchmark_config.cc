// Copyright (c) 2017 The Ustore Authors.

#include <utility>

#include "benchmark/benchmark_config.h"

namespace ustore {

bool BenchmarkConfig::is_help = false;
int BenchmarkConfig::num_clients = 2;
int BenchmarkConfig::num_validations = 100;
int BenchmarkConfig::num_strings = 100000;
int BenchmarkConfig::num_blobs = 5000;
int BenchmarkConfig::string_len = 64;
int BenchmarkConfig::blob_size = 8192;

bool BenchmarkConfig::ParseCmdArgs(int argc, char* argv[]) {
  po::variables_map vm;
  GUARD(ParseCmdArgs(argc, argv, &vm));
  try {
    num_clients = vm["num-client"].as<int>();
    GUARD(CheckArgGT(num_clients, 0, "Number of clients"));

    num_validations = vm["num-validate"].as<int>();
    GUARD(CheckArgGT(num_validations, 0, "Number of validations"));

    num_strings = vm["num-string"].as<int>();
    GUARD(CheckArgGT(num_strings, 0, "Number of strings"));

    num_blobs = vm["num-blob"].as<int>();
    GUARD(CheckArgGT(num_blobs, 0, "Number of Blobs"));

    string_len = vm["string-len"].as<int>();
    GUARD(CheckArgGT(string_len, 0, "String length"));

    blob_size = vm["blob-size"].as<int>();
    GUARD(CheckArgGT(blob_size, 0, "Blob size"));
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
  ("num-validate,v", po::value<int>()->default_value(100),
   "number of validations")
  ("num-string,s", po::value<int>()->default_value(100000),
   "number of strings")
  ("num-blob,b", po::value<int>()->default_value(5000),
   "number of blobs")
  ("string-len,l", po::value<int>()->default_value(64),
   "string length")
  ("blob-size,z", po::value<int>()->default_value(8192));

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
