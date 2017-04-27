// Copyright (c) 2017 The Ustore Authors.

#include "config.h"

namespace ustore {
namespace example {
namespace ca {

const WorkerID Config::kWorkID = 43;
const size_t Config::kDefaultNumColumns = 3;
const size_t Config::kDefaultNumRecords = 10;
const size_t Config::kDefaultNumIterations = 1000;
const double Config::kDefaultProbability = 0.01;

bool Config::is_help = false;
size_t Config::n_columns = Config::kDefaultNumColumns;
size_t Config::n_records = Config::kDefaultNumRecords;
double Config::p = Config::kDefaultProbability;
size_t Config::iters = Config::kDefaultNumIterations;

bool Config::ParseCmdArgs(const int& argc, char* argv[]) {
  po::variables_map vm;
  if (!ParseCmdArgs(argc, argv, vm)) return false;

  n_columns = vm["columns"].as<size_t>();
  if (!CheckArgGE(n_columns, 3, "Number of columns")) return false;

  n_records = vm["records"].as<size_t>();
  if (!CheckArgGT(n_records, 0, "Number of records")) return false;

  p = vm["probability"].as<double>();
  if (!CheckArgInRange(p, 0, 1, "Probability")) return false;

  iters = vm["iterations"].as<size_t>();
  if (!CheckArgGT(iters, 0, "Number of iterations")) return false;

  return true;
}

bool Config::ParseCmdArgs(const int& argc, char* argv[],
                          po::variables_map& vm) {
  try {
    po::options_description desc("Allowed options", 120);
    desc.add_options()
    ("help,?", "print usage message")
    ("columns,c", po::value<size_t>()->default_value(kDefaultNumColumns),
     "number of columns in a simple table")
    ("records,n", po::value<size_t>()->default_value(kDefaultNumRecords),
     "number of records in a simple table")
    ("probability,p", po::value<double>()->default_value(kDefaultProbability),
     "probability used in the analytical simulation")
    ("iterations,i", po::value<size_t>()->default_value(kDefaultNumIterations),
     "number of iterations in the analytical simulation")
    ;

    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help")) {
      is_help = true;
      std::cout << desc << std::endl;
      return false;
    }
  } catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    return false;
  }
  return true;
}

} // namespace ca
} // namespace example
} // namespace ustore
