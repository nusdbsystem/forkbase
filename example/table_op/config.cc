// Copyright (c) 2017 The Ustore Authors.

#include <boost/algorithm/string.hpp>

#include "config.h"

namespace ustore {
namespace example {
namespace table_op {

bool Config::is_help;
std::string Config::file;

bool Config::ParseCmdArgs(int argc, char* argv[]) {
  po::variables_map vm;
  GUARD(ParseCmdArgs(argc, argv, &vm));
  try {
    file = vm["file"].as<std::string>();
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl;
    return false;
  }
  return true;
}

bool Config::ParseCmdArgs(int argc, char* argv[], po::variables_map* vm) {
  po::options_description desc(BLUE_STR("Allowed Options"), 120);
  desc.add_options()
  ("help,?", "print usage message")
  ("file,f", po::value<std::string>()->default_value(""), "path of input file");

  try {
    po::store(po::parse_command_line(argc, argv, desc), *vm);
    if (vm->count("help")) {
      is_help = true;
      std::cout << desc << std::endl;
    }
    po::notify(*vm);
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl;
    return false;
  }
  return true;
}

}  // namespace table_op
}  // namespace example
}  // namespace ustore
