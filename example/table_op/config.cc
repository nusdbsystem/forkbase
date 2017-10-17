// Copyright (c) 2017 The Ustore Authors.

#include <boost/algorithm/string.hpp>

#include "config.h"

namespace ustore {
namespace example {
namespace table_op {

bool Config::is_help = false;
std::string Config::file = "";
std::string Config::update_ref_col = "";
std::string Config::update_ref_val = "";
std::string Config::update_eff_col = "";
std::string Config::agg_col = "";

bool Config::ParseCmdArgs(int argc, char* argv[]) {
  po::variables_map vm;
  GUARD(ParseCmdArgs(argc, argv, &vm));
  try {
    file = vm["file"].as<std::string>();
    update_ref_col = vm["update-ref-col"].as<std::string>();
    update_ref_val = vm["update-ref-val"].as<std::string>();
    update_eff_col = vm["update-eff-col"].as<std::string>();
    agg_col = vm["aggregate-col"].as<std::string>();
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
  ("file,f", po::value<std::string>()->default_value(""),
   "path of input file")
  ("update-ref-col,r", po::value<std::string>()->default_value(""),
   "name of update-referring column")
  ("update-ref-val,v", po::value<std::string>()->default_value(""),
   "path of update-referring-value file")
  ("update-eff-col,e", po::value<std::string>()->default_value(""),
   "name of update-effecting column")
  ("aggregate-col,a", po::value<std::string>()->default_value(""),
   "name of aggregating column");

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
