// Copyright (c) 2017 The Ustore Authors.

#include <utility>

#include "cli/command.h"
#include "cli/config.h"

namespace ustore {
namespace cli {

bool Config::is_help = false;
std::string Config::command = "";
std::string Config::key = "";
std::string Config::value = "";
std::string Config::branch = "";
std::string Config::ref_branch = "";
std::string Config::version = "";
std::string Config::ref_version = "";
std::string Config::ref_version2 = "";

void Config::Reset() {
  is_help = false;
  command = "";
  key = "";
  value = "";
  branch = "";
  ref_branch = "";
  version = "";
  ref_version = "";
  ref_version2 = "";
}

bool Config::ParseCmdArgs(int argc, char* argv[]) {
  Reset();
  po::variables_map vm;
  GUARD(ParseCmdArgs(argc, argv, &vm));

  auto arg_command = vm["command"].as<std::string>();
  Command::Normalize(&arg_command);
  GUARD(Command::IsValid(arg_command));
  command = std::move(arg_command);

  key = vm["key"].as<std::string>();
  value = vm["value"].as<std::string>();

  branch = vm["branch"].as<std::string>();
  ref_branch = vm["ref-branch"].as<std::string>();

  version = vm["version"].as<std::string>();
  ref_version = vm["ref-version"].as<std::string>();
  ref_version2 = vm["ref-version2"].as<std::string>();

  return true;
}

bool Config::ParseCmdArgs(int argc, char* argv[], po::variables_map* vm) {
  po::options_description desc("Options", 120);
  desc.add_options()
  ("command", po::value<std::string>()->required(),
   "UStore command [REQUIRED]")
  ("key,k", po::value<std::string>()->default_value(""),
   "key of data")
  ("value,x", po::value<std::string>()->default_value(""),
   "data value")
  ("branch,b", po::value<std::string>()->default_value(""),
   "the operating branch")
  ("ref-branch", po::value<std::string>()->default_value(""),
   "the referring branch")
  ("version,v", po::value<std::string>()->default_value(""),
   "the operating version")
  ("ref-version", po::value<std::string>()->default_value(""),
   "the referring version")
  ("ref-version2", po::value<std::string>()->default_value(""),
   "the second referring version")
  ("help,?", "print usage message");

  po::positional_options_description pos_opts;
  pos_opts.add("command", 1);

  try {
    po::store(po::command_line_parser(argc, argv).options(desc)
              .positional(pos_opts).run(), *vm);
    if (vm->count("help")) {
      is_help = true;
      std::cout << desc << std::endl;
      return false;
    }
    po::notify(*vm);
  } catch (std::exception& e) {
    std::cerr << "[ERROR] " << e.what() << std::endl << std::endl
              << desc << std::endl;
    return false;
  }
  return true;
}

}  // namespace cli
}  // namespace ustore
