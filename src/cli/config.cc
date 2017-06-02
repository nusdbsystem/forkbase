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
std::string Config::table = "";
std::string Config::column = "";

void Config::Reset() {
  is_help = false;
  command = "";
  key = "";
  value = "";
  branch = "";
  ref_branch = "";
  version = "";
  ref_version = "";
  table = "";
  column = "";
}

bool Config::ParseCmdArgs(int argc, char* argv[]) {
  Reset();
  po::variables_map vm;
  GUARD(ParseCmdArgs(argc, argv, &vm));

  auto arg_command = vm["command"].as<std::string>();
  Command::Normalize(&arg_command);
  command = std::move(arg_command);

  key = vm["key"].as<std::string>();
  value = vm["value"].as<std::string>();

  branch = vm["branch"].as<std::string>();
  ref_branch = vm["ref-branch"].as<std::string>();

  version = vm["version"].as<std::string>();
  GUARD(CheckArg(version.size(), version.empty() || version.size() == 32,
                 "Length of the operating version", "0 or 32"));

  ref_version = vm["ref-version"].as<std::string>();
  GUARD(CheckArg(ref_version.size(),
                 ref_version.empty() || ref_version.size() == 32,
                 "Length of the reffering version", "0 or 32"));

  table = vm["table"].as<std::string>();
  column = vm["column"].as<std::string>();

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
  ("ref-branch,c", po::value<std::string>()->default_value(""),
   "the referring branch")
  ("version,v", po::value<std::string>()->default_value(""),
   "the operating version")
  ("ref-version,u", po::value<std::string>()->default_value(""),
   "the referring version")
  ("table,t", po::value<std::string>()->default_value(""),
   "table name")
  ("column,a", po::value<std::string>()->default_value(""),
   "column name")
  ("help,?", "print usage message");

  po::positional_options_description pos_opts;
  pos_opts.add("command", 1);

  auto f_print_help = [&desc]() {
    Command::PrintCommandHelp();
    std::cout << std::endl << desc << std::endl;
  };

  try {
    po::store(po::command_line_parser(argc, argv).options(desc)
              .style(po::command_line_style::unix_style)
              .positional(pos_opts).run(), *vm);
    if (vm->count("help")) {
      is_help = true;
      f_print_help();
      return false;
    }
    po::notify(*vm);
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl << std::endl;
    f_print_help();
    return false;
  }
  return true;
}

}  // namespace cli
}  // namespace ustore
