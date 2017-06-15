// Copyright (c) 2017 The Ustore Authors.

#include <boost/algorithm/string.hpp>
#include <utility>

#include "cli/config.h"

namespace ustore {
namespace cli {

bool Config::is_help = false;
std::string Config::command_options_help = "";
std::string Config::command = "";
std::string Config::script = "";
std::string Config::file = "";
std::string Config::key = "";
std::string Config::value = "";
std::string Config::branch = "";
std::string Config::ref_branch = "";
std::string Config::version = "";
std::string Config::ref_version = "";
std::string Config::table = "";
std::string Config::ref_table = "";
std::string Config::column = "";
std::string Config::ref_column = "";

void Config::Reset() {
  is_help = false;
  command_options_help = "";
  command = "";
  script = "";
  file = "";
  key = "";
  value = "";
  branch = "";
  ref_branch = "";
  version = "";
  ref_version = "";
  table = "";
  ref_table = "";
  column = "";
  ref_column = "";
}

bool Config::ParseCmdArgs(int argc, char* argv[]) {
  Reset();
  po::variables_map vm;
  GUARD(ParseCmdArgs(argc, argv, &vm));
  try {
    command = vm["command"].as<std::string>();
    boost::to_upper(command);

    script = vm["script"].as<std::string>();
    file = vm["file"].as<std::string>();

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
    ref_table = vm["ref-table"].as<std::string>();

    column = vm["column"].as<std::string>();
    ref_column = vm["ref-column"].as<std::string>();
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl;
    return false;
  }
  return true;
}

bool Config::ParseCmdArgs(int argc, char* argv[], po::variables_map* vm) {
  po::options_description desc(BLUE_STR("Options"), 120);
  desc.add_options()
  ("help,?", "print usage message")
  ("command", po::value<std::string>()->default_value(""),
   "UStore command")
  ("script", po::value<std::string>()->default_value(""),
   "script of UStore commands")
  ("file", po::value<std::string>()->default_value(""),
   "path of input/output file")
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
   "the operating table")
  ("ref-table,s", po::value<std::string>()->default_value(""),
   "the referring table")
  ("column,m", po::value<std::string>()->default_value(""),
   "the operating column")
  ("ref-column,n", po::value<std::string>()->default_value(""),
   "the referring column");

  po::positional_options_description pos_opts;
  pos_opts.add("command", 1);
  pos_opts.add("file", 1);

  try {
    po::store(po::command_line_parser(argc, argv).options(desc)
              .style(po::command_line_style::unix_style)
              .positional(pos_opts).run(), *vm);
    if (vm->count("help")) {
      is_help = true;
      std::stringstream ss;
      ss << desc;
      command_options_help = ss.str();
    }
    po::notify(*vm);
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl << std::endl;
    return false;
  }
  return true;
}

bool Config::ParseCmdArgs(const std::vector<std::string>& args) {
  int argc = args.size() + 1;
  static char dummy_cmd[] = "ustore_cli";
  char* argv[argc];
  argv[0] = dummy_cmd;
  for (size_t i = 1, j = 0; i < argc; ++i, ++j) {
    auto& arg = args[j];
    argv[i] = new char[arg.size()];
    std::strcpy(argv[i], arg.c_str());
  }
  auto ec = ParseCmdArgs(argc, argv);
  for (size_t i = 1; i < argc; ++i) delete argv[i];
  return ec;
}

}  // namespace cli
}  // namespace ustore
