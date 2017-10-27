// Copyright (c) 2017 The Ustore Authors.

#include <boost/algorithm/string.hpp>

#include "utils/arguments.h"

namespace ustore {

bool Arguments::ParseCmdArgs(int argc, char* argv[]) {
  po::variables_map vm;
  GUARD(ParseCmdArgs(argc, argv, &vm));
  if (is_help) return true;
  try {
    AssignArgs(args_, vm);
    AssignArgs(bool_args_, vm);
    AssignArgs(int_args_, vm);
    AssignArgs(int64_args_, vm);
    AssignArgs(double_args_, vm);
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl;
    return false;
  }
  return CheckArgs();
}

bool Arguments::ParseCmdArgs(int argc, char* argv[], po::variables_map* vm) {
  po::options_description od(BLUE_STR("Options"), 120);
  AddBoolArgs(bool_args_, &od);
  AddArgs(args_, &od);
  AddArgs(int_args_, &od);
  AddArgs(int64_args_, &od);
  AddArgs(double_args_, &od);

  po::positional_options_description pos_od;
  for (auto& name_long : pos_arg_names_) {
    pos_od.add(name_long.c_str(), 1);
  }
  try {
    po::store(po::command_line_parser(argc, argv).options(od)
              .style(po::command_line_style::unix_style)
              .positional(pos_od).run(), *vm);
    if (vm->count("help")) {
      is_help = true;
      po::options_description visible;
      visible.add(od);
      std::cout << visible << std::endl << MoreHelpMessage();
    }
    po::notify(*vm);
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl;
    return false;
  }
  return true;
}

bool Arguments::ParseCmdArgs(const std::vector<std::string>& args) {
  size_t argc = args.size() + 1;
  static char dummy_cmd[] = "CMD";
  char* argv[argc];
  argv[0] = dummy_cmd;
  for (size_t i = 1, j = 0; i < argc; ++i, ++j) {
    auto& arg = args[j];
    argv[i] = new char[arg.size() + 1];
    std::strcpy(argv[i], arg.c_str());
  }
  auto ec = ParseCmdArgs(argc, argv);
  for (size_t i = 1; i < argc; ++i) delete argv[i];
  return ec;
}

void Arguments::AddBoolArgs(const std::vector<Meta<bool>>& args,
                            po::options_description* od) {
  for (auto& meta : args) {
    auto& name_long = meta.name_long;
    auto& name_short = meta.name_short;
    auto cfg = name_long + (name_short.empty() ? "" : "," + name_short);
    auto& desc = meta.desc;

    od->add_options()(cfg.c_str(), desc.c_str());
  }
}

}  // namespace ustore
