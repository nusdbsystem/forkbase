// Copyright (c) 2017 The Ustore Authors.

#include <boost/algorithm/string.hpp>

#include "utils/argument.h"

namespace ustore {

Argument::Argument() noexcept : is_help(false) {}

bool Argument::ParseCmdArgs(int argc, char* argv[]) {
  po::variables_map vm;
  GUARD(ParseCmdArgs(argc, argv, &vm));
  try {
    AssignArgs(args_, vm);
    AssignArgs(bool_args_, vm);
    AssignArgs(int_args_, vm);
    AssignArgs(int64_args_, vm);
    AssignArgs(size_args_, vm);
    AssignArgs(double_args_, vm);
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl;
    return false;
  }
  return CheckArgs();
}

bool Argument::ParseCmdArgs(int argc, char* argv[], po::variables_map* vm) {
  po::options_description od(BLUE_STR("Options"), 120);
  od.add_options()("help,?", "print usage message");
  AddArgs(args_, &od);
  AddArgs(bool_args_, &od);
  AddArgs(int_args_, &od);
  AddArgs(int64_args_, &od);
  AddArgs(size_args_, &od);
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
      std::cout << visible << std::endl;
    }
    po::notify(*vm);
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl;
    return false;
  }
  return true;
}

bool Argument::ParseCmdArgs(const std::vector<std::string>& args) {
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

}  // namespace ustore
