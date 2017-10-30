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

    AssignArgs(hidden_args_, vm);
    AssignArgs(hidden_bool_args_, vm);
    AssignArgs(hidden_int_args_, vm);
    AssignArgs(hidden_int64_args_, vm);
    AssignArgs(hidden_double_args_, vm);
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl;
    return false;
  }
  return CheckArgs();
}

bool Arguments::ParseCmdArgs(int argc, char* argv[], po::variables_map* vm) {
  po::options_description general_od(BLUE_STR("Options"), 120);
  AddBoolArgs(bool_args_, &general_od);
  AddArgs(args_, &general_od);
  AddArgs(int_args_, &general_od);
  AddArgs(int64_args_, &general_od);
  AddArgs(double_args_, &general_od);

  po::options_description backend_od("Hidden Options", 120);
  AddBoolArgs(hidden_bool_args_, &backend_od);
  AddArgs(hidden_args_, &backend_od);
  AddArgs(hidden_int_args_, &backend_od);
  AddArgs(hidden_int64_args_, &backend_od);
  AddArgs(hidden_double_args_, &backend_od);

  po::positional_options_description pos_od;
  for (auto& name_long : pos_arg_names_) {
    pos_od.add(name_long.c_str(), 1);
  }

  po::options_description all_od("Allowed Options");
  all_od.add(general_od).add(backend_od);

  po::options_description visible_od;
  visible_od.add(general_od);
  try {
    po::store(po::command_line_parser(argc, argv).options(all_od)
              .style(po::command_line_style::unix_style)
              .positional(pos_od).run(), *vm);
    if (vm->count("help")) {
      is_help = true;
      std::cout << visible_od << std::endl << MoreHelpMessage();
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
