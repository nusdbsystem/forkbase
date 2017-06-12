// Copyright (c) 2017 The Ustore Authors.

#include <boost/algorithm/string.hpp>
#include <fstream>
#include <iostream>
#include <iomanip>
#include "cli/console.h"

namespace ustore {
namespace cli {

Console::Console(DB* db) noexcept
  : Command(db) {
  // console-specific commands
  CMD_HANDLER("HISTORY", ExecHistory);
  CMD_HANDLER("DUMP_HISTORY", ExecDumpHistory);
  CMD_ALIAS("DUMP_HISTORY", "DUMP-HISTORY");
}

const int kPrintConsoleCmdWidth = 20;

#define FORMAT_CONSOLE_CMD(cmd) \
  "* " << std::left << std::setw(kPrintConsoleCmdWidth) << cmd << " "

void Console::PrintConsoleCommandHelp(std::ostream& os) {
  os << BLUE("Console-specific Commands") << ":" << std::endl
     << FORMAT_CONSOLE_CMD("HISTORY") << std::endl
     << FORMAT_CONSOLE_CMD("DUMP_HISTORY") << "<file>" << std::endl;
}

ErrorCode Console::Run() {
  std::cout << "Welcome to UStore console." << std::endl
            << "Type in commands to have them evaluated." << std::endl
            << "Type \"--help\" for more information." << std::endl
            << "Type \"q\" or \"quit\" to exit." << std::endl << std::endl;
  std::string line;
  std::vector<std::string> args;
  while (true) {
    std::cout << YELLOW("ustore> ");
    std::getline(std::cin, line);
    boost::trim(line);
    if (line.empty()) continue;
    if (line == "q" || line == "quit") break;
    history_.emplace_back(line);

    if (!Utils::TokenizeArgs(line, &args)) { // tokenize command line
      std::cerr << BOLD_RED("[ERROR] ") << "Illegal command line"
                << std::endl << std::endl;
    } else if (!Config::ParseCmdArgs(args)) { // parse command-line arguments
      std::cerr << BOLD_RED("[ERROR] ")
                << "Found invalid command-line option" << std::endl;
    } else if (Config::is_help) { // print help message, if specified
      PrintCommandHelp();
      std::cout << std::endl;
      PrintConsoleCommandHelp();
      std::cout << std::endl << Config::command_options_help;
    } else if (!Config::script.empty()) { // prohibit running with script
      std::cerr << BOLD_RED("[ERROR] ") << "\"--script\" cannot be used in "
                << "UStore console" << std::endl;
    } else if (Config::command.empty()) { // validate the existence of command
      std::cerr << BOLD_RED("[ERROR] ") << "UStore command is missing"
                << std::endl;
    } else { // everything is ready, go!
      ExecCommand(Config::command);
    }
    std::cout << std::endl;
  }
  return ErrorCode::kOK;
}

ErrorCode Console::ExecHistory() {
  int line_num_width = 1;
  for (size_t num = history_.size(); num != 0; num /= 10) ++line_num_width;
  size_t cnt_line = 0;
  for (auto& line : history_) {
    std::cout << std::right << std::setw(line_num_width) << ++cnt_line
              << "  " << line << std::endl;
  }
  return ErrorCode::kOK;
}

ErrorCode Console::ExecDumpHistory() {
  const auto& file_path = Config::file;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: DUMP_HISTORY] ")
              << "File: \"" << file_path << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << BOLD_GREEN("[SUCCESS: DUMP_HISTORY] ")
              << "History of command lines has been exported" << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: DUMP_HISTORY] ")
              << "File: \"" << file_path << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (file_path.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  std::ofstream ofs(file_path);
  if (!ofs) {
    f_rpt_fail(ErrorCode::kFailedOpenFile);
    return ErrorCode::kFailedOpenFile;
  }
  for (auto& line : history_) ofs << line << std::endl;
  ofs.close();
  f_rpt_success();
  return ErrorCode::kOK;
}

}  // namespace cli
}  // namespace ustore
