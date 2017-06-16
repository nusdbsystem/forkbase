// Copyright (c) 2017 The Ustore Authors.

#include <boost/algorithm/string.hpp>
#include <fstream>
#include <regex>
#include "cli/console.h"

namespace ustore {
namespace cli {

Console::Console(DB* db) noexcept : Command(db) {
  console_commands_.insert("HELP");
  console_commands_.insert("INFO");
  console_commands_.insert("STATE");
  console_commands_.insert("STATUS");
  console_commands_.insert("STAT");
  // console-specific commands
  CONSOLE_CMD_HANDLER("HISTORY", ExecHistory);
  CONSOLE_CMD_HANDLER("DUMP_HISTORY", ExecDumpHistory);
  CONSOLE_CMD_ALIAS("DUMP_HISTORY", "DUMP-HISTORY");
}

const int kPrintConsoleCmdWidth = 20;

#define FORMAT_CONSOLE_CMD(cmd) FORMAT_CMD(cmd, kPrintConsoleCmdWidth)

void Console::PrintConsoleCommandHelp(std::ostream& os) {
  os << BLUE("UStore Console Commands") << ":" << std::endl
     << FORMAT_CONSOLE_CMD("HISTORY") << std::endl
     << FORMAT_CONSOLE_CMD("!!") << "{...}" << std::endl
     << FORMAT_CONSOLE_CMD("!<number>") << "{...}" << std::endl
     << FORMAT_CONSOLE_CMD("!-<number>") << "{...}" << std::endl
     << FORMAT_CONSOLE_CMD("DUMP_HISTORY") << "<file>" << std::endl;
}

void Console::PrintHelp() {
  DCHECK(Config::is_help);
  std::cout << BOLD_GREEN("Usage") << ": "
            << "<command> {{<option>} <argument|file> ...}" << std::endl
            << std::endl;
  PrintCommandHelp();
  std::cout << std::endl;
  PrintConsoleCommandHelp();
  std::cout << std::endl << Config::command_options_help;
}

ErrorCode Console::Run() {
  std::cout << "Welcome to UStore console." << std::endl
            << "Type in commands to have them evaluated." << std::endl
            << "Type \"help\" for more information." << std::endl
            << "Type \"q\" or \"quit\" to exit." << std::endl << std::endl;
  std::string line;
  std::vector<std::string> args;
  while (true) {
    std::cout << YELLOW("ustore> ");
    std::getline(std::cin, line);
    boost::trim(line);
    if (line.empty()) continue;
    if (line == "q" || line == "quit") break;
    if (boost::starts_with(line, "!")) ReplaceWithHistory(line);
    if (!line.empty()) {
      history_.push_back(std::move(line));
      Run(history_.back());
    }
    std::cout << std::endl;
  }
  return ErrorCode::kOK;
}

void Console::Run(const std::string& cmd_line) {
  std::vector<std::string> args;
  if (!Utils::TokenizeArgs(cmd_line, &args)) {  // tokenize command line
    std::cerr << BOLD_RED("[ERROR] ") << "Illegal command line"
              << std::endl;
    MarkCurrentCommandLineToComment();
  } else if (!Config::ParseCmdArgs(args)) {  // parse command-line arguments
    std::cerr << BOLD_RED("[ERROR] ")
              << "Found invalid command-line option" << std::endl;
    MarkCurrentCommandLineToComment();
  } else if (Config::is_help) {  // print help message, if specified
    PrintHelp();
    MarkCurrentCommandLineToComment();
  } else if (!Config::script.empty()) {  // prohibit running with script
    std::cerr << BOLD_RED("[ERROR] ") << "\"--script\" cannot be used in "
              << "UStore console" << std::endl;
    MarkCurrentCommandLineToComment();
  } else if (Config::command.empty()) {  // validate the existence of command
    std::cerr << BOLD_RED("[ERROR] ") << "UStore command is missing"
              << std::endl;
    MarkCurrentCommandLineToComment();
  } else {  // everything is ready, go!
    auto ec = ExecCommand(Config::command);
    if (ec != ErrorCode::kOK ||
        console_commands_.find(Config::command) != console_commands_.end()) {
      MarkCurrentCommandLineToComment();
    }
  }
}

bool Console::ReplaceWithHistory(std::string& cmd_line) {
  DCHECK(boost::starts_with(cmd_line, "!"));
  const auto origin = cmd_line;
  cmd_line.clear();
  const auto f_set_cmd_line = [&cmd_line](const std::string && content) {
    cmd_line = content;
    std::cout << cmd_line << std::endl;
  };
  // refer to the previous command
  if (boost::starts_with(origin, "!!")) {
    if (!history_.empty()) {
      f_set_cmd_line(history_.back() + origin.substr(2));
      return true;
    }
    std::cerr << BOLD_RED("[FAILURE] ")
              << "Command history is empty" << std::endl;
    return false;
  }
  // refer to the specified command line in history
  static const std::regex rgx("^!([-]?[1-9][0-9]*)(.*$)");
  std::smatch match;
  if (std::regex_match(origin, match, rgx)) {
    int num = std::stoi(match[1]);
    auto residue = match[2].str();
    auto history_size = history_.size();
    if (0 < num && num <= history_size) {
      f_set_cmd_line(history_[num - 1] + residue);
      return true;
    }
    if (-history_size <= num && num < 0) {
      f_set_cmd_line(history_[history_size + num] + residue);
      return true;
    }
    std::cerr << BOLD_RED("[FAILURE] ")
              << "!" << num << ": event not found" << std::endl;
    return false;
  }
  // history substitution
  const auto hint = origin.substr(1);
  for (auto rit = history_.rbegin(); rit != history_.rend(); ++rit) {
    if (boost::starts_with(*rit, hint)) {
      f_set_cmd_line(std::string(*rit));
      return true;
    }
  }
  std::cerr << BOLD_RED("[FAILURE] ")
            << origin << ": event not found" << std::endl;
  return false;
}

void Console::MarkCurrentCommandLineToComment() {
  auto line_num = history_.size();
  CHECK(comment_history_lines.empty() ||
        line_num > comment_history_lines.back());
  comment_history_lines.emplace_back(line_num);
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
  auto it_cmt = comment_history_lines.begin();
  for (size_t i = 0; i < history_.size() - 1; ++i) {
    if (it_cmt != comment_history_lines.end() && (i + 1) == *it_cmt) {
      ofs << "# ";
      ++it_cmt;
    }
    ofs << history_[i] << std::endl;
  }
  DCHECK(it_cmt == comment_history_lines.end());
  ofs.close();
  f_rpt_success();
  return ErrorCode::kOK;
}

}  // namespace cli
}  // namespace ustore
