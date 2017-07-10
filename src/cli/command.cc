// Copyright (c) 2017 The Ustore Authors.

#include <boost/algorithm/string.hpp>
#include <fstream>
#include <limits>
#include "cli/console.h"
#include "cli/command.h"

namespace ustore {
namespace cli {

const size_t Command::kDefaultLimitPrintElems(10);
size_t Command::limit_print_elems(kDefaultLimitPrintElems);

Command::Command(DB* db) noexcept : odb_(db), cs_(db) {
  // basic commands
  CMD_HANDLER("GET", ExecGet());
  CMD_HANDLER("GET_ALL", ExecGetAll());
  CMD_ALIAS("GET_ALL", "GET-ALL");
  CMD_HANDLER("PUT", ExecPut());
  CMD_HANDLER("APPEND", ExecAppend());
  CMD_HANDLER("MERGE", ExecMerge());
  CMD_HANDLER("BRANCH", ExecBranch());
  CMD_HANDLER("RENAME", ExecRename());
  CMD_HANDLER("DELETE", ExecDelete());
  CMD_ALIAS("DELETE", "DEL");
  CMD_HANDLER("LIST_KEY", ExecListKey());
  CMD_ALIAS("LIST_KEY", "LIST-KEY");
  CMD_ALIAS("LIST_KEY", "LISTKEY");
  CMD_ALIAS("LIST_KEY", "LSKEY");
  CMD_ALIAS("LIST_KEY", "LS_KEY");
  CMD_ALIAS("LIST_KEY", "LS-KEY");
  CMD_ALIAS("LIST_KEY", "LSK");
  CMD_HANDLER("LIST_KEY_ALL", ExecListKeyAll());
  CMD_ALIAS("LIST_KEY_ALL", "LIST-KEY-ALL");
  CMD_ALIAS("LIST_KEY_ALL", "LSKEY_ALL");
  CMD_ALIAS("LIST_KEY_ALL", "LSKEY-ALL");
  CMD_ALIAS("LIST_KEY_ALL", "LS_KEY_ALL");
  CMD_ALIAS("LIST_KEY_ALL", "LS-KEY-ALL");
  CMD_ALIAS("LIST_KEY_ALL", "LSK_ALL");
  CMD_ALIAS("LIST_KEY_ALL", "LSK-ALL");
  CMD_HANDLER("LIST_BRANCH", ExecListBranch());
  CMD_ALIAS("LIST_BRANCH", "LIST-BRANCH");
  CMD_ALIAS("LIST_BRANCH", "LISTBRANCH");
  CMD_ALIAS("LIST_BRANCH", "LSBRANCH");
  CMD_ALIAS("LIST_BRANCH", "LS_BRANCH");
  CMD_ALIAS("LIST_BRANCH", "LS-BRANCH");
  CMD_ALIAS("LIST_BRANCH", "LSB");
  CMD_HANDLER("HEAD", ExecHead());
  CMD_HANDLER("LATEST", ExecLatest());
  CMD_HANDLER("LATEST_ALL", ExecLatestAll());
  CMD_ALIAS("LATEST_ALL", "LATEST-ALL");
  CMD_HANDLER("EXISTS", ExecExists());
  CMD_ALIAS("EXISTS", "EXIST");
  CMD_HANDLER("IS_HEAD", ExecIsHead());
  CMD_ALIAS("IS_HEAD", "ISHEAD");
  CMD_ALIAS("IS_HEAD", "ISH");
  CMD_HANDLER("IS_LATEST", ExecIsLatest());
  CMD_ALIAS("IS_LATEST", "ISLATEST");
  CMD_ALIAS("IS_LATEST", "ISL");
  CMD_HANDLER("META", ExecMeta());
  CMD_ALIAS("META", "GET_META");
  CMD_ALIAS("META", "GET-META");
  // relational commands
  CMD_HANDLER("CREATE_TABLE", ExecCreateTable());
  CMD_ALIAS("CREATE_TABLE", "CREATE-TABLE");
  CMD_ALIAS("CREATE_TABLE", "CREATETABLE");
  CMD_ALIAS("CREATE_TABLE", "CREATE");
  CMD_ALIAS("CREATE_TABLE", "CREATE_TAB");
  CMD_ALIAS("CREATE_TABLE", "CREATE-TAB");
  CMD_ALIAS("CREATE_TABLE", "CREATETAB");
  CMD_HANDLER("GET_TABLE", ExecGetTable());
  CMD_ALIAS("GET_TABLE", "GET-TABLE");
  CMD_ALIAS("GET_TABLE", "GETTABLE");
  CMD_ALIAS("GET_TABLE", "GET_TAB");
  CMD_ALIAS("GET_TABLE", "GET-TAB");
  CMD_ALIAS("GET_TABLE", "GETTAB");
  CMD_HANDLER("BRANCH_TABLE", ExecBranchTable());
  CMD_ALIAS("BRANCH_TABLE", "BRANCH-TABLE");
  CMD_ALIAS("BRANCH_TABLE", "BRANCHTABLE");
  CMD_ALIAS("BRANCH_TABLE", "BRANCH_TAB");
  CMD_ALIAS("BRANCH_TABLE", "BRANCH-TAB");
  CMD_ALIAS("BRANCH_TABLE", "BRANCHTAB");
  CMD_HANDLER("LIST_TABLE_BRANCH", ExecListTableBranch());
  CMD_ALIAS("LIST_TABLE_BRANCH", "LIST-TABLE-BRANCH");
  CMD_ALIAS("LIST_TABLE_BRANCH", "LIST_TAB_BRANCH");
  CMD_ALIAS("LIST_TABLE_BRANCH", "LIST-TAB-BRANCH");
  CMD_ALIAS("LIST_TABLE_BRANCH", "LS_TAB_BRANCH");
  CMD_ALIAS("LIST_TABLE_BRANCH", "LS-TAB-BRANCH");
  CMD_HANDLER("DELETE_TABLE", ExecDeleteTable());
  CMD_ALIAS("DELETE_TABLE", "DELETE-TABLE");
  CMD_ALIAS("DELETE_TABLE", "DELETETABLE");
  CMD_ALIAS("DELETE_TABLE", "DELETE_TAB");
  CMD_ALIAS("DELETE_TABLE", "DELETE-TAB");
  CMD_ALIAS("DELETE_TABLE", "DELETETAB");
  CMD_ALIAS("DELETE_TABLE", "DEL_TABLE");
  CMD_ALIAS("DELETE_TABLE", "DEL-TABLE");
  CMD_ALIAS("DELETE_TABLE", "DELTABLE");
  CMD_ALIAS("DELETE_TABLE", "DEL_TAB");
  CMD_ALIAS("DELETE_TABLE", "DEL-TAB");
  CMD_ALIAS("DELETE_TABLE", "DELTAB");
  CMD_ALIAS("DELETE_TABLE", "DROP_TABLE");
  CMD_ALIAS("DELETE_TABLE", "DROP-TABLE");
  CMD_ALIAS("DELETE_TABLE", "DROPTABLE");
  CMD_ALIAS("DELETE_TABLE", "DROP_TAB");
  CMD_ALIAS("DELETE_TABLE", "DROP-TAB");
  CMD_ALIAS("DELETE_TABLE", "DROPTAB");
  CMD_ALIAS("DELETE_TABLE", "DROP");
  CMD_HANDLER("GET_COLUMN", ExecGetColumn());
  CMD_ALIAS("GET_COLUMN", "GET-COLUMN");
  CMD_ALIAS("GET_COLUMN", "GET_COL");
  CMD_ALIAS("GET_COLUMN", "GET-COL");
  CMD_ALIAS("GET_COLUMN", "GETCOLUMN");
  CMD_ALIAS("GET_COLUMN", "GETCOL");
  CMD_HANDLER("GET_COLUMN_ALL", ExecGetColumnAll());
  CMD_ALIAS("GET_COLUMN_ALL", "GET-COLUMN-ALL");
  CMD_ALIAS("GET_COLUMN_ALL", "GET_COL_ALL");
  CMD_ALIAS("GET_COLUMN_ALL", "GET-COL-ALL");
  CMD_ALIAS("GET_COLUMN_ALL", "GETCOLUMN_ALL");
  CMD_ALIAS("GET_COLUMN_ALL", "GETCOLUMN-ALL");
  CMD_ALIAS("GET_COLUMN_ALL", "GETCOL_ALL");
  CMD_ALIAS("GET_COLUMN_ALL", "GETCOL-ALL");
  CMD_HANDLER("LIST_COLUMN_BRANCH", ExecListColumnBranch());
  CMD_ALIAS("LIST_COLUMN_BRANCH", "LIST-COLUMN-BRANCH");
  CMD_ALIAS("LIST_COLUMN_BRANCH", "LIST_COL_BRANCH");
  CMD_ALIAS("LIST_COLUMN_BRANCH", "LIST-COL-BRANCH");
  CMD_ALIAS("LIST_COLUMN_BRANCH", "LS_COL_BRANCH");
  CMD_ALIAS("LIST_COLUMN_BRANCH", "LS-COL-BRANCH");
  CMD_HANDLER("DELETE_COLUMN", ExecDeleteColumn());
  CMD_ALIAS("DELETE_COLUMN", "DELETE-COLUMN");
  CMD_ALIAS("DELETE_COLUMN", "DELETECOLUMN");
  CMD_ALIAS("DELETE_COLUMN", "DELETE_COL");
  CMD_ALIAS("DELETE_COLUMN", "DELETE-COL");
  CMD_ALIAS("DELETE_COLUMN", "DELETECOL");
  CMD_ALIAS("DELETE_COLUMN", "DEL_COLUMN");
  CMD_ALIAS("DELETE_COLUMN", "DEL-COLUMN");
  CMD_ALIAS("DELETE_COLUMN", "DELCOLUMN");
  CMD_ALIAS("DELETE_COLUMN", "DEL_COL");
  CMD_ALIAS("DELETE_COLUMN", "DEL-COL");
  CMD_ALIAS("DELETE_COLUMN", "DELCOL");
  CMD_HANDLER("DIFF", ExecDiff());
  CMD_HANDLER("DIFF_TABLE", ExecDiffTable());
  CMD_ALIAS("DIFF_TABLE", "DIFF-TABLE");
  CMD_ALIAS("DIFF_TABLE", "DIFFTABLE");
  CMD_ALIAS("DIFF_TABLE", "DIFF_TAB");
  CMD_ALIAS("DIFF_TABLE", "DIFF-TAB");
  CMD_ALIAS("DIFF_TABLE", "DIFFTAB");
  CMD_HANDLER("DIFF_COLUMN", ExecDiffColumn());
  CMD_ALIAS("DIFF_COLUMN", "DIFF-COLUMN");
  CMD_ALIAS("DIFF_COLUMN", "DIFFCOLUMN");
  CMD_ALIAS("DIFF_COLUMN", "DIFF_COL");
  CMD_ALIAS("DIFF_COLUMN", "DIFF-COL");
  CMD_ALIAS("DIFF_COLUMN", "DIFFCOL");
  CMD_HANDLER("EXISTS_TABLE", ExecExistsTable());
  CMD_ALIAS("EXISTS_TABLE", "EXISTS-TABLE");
  CMD_ALIAS("EXISTS_TABLE", "EXISTSTABLE");
  CMD_ALIAS("EXISTS_TABLE", "EXISTS_TAB");
  CMD_ALIAS("EXISTS_TABLE", "EXISTS-TAB");
  CMD_ALIAS("EXISTS_TABLE", "EXISTSTAB");
  CMD_ALIAS("EXISTS_TABLE", "EXIST_TABLE");
  CMD_ALIAS("EXISTS_TABLE", "EXIST-TABLE");
  CMD_ALIAS("EXISTS_TABLE", "EXISTTABLE");
  CMD_ALIAS("EXISTS_TABLE", "EXIST_TAB");
  CMD_ALIAS("EXISTS_TABLE", "EXIST-TAB");
  CMD_ALIAS("EXISTS_TABLE", "EXISTTAB");
  CMD_HANDLER("EXISTS_COLUMN", ExecExistsTable());
  CMD_ALIAS("EXISTS_COLUMN", "EXISTS-COLUMN");
  CMD_ALIAS("EXISTS_COLUMN", "EXISTSCOLUMN");
  CMD_ALIAS("EXISTS_COLUMN", "EXISTS_COL");
  CMD_ALIAS("EXISTS_COLUMN", "EXISTS-COL");
  CMD_ALIAS("EXISTS_COLUMN", "EXISTSCOL");
  CMD_ALIAS("EXISTS_COLUMN", "EXIST_COLUMN");
  CMD_ALIAS("EXISTS_COLUMN", "EXIST-COLUMN");
  CMD_ALIAS("EXISTS_COLUMN", "EXISTCOLUMN");
  CMD_ALIAS("EXISTS_COLUMN", "EXIST_COL");
  CMD_ALIAS("EXISTS_COLUMN", "EXIST-COL");
  CMD_ALIAS("EXISTS_COLUMN", "EXISTCOL");
  CMD_HANDLER("GET_ROW", ExecGetRow());
  CMD_ALIAS("GET_ROW", "GET-ROW");
  CMD_ALIAS("GET_ROW", "GETROW");
  CMD_HANDLER("INSERT", ExecInsert());
  CMD_HANDLER("INSERT_ROW", ExecInsertRow());
  CMD_ALIAS("INSERT_ROW", "INSERT-ROW");
  CMD_HANDLER("UPDATE", ExecUpdate());
  CMD_HANDLER("UPDATE_ROW", ExecUpdateRow());
  CMD_ALIAS("UPDATE_ROW", "UPDATE-ROW");
  CMD_HANDLER("DELETE_ROW", ExecDeleteRow());
  CMD_ALIAS("DELETE_ROW", "DELETE-ROW");
  CMD_ALIAS("DELETE_ROW", "DELETEROW");
  CMD_ALIAS("DELETE_ROW", "DEL_ROW");
  CMD_ALIAS("DELETE_ROW", "DEL-ROW");
  CMD_ALIAS("DELETE_ROW", "DELROW");
  CMD_HANDLER("LOAD_CSV", ExecLoadCSV());
  CMD_ALIAS("LOAD_CSV", "LOAD-CSV");
  CMD_ALIAS("LOAD_CSV", "LOADCSV");
  CMD_ALIAS("LOAD_CSV", "LOAD");
  CMD_HANDLER("DUMP_CSV", ExecDumpCSV());
  CMD_ALIAS("DUMP_CSV", "DUMP-CSV");
  CMD_ALIAS("DUMP_CSV", "DUMPCSV");
  CMD_ALIAS("DUMP_CSV", "DUMP");
  // utility commands
  CMD_HANDLER("HELP", ExecHelp());
  CMD_HANDLER("INFO", ExecInfo());
  CMD_ALIAS("INFO", "STATE");
  CMD_ALIAS("INFO", "STATUS");
  CMD_ALIAS("INFO", "STAT");
}

const int kPrintBasicCmdWidth = 15;
const int kPrintRelationalCmdWidth = 19;
const int kPrintUtilCmdWidth = 12;

#define FORMAT_BASIC_CMD(cmd)       FORMAT_CMD(cmd, kPrintBasicCmdWidth)
#define FORMAT_RELATIONAL_CMD(cmd)  FORMAT_CMD(cmd, kPrintRelationalCmdWidth)
#define FORMAT_UTIL_CMD(cmd)        FORMAT_CMD(cmd, kPrintUtilCmdWidth)

void Command::PrintCommandHelp(std::ostream& os) {
  os << BLUE("UStore Basic Commands") << ":"
     << std::endl
     << FORMAT_BASIC_CMD("GET{_ALL}")
     << "-k <key> [-b <branch> | -v <version>]" << std::endl
     << FORMAT_BASIC_CMD("PUT")
     << "-k <key> -x <value> {-p <type>} {-b <branch> |"
     << std::endl << std::setw(kPrintBasicCmdWidth + 36) << ""
     << "-u <refer_version>}" << std::endl
     << FORMAT_BASIC_CMD("APPEND")
     << "-k <key> -x <value> [-b <branch> | -u <refer_version>]" << std::endl
     << FORMAT_BASIC_CMD("BRANCH")
     << "-k <key> -b <new_branch> [-c <base_branch> | "
     << std::endl << std::setw(kPrintBasicCmdWidth + 29) << ""
     << "-u <refer_version>]" << std::endl
     << FORMAT_BASIC_CMD("MERGE")
     << "-k <key> -x <value> [-b <target_branch> -c <refer_branch> | "
     << std::endl << std::setw(kPrintBasicCmdWidth + 24) << ""
     << "-b <target_branch> -u <refer_version> | "
     << std::endl << std::setw(kPrintBasicCmdWidth + 24) << ""
     << "-u <refer_version> -v <refer_version_2>]" << std::endl
     << FORMAT_BASIC_CMD("RENAME")
     << "-k <key> "
     << "-c <from_branch> -b <to_branch>" << std::endl
     << FORMAT_BASIC_CMD("DELETE")
     << "-k <key> -b <branch>" << std::endl
     << FORMAT_BASIC_CMD("HEAD")
     << "-k <key> -b <branch>" << std::endl
     << FORMAT_BASIC_CMD("LATEST_{_ALL}")
     << "-k <key>" << std::endl
     << FORMAT_BASIC_CMD("EXISTS")
     << "-k <key> {-b <branch>}" << std::endl
     << FORMAT_BASIC_CMD("IS_HEAD")
     << "-k <key> -b <branch> -v <version>" << std::endl
     << FORMAT_BASIC_CMD("IS_LATEST")
     << "-k <key> -v <version>" << std::endl
     << FORMAT_BASIC_CMD("LIST_BRANCH")
     << "-k <key>" << std::endl
     << FORMAT_BASIC_CMD("LIST_KEY{_ALL}") << std::endl
     << FORMAT_BASIC_CMD("META")
     << "-k <key> [-b <branch> | -v <version>]" << std::endl
     << std::endl
     << BLUE("UStore Relational (Columnar) Commands") << ":"
     << std::endl
     << FORMAT_RELATIONAL_CMD("CREATE_TABLE")
     << "-t <table> -b <branch>" << std::endl
     << FORMAT_RELATIONAL_CMD("EXISTS_TABLE")
     << "-t <table> {-b <branch>}" << std::endl
     << FORMAT_RELATIONAL_CMD("GET_TABLE")
     << "-t <table> -b <branch>" << std::endl
     << FORMAT_RELATIONAL_CMD("BRANCH_TABLE")
     << "-t <table> -b <target_branch> -c <refer_branch>" << std::endl
     << FORMAT_RELATIONAL_CMD("LIST_TABLE_BRANCH")
     << "-t <table>" << std::endl
     << FORMAT_RELATIONAL_CMD("DELETE_TABLE")
     << "-t <table> -b <branch>" << std::endl
     << FORMAT_RELATIONAL_CMD("DIFF_TABLE")
     << "-t <table> -b <branch> -c <branch_2> {-s <table_2>}"
     << std::endl
     << FORMAT_RELATIONAL_CMD("EXISTS_COLUMN")
     << "-t <table> -m <column> {-b <branch>}" << std::endl
     << FORMAT_RELATIONAL_CMD("GET_COLUMN{_ALL}")
     << "-t <table> -b <branch> -m <column>" << std::endl
     << FORMAT_RELATIONAL_CMD("LIST_COLUMN_BRANCH")
     << "-t <table> -m <column>" << std::endl
     << FORMAT_RELATIONAL_CMD("DELETE_COLUMN")
     << "-t <table> -b <branch> -m <column>" << std::endl
     << FORMAT_RELATIONAL_CMD("DIFF_COLUMN")
     << "-t <table> -m <column> -b <branch> -c <branch_2> "
     << std::endl << std::setw(kPrintRelationalCmdWidth + 3) << ""
     << "{-s <table_2>} {-n <column_2>}" << std::endl
     << FORMAT_RELATIONAL_CMD("GET_ROW")
     << "-t <table> -b <branch> -n <refer_colum> -y <refer_value>" << std::endl
     << FORMAT_RELATIONAL_CMD("INSERT_ROW")
     << "-t <table> -b <branch> -x <field_list>" << std::endl
     << FORMAT_RELATIONAL_CMD("UPDATE_ROW")
     << "-t <table> -b <branch> -x <field_list> "
     << std::endl << std::setw(kPrintRelationalCmdWidth + 3) << ""
     << "[-n <refer_colum> -y <refer_value> | -i <row_index>]" << std::endl
     << FORMAT_RELATIONAL_CMD("DELETE_ROW")
     << "-t <table> -b <branch>"
     << std::endl << std::setw(kPrintRelationalCmdWidth + 3) << ""
     << "[-n <refer_colum> -y <refer_value> | -i <row_index>]" << std::endl
     << FORMAT_RELATIONAL_CMD("LOAD_CSV")
     << "<file> -t <table> -b <branch>" << std::endl
     << FORMAT_RELATIONAL_CMD("DUMP_CSV")
     << "<file> -t <table> -b <branch>" << std::endl
     << std::endl
     << BLUE("UStore Utility Commands") << ":"
     << std::endl
     << FORMAT_UTIL_CMD("HELP") << std::endl
     << FORMAT_UTIL_CMD("INFO") << std::endl;
}

void Command::PrintHelp() {
  DCHECK(Config::is_help);
  std::cout << BOLD_GREEN("Usage") << ": "
            << "ustore_cli <command> {{<option>} <argument|file> ...}"
            << std::endl << std::setw(3) << "" << "or  "
            << "ustore_cli --script <file>"
            << std::endl << std::setw(3) << "" << "or  "
            << "ustore_cli --help" << std::endl
            << std::endl;
  Command::PrintCommandHelp();
  std::cout << Config::command_options_help << std::endl;
}

ErrorCode Command::ExecHelp() {
  char arg0[] = "ustore_cli";
  char arg1[] = "--help";
  char* argv[] = {arg0, arg1};
  Run(2, argv);
  Config::command = "HELP";
  return ErrorCode::kOK;
}

ErrorCode Command::ExecCommand(const std::string& cmd) {
  auto it_cmd_exec = cmd_exec_.find(cmd);
  if (it_cmd_exec == cmd_exec_.end()) {
    auto it_alias_exec = alias_exec_.find(cmd);
    if (it_alias_exec == alias_exec_.end()) {
      std::cerr << BOLD_RED("[ERROR] ")
                << "Unknown command: " << cmd << std::endl;
      return ErrorCode::kUnknownCommand;
    }
    return (*(it_alias_exec->second))();
  }
  return it_cmd_exec->second();
}

ErrorCode Command::Run(int argc, char* argv[]) {
  if (!Config::ParseCmdArgs(argc, argv)) {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Found invalid command-line option" << std::endl;
    return ErrorCode::kInvalidCommandArgument;
  }
  if (Config::is_help) {
    PrintHelp();
    return ErrorCode::kOK;
  }
  if (!Config::command.empty() && Config::script.empty()) {
    ErrorCode ec(ErrorCode::kUnknownOp);
    Time([this, &ec] { ec = ExecCommand(Config::command); });
    std::cout << TimeDisplay("", "\n");
    return ec;
  }
  if (Config::command.empty() && !Config::script.empty()) {
    return ExecScript(Config::script);
  }
  std::cerr << BOLD_RED("[ERROR] ")
            << "Either UStore command or \"--script\" must be given, "
            << "but not both" << std::endl;
  return ErrorCode::kInvalidCommandArgument;
}

ErrorCode Command::ExecScript(const std::string& script) {
  CHECK(!script.empty()) << BOLD_RED("[ERROR] ")
                         << "Path of script must be given";
  std::ifstream ifs(script);
  if (!ifs) {
    std::cerr << BOLD_RED("[FAILURE] ") << "Invalid path of script: "
              << script << std::endl;
    return ErrorCode::kFailedOpenFile;
  }
  auto ec = ErrorCode::kOK;
  std::string line;
  size_t cnt_line = 0;
  std::vector<std::string> args;
  while ((ec == ErrorCode::kOK) && std::getline(ifs, line)) {
    ++cnt_line;
    boost::trim(line);
    if (line.empty() || boost::starts_with(line, "#")) continue;
    std::cout << YELLOW(cnt_line << ":> ") << line << std::endl;
    if (!Utils::TokenizeArgs(line, &args)) {
      std::cerr << BOLD_RED("[ERROR] ") << "Illegal command line"
                << std::endl << std::endl;
      ec = ErrorCode::kInvalidCommandArgument;
    } else if (!Config::ParseCmdArgs(args)) {  // parse command-line arguments
      std::cerr << BOLD_RED("[ERROR] ")
                << "Found invalid command-line option" << std::endl;
      ec = ErrorCode::kInvalidCommandArgument;
    } else if (Config::is_help) {  // ignore printing help message
      std::cout << "...(ignored for script running)" << std::endl;
    } else if (!Config::script.empty()) {  // prohibit running with script
      std::cerr << BOLD_RED("[ERROR] ") << "\"--script\" cannot be used in "
                << "UStore script" << std::endl;
      ec = ErrorCode::kInvalidCommandArgument;
    } else if (Config::command.empty()) {  // validate the existence of command
      std::cerr << BOLD_RED("[ERROR] ") << "UStore command is missing"
                << std::endl;
      ec = ErrorCode::kInvalidCommandArgument;
    } else {  // everything is ready, go!
      Time([this, &ec] { ec = ExecCommand(Config::command); });
    }
    std::cout << TimeDisplay("", "\n") << std::endl;
  }
  ifs.close();
  return ec;
}

ErrorCode Command::ExecManipMeta(
  const std::function<ErrorCode(const VMeta&)>& f_manip_meta) {
  const auto& key = Config::key;
  const auto& branch = Config::branch;
  const auto& ver = Config::version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: GET] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\", "
              << "Version: \"" << ver << "\"" << std::endl;
  };
  const auto f_rpt_fail_by_branch = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: GET] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  const auto f_rpt_fail_by_ver = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: GET] ")
              << "Key: \"" << key << "\", "
              << "Version: \"" << ver << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (key.empty() || !(branch.empty() ^ ver.empty())) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto rst = !branch.empty() && ver.empty()
             ? odb_.Get(Slice(key), Slice(branch))
             : odb_.Get(Slice(key), Hash::FromBase32(ver));
  auto& ec = rst.stat;
  if (ec == ErrorCode::kOK) {
    ec = f_manip_meta(rst.value);
  } else {
    branch.empty() ? f_rpt_fail_by_ver(ec) : f_rpt_fail_by_branch(ec);
  }
  return ec;
}

ErrorCode Command::ExecGet() {
  // redirection
  if (!Config::table.empty() && Config::key.empty()) {
    if (!Config::column.empty()) return ExecGetColumn();
    if (!Config::ref_column.empty()) return ExecGetRow();
    return ExecGetTable();
  }

  const auto f_manip_meta = [](const VMeta & meta) {
    auto type = meta.type();
    if (!Config::is_vert_list) {
      std::cout << BOLD_GREEN("[SUCCESS: GET] ")
                << "Value" << "<" << type << ">: ";
    }
    switch (type) {
      case UType::kString:
      case UType::kBlob:
        std::cout << "\"" << meta << "\"";
        break;
      case UType::kList: {
        auto list = meta.List();
        if (Config::is_vert_list) {
          for (auto it = list.Scan(); !it.end(); it.next())
            std::cout << it.value() << std::endl;
        } else {
          Utils::Print(list, "[", "]", ", ", true, limit_print_elems);
        }
        break;
      }
      case UType::kMap: {
        auto map = meta.Map();
        if (Config::is_vert_list) {
          for (auto it = map.Scan(); !it.end(); it.next())
            std::cout << it.key() << " -> " << it.value() << std::endl;
        } else {
          Utils::Print(
            meta.Map(), "[", "]", ", ", "(", ")", ",", true, limit_print_elems);
        }
        break;
      }
      default:
        std::cout << meta;
        break;
    }
    if (!Config::is_vert_list) std::cout << std::endl;
    return ErrorCode::kOK;
  };
  return ExecManipMeta(f_manip_meta);
}

ErrorCode Command::ExecPut(const std::string& cmd, const VObject& obj) {
  const auto& key = Config::key;
  const auto& branch = Config::branch;
  const auto& ref_ver = Config::ref_version;
  DCHECK(branch.empty() ^ ref_ver.empty());
  // screen printing
  const auto f_rpt_success = [&](const Hash & ver) {
    std::cout << BOLD_GREEN("[SUCCESS: " << cmd << "] ")
              << "Type: \"" << obj.value().type << "\", "
              << "Version: \"" << ver << '\"' << std::endl;
  };
  const auto f_rpt_fail_by_branch = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: " << cmd << "] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  const auto f_rpt_fail_by_ver = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: " << cmd << "] ")
              << "Key: \"" << key << "\", "
              << "Ref. Version: \"" << ref_ver << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (!branch.empty() && ref_ver.empty()) {
    DCHECK(ref_ver.empty());
    auto rst = odb_.Put(Slice(key), obj, Slice(branch));
    auto& ec = rst.stat;
    auto& ver = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail_by_branch(ec);
    return ec;
  } else if (branch.empty() && !ref_ver.empty()) {
    DCHECK(!ref_ver.empty());
    auto rst = odb_.Put(Slice(key), obj, Hash::FromBase32(ref_ver));
    auto& ec = rst.stat;
    auto& ver = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail_by_ver(ec);
    return ec;
  } else { // branch.empty() && ref_ver.empty()
    auto rst = odb_.Put(Slice(key), obj, Hash::kNull);
    auto& ec = rst.stat;
    auto& ver = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail_by_ver(ec);
    return ec;
  }
}

ErrorCode Command::ExecPut() {
  const auto& type = Config::type;
  const auto& key = Config::key;
  const auto& val = Config::value;
  const auto& branch = Config::branch;
  const auto& ref_ver = Config::ref_version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: PUT] ")
              << "Type: \"" << type << "\", "
              << "Key: \"" << key << "\", "
              << "Value: \"" << val << "\", "
              << "Branch: \"" << branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\"" << std::endl;
  };
  if (key.empty() || val.empty() || !(branch.empty() || ref_ver.empty())) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto f_put = [this](const VObject & obj) { return ExecPut("PUT", obj); };
  ErrorCode ec(ErrorCode::kUnknownOp);
  switch (type) {
    case UType::kString:
      ec = f_put(VString(Slice(val)));
      break;
    case UType::kBlob:
      // TODO(linqian)
      break;
    case UType::kList: {
      auto elems = Utils::Tokenize(val, "{}[]|,; ");
      std::vector<Slice> slice_elems;
      slice_elems.reserve(elems.size());
      for (auto& e : elems) slice_elems.emplace_back(Slice(e));
      ec = f_put(VList(slice_elems));
      break;
    }
    case UType::kMap: {
      auto elems = Utils::Tokenize(val, "{}[]()|,;: ");
      std::vector<Slice> keys, vals;
      ec = ErrorCode::kOK;
      for (auto it = elems.begin(); it != elems.end();) {
        keys.emplace_back(*it++);
        if (it == elems.end()) {
          f_rpt_invalid_args();
          ec = ErrorCode::kInvalidCommandArgument;
          break;
        }
        vals.emplace_back(*it++);
      }
      if (ec == ErrorCode::kOK) ec = f_put(VMap(keys, vals));
      break;
    }
    default:
      std::cout << BOLD_RED("[FAILED: PUT] ")
                << "The operation is not supported for data type \""
                << type << "\"" << std::endl;
      ec = ErrorCode::kTypeUnsupported;
      break;
  }
  return ec;
}

ErrorCode Command::ExecAppend() {
  Config::version = Config::ref_version;
  return ExecManipMeta([this](const VMeta & meta) { return ExecAppend(meta); });
}

ErrorCode Command::ExecAppend(const VMeta& meta) {
  const auto& val = Config::value;
  auto type = meta.type();
  auto f_put = [this](const VObject & obj) { return ExecPut("APPEND", obj); };
  ErrorCode ec(ErrorCode::kUnknownOp);
  switch (type) {
    case UType::kBlob:
      // TODO(linqian)
      break;
    case UType::kList: {
      auto elems = Utils::Tokenize(val, "{}[]|,; ");
      if (elems.empty()) {
        std::cerr << BOLD_RED("[INVALID ARGS: APPEND] ")
                  << "No element is provided: \"" << val << "\""
                  << std::endl;
        ec = ErrorCode::kInvalidCommandArgument;
      } else {
        std::vector<Slice> slice_elems;
        slice_elems.reserve(elems.size());
        for (auto& e : elems) slice_elems.emplace_back(Slice(e));
        auto list = meta.List();
        list.Append(slice_elems);
        ec = f_put(list);
      }
      break;
    }
    default:
      std::cout << BOLD_RED("[FAILED: APPEND] ")
                << "The operation is not supported for data type \""
                << type << "\"" << std::endl;
      ec = ErrorCode::kTypeUnsupported;
  }
  return ec;
}

ErrorCode Command::ExecDeleteListElements(const VMeta& meta) {
  // TODO(linqian)
  return ErrorCode::kUnknownOp;
}

ErrorCode Command::ExecInsertListElements(const VMeta& meta) {
  // TODO(linqian)
  return ErrorCode::kUnknownOp;
}

ErrorCode Command::ExecReplaceListElement(const VMeta& meta) {
  // TODO(linqian)
  return ErrorCode::kUnknownOp;
}

ErrorCode Command::ExecMerge() {
  const auto& key = Config::key;
  const auto& val = Config::value;
  const auto& tgt_branch = Config::branch;
  const auto& ref_branch = Config::ref_branch;
  const auto& ref_ver = Config::ref_version;
  const auto& ref_ver2 = Config::version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: MERGE] ")
              << "Key: \"" << key << "\", "
              << "Value: \"" << val << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Ref. Branch: \"" << ref_branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\", "
              << "Ref. Version (2nd): \"" << ref_ver2 << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const Hash & ver) {
    std::cout << BOLD_GREEN("[SUCCESS: MERGE] ")
              << "Version: \"" << ver << '\"' << std::endl;
  };
  const auto f_rpt_fail_by_branch = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: MERGE] ")
              << "Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Ref. Branch: \"" << ref_branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  const auto f_rpt_fail_by_branch_ver = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: MERGE] ")
              << "Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  const auto f_rpt_fail_by_ver = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: MERGE] ")
              << "Key: \"" << key << "\", "
              << "Ref. Version: \"" << ref_ver << "\", "
              << "Ref. Version (2nd): \"" << ref_ver2 << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (key.empty() || val.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (!tgt_branch.empty() && !ref_branch.empty() && ref_ver.empty()) {
    auto rst = odb_.Merge(Slice(key), VString(Slice(val)), Slice(tgt_branch),
                          Slice(ref_branch));
    auto& ec = rst.stat;
    auto& ver = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail_by_branch(ec);
    return ec;
  }
  if (!tgt_branch.empty() && ref_branch.empty() && !ref_ver.empty()) {
    auto rst = odb_.Merge(Slice(key), VString(Slice(val)), Slice(tgt_branch),
                          Hash::FromBase32(ref_ver));
    auto& ec = rst.stat;
    auto& ver = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail_by_branch_ver(ec);
    return ec;
  }
  if (tgt_branch.empty() && !ref_ver.empty() && !ref_ver2.empty()) {
    auto rst = odb_.Merge(Slice(key), VString(Slice(val)),
                          Hash::FromBase32(ref_ver),
                          Hash::FromBase32(ref_ver2));
    auto& ec = rst.stat;
    auto& ver = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail_by_ver(ec);
    return ec;
  }
  // illegal settings of arguments
  f_rpt_invalid_args();
  return ErrorCode::kInvalidCommandArgument;
}

ErrorCode Command::ExecBranch() {
  // redirection
  if (!Config::table.empty() && Config::key.empty()) {
    return ExecBranchTable();
  }

  const auto& key = Config::key;
  const auto& tgt_branch = Config::branch;
  const auto& ref_branch = Config::ref_branch;
  const auto& ref_ver = Config::ref_version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: BRANCH] ")
              << "Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Referring Branch: \"" << ref_branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [&key, &tgt_branch]() {
    std::cout << BOLD_GREEN("[SUCCESS: BRANCH] ")
              << "Branch \"" << tgt_branch
              << "\" has been created for Key \"" << key << "\""
              << std::endl;
  };
  const auto f_rpt_fail_by_branch = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: BRANCH] ")
              << "Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Referring Branch: \"" << ref_branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  const auto f_rpt_fail_by_ver = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: BRANCH] ")
              << "Key: \"" << key << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Ref. Version: \"" << ref_ver << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (key.empty() || tgt_branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (!ref_branch.empty() && ref_ver.empty()) {
    auto ec = odb_.Branch(Slice(key), Slice(ref_branch), Slice(tgt_branch));
    ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail_by_branch(ec);
    return ec;
  }
  if (ref_branch.empty() && !ref_ver.empty()) {
    auto ec = odb_.Branch(Slice(key), Hash::FromBase32(ref_ver),
                          Slice(tgt_branch));
    ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail_by_ver(ec);
    return ec;
  }
  // illegal: ref_branch and ref_ver are neither set or both set
  f_rpt_invalid_args();
  return ErrorCode::kInvalidCommandArgument;
}

ErrorCode Command::ExecRename() {
  const auto& key = Config::key;
  const auto& old_branch = Config::ref_branch;
  const auto& new_branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: RENAME] ")
              << "Key: \"" << key << "\", "
              << "Referring Branch: \"" << old_branch << "\", "
              << "Target Branch: \"" << new_branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << BOLD_GREEN("[SUCCESS: RENAME] ")
              << "Branch \"" << old_branch
              << "\" has been renamed to \"" << new_branch
              << "\" for Key \"" << key << "\"" << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: RENAME] ")
              << "Key: \"" << key << "\", "
              << "Ref. Branch: \"" << old_branch << "\", "
              << "Target Branch: \"" << new_branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (key.empty() || old_branch.empty() || new_branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = odb_.Rename(Slice(key), Slice(old_branch), Slice(new_branch));
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecDelete() {
  // redirection
  if (!Config::table.empty() && Config::key.empty()) {
    if (!Config::column.empty()) return ExecDeleteColumn();
    if (!Config::ref_column.empty()
        || Config::position >= 0) return ExecDeleteRow();
    return ExecDeleteTable();
  }

  const auto& key = Config::key;
  const auto& branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: DELETE] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&key, &branch]() {
    std::cout << BOLD_GREEN("[SUCCESS: DELETE] ")
              << "Branch \"" << branch
              << "\" has been deleted for Key \"" << key << "\"" << std::endl;
  };
  const auto f_rpt_fail = [&key, &branch](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: DELETE] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (key.empty() || branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = odb_.Delete(Slice(key), Slice(branch));
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecListKey() {
  // screen printing
  const auto f_rpt_success = [](const std::vector<std::string>& keys) {
    if (Config::is_vert_list) {
      for (auto& k : keys) std::cout << k << std::endl;
    } else {
      std::cout << BOLD_GREEN("[SUCCESS: LIST_KEY] ") << "Keys: ";
      Utils::Print(keys, "[", "]", ", ", true, limit_print_elems);
      std::cout << std::endl;
    }
  };
  const auto f_rpt_fail = [](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: LIST_KEY] ")
              << "--> ErrorCode: " << ec << std::endl;
  };
  // execution
  auto rst = odb_.ListKeys();
  auto& ec = rst.stat;
  auto& keys = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(keys) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecListBranch() {
  // redirection
  if (!Config::table.empty() && Config::key.empty()) {
    return Config::column.empty() ?
           ExecListTableBranch() : ExecListColumnBranch();
  }

  const auto& key = Config::key;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: LIST_BRANCH] ")
              << "Key: \"" << key << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const std::vector<std::string>& branches) {
    if (Config::is_vert_list) {
      for (auto& b : branches) std::cout << b << std::endl;
    } else {
      std::cout << BOLD_GREEN("[SUCCESS: LIST_BRANCH] ") << "Branches: ";
      Utils::Print(branches, "[", "]", ", ", true);
      std::cout << std::endl;
    }
  };
  const auto f_rpt_fail = [&key](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: LIST_BRANCH] ")
              << "Key: \"" << key << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (key.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto rst = odb_.ListBranches(Slice(key));
  auto& ec = rst.stat;
  auto& branches = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(branches) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecHead() {
  const auto& key = Config::key;
  const auto& branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: HEAD] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const Hash & ver) {
    std::cout << BOLD_GREEN("[SUCCESS: HEAD] ")
              << "Version: \"" << ver << '\"' << std::endl;
  };
  const auto f_rpt_fail = [&key, &branch](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: HEAD] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (key.empty() || branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto rst = odb_.GetBranchHead(Slice(key), Slice(branch));
  auto& ec = rst.stat;
  auto& ver = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(ver) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecLatest() {
  const auto& key = Config::key;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: LATEST] ")
              << "Key: \"" << key << "\""
              << std::endl;
  };
  const auto f_rpt_success = [](const std::vector<Hash>& vers) {
    if (Config::is_vert_list) {
      for (auto& v : vers) std::cout << v << std::endl;
    } else {
      std::cout << BOLD_GREEN("[SUCCESS: LATEST] ") << "Versions: ";
      Utils::Print(vers, "[", "]", ", ", true, limit_print_elems);
      std::cout << std::endl;
    }
  };
  const auto f_rpt_fail = [&key](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: LATEST] ")
              << "Key: \"" << key << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (key.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto rst = odb_.GetLatestVersions(Slice(key));
  auto& ec = rst.stat;
  auto& vers = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(vers) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecExists() {
  // redirection
  if (!Config::table.empty() && Config::key.empty()) {
    return Config::column.empty() ? ExecExistsTable() : ExecExistsColumn();
  }

  const auto& key = Config::key;
  const auto& branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: EXISTS] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success_by_key = [](const bool exist) {
    std::cout << BOLD_GREEN("[SUCCESS: EXISTS KEY] ")
              << (exist ? "True" : "False") << std::endl;
  };
  const auto f_rpt_success_by_branch = [](const bool exist) {
    std::cout << BOLD_GREEN("[SUCCESS: EXISTS BRANCH] ")
              << (exist ? "True" : "False") << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: EXISTS] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (key.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (branch.empty()) {
    auto rst = odb_.Exists(Slice(key));
    auto& ec = rst.stat;
    auto exist = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success_by_key(exist) : f_rpt_fail(ec);
    return ec;
  } else {
    auto rst = odb_.Exists(Slice(key), Slice(branch));
    auto& ec = rst.stat;
    auto exist = rst.value;
    ec == ErrorCode::kOK ? f_rpt_success_by_branch(exist) : f_rpt_fail(ec);
    return ec;
  }
}

ErrorCode Command::ExecIsHead() {
  const auto& key = Config::key;
  const auto& branch = Config::branch;
  const auto& ver = Config::version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: IS_HEAD] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\", "
              << "Version: \"" << ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const bool is_head) {
    std::cout << BOLD_GREEN("[SUCCESS: IS_HEAD] ")
              << (is_head ? "True" : "False") << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: IS_HEAD] ")
              << "Key: \"" << key << "\", "
              << "Branch: \"" << branch << "\", "
              << "Version: \"" << ver << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (key.empty() || branch.empty() || ver.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto rst =
    odb_.IsBranchHead(Slice(key), Slice(branch), Hash::FromBase32(ver));
  auto& ec = rst.stat;
  auto& is_head = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(is_head) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecIsLatest() {
  const auto& key = Config::key;
  const auto& ver = Config::version;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: IS_LATEST] ")
              << "Key: \"" << key << "\", "
              << "Version: \"" << ver << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const bool is_latest) {
    std::cout << BOLD_GREEN("[SUCCESS: IS_LATEST] ")
              << (is_latest ? "True" : "False") << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: IS_LATEST] ")
              << "Key: \"" << key << "\", "
              << "Version: \"" << ver << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (key.empty() || ver.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto rst = odb_.IsLatestVersion(Slice(key), Hash::FromBase32(ver));
  auto& ec = rst.stat;
  auto& is_latest = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(is_latest) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecCreateTable() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: CREATE_TABLE] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << BOLD_GREEN("[SUCCESS: CREATE_TABLE] ")
              << "Table \"" << tab << "\" has been created for Branch \""
              << branch << "\"" << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: CREATE_TABLE] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty() || branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = cs_.CreateTable(tab, branch);
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecGetTable() {
  const auto& tab_name = Config::table;
  const auto& branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: GET_TABLE] ")
              << "Table: \"" << tab_name << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const Table & tab) {
    if (Config::is_vert_list) {
      for (auto it = tab.Scan(); !it.end(); it.next())
        std::cout << it.key() << std::endl;
    } else {
      std::cout << BOLD_GREEN("[SUCCESS: GET_TABLE] ") << "Columns: ";
      Utils::PrintKeys(tab, "[", "]", ", ", true);
      std::cout << std::endl;
    }
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: GET_TABLE] ")
              << "Table: \"" << tab_name << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab_name.empty() || branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  Table tab;
  auto ec = cs_.GetTable(tab_name, branch, &tab);
  ec == ErrorCode::kOK ? f_rpt_success(tab) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecBranchTable() {
  const auto& tab = Config::table;
  const auto& tgt_branch = Config::branch;
  const auto& ref_branch = Config::ref_branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: BRANCH_TABLE] ")
              << "Table: \"" << tab << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Referring Branch: \"" << ref_branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << BOLD_GREEN("[SUCCESS: BRANCH_TABLE] ")
              << "Branch \"" << tgt_branch
              << "\" has been created for Table \"" << tab << "\""
              << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: BRANCH_TABLE] ")
              << "Table: \"" << tab << "\", "
              << "Target Branch: \"" << tgt_branch << "\", "
              << "Referring Branch: \"" << ref_branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty() || tgt_branch.empty() || ref_branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = cs_.BranchTable(tab, ref_branch, tgt_branch);
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecListTableBranch() {
  const auto& tab = Config::table;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: LIST_TABLE_BRANCH] ")
              << "Table: \"" << tab << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const std::vector<std::string>& branches) {
    if (Config::is_vert_list) {
      for (auto& b : branches) std::cout << b << std::endl;
    } else {
      std::cout << BOLD_GREEN("[SUCCESS: LIST_TABLE_BRANCH] ") << "Branches: ";
      Utils::Print(branches, "[", "]", ", ", true);
      std::cout << std::endl;
    }
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: LIST_TABLE_BRANCH] ")
              << "Table: \"" << tab << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  std::vector<std::string> branches;
  auto ec = cs_.ListTableBranch(tab, &branches);
  ec == ErrorCode::kOK ? f_rpt_success(branches) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecDeleteTable() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: DELETE_TABLE] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << BOLD_GREEN("[SUCCESS: DELETE_TABLE] ")
              << "Table \"" << tab << "\" of Branch \"" << branch
              << "\" has been deleted" << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: DELETE_TABLE] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty() || branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = cs_.DeleteTable(tab, branch);
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecGetColumn() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  const auto& col_name = Config::column;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: GET_COLUMN] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Column: \"" << col_name << "\"" << std::endl;
  };
  const auto f_rpt_success = [&](const Column & col) {
    if (Config::is_vert_list) {
      for (auto it = col.Scan(); !it.end(); it.next())
        std::cout << it.value() << std::endl;
    } else {
      std::cout << BOLD_GREEN("[SUCCESS: GET_COLUMN] ") << "Column \""
                << col_name << "\": ";
      Utils::Print(col, "[", "]", ", ", true, limit_print_elems);
      std::cout << std::endl;
    }
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: GET_COLUMN] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Column: \"" << col_name << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty() || branch.empty() || col_name.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  Column col;
  auto ec = cs_.GetColumn(tab, branch, col_name, &col);
  ec == ErrorCode::kOK ? f_rpt_success(col) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecListColumnBranch() {
  const auto& tab = Config::table;
  const auto& col = Config::column;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: LIST_COLUMN_BRANCH] ")
              << "Table: \"" << tab << "\", "
              << "Column: \"" << col << "\"" << std::endl;
  };
  const auto f_rpt_success = [](const std::vector<std::string>& branches) {
    if (Config::is_vert_list) {
      for (auto& b : branches) std::cout << b << std::endl;
    } else {
      std::cout << BOLD_GREEN("[SUCCESS: LIST_COLUMN_BRANCH] ") << "Branches: ";
      Utils::Print(branches, "[", "]", ", ", true);
      std::cout << std::endl;
    }
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: LIST_COLUMN_BRANCH] ")
              << "Table: \"" << tab << "\", "
              << "Column: \"" << col << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty() || col.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  std::vector<std::string> branches;
  auto ec = cs_.ListColumnBranch(tab, col, &branches);
  ec == ErrorCode::kOK ? f_rpt_success(branches) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecDeleteColumn() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  const auto& col_name = Config::column;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: DELETE_COLUMN] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Column: \"" << col_name << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << BOLD_GREEN("[SUCCESS: DELETE_COLUMN] ")
              << "Column \"" << col_name
              << "\" has been deleted in Table \"" << tab
              << "\" of Branch \"" << branch << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: DELETE_COLUMN] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Column: \"" << col_name << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty() || branch.empty() || col_name.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = cs_.DeleteColumn(tab, branch, col_name);
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecDiff() {
  // redirection
  return Config::column.empty() ? ExecDiffTable() : ExecDiffColumn();
}

ErrorCode Command::ExecDiffTable() {
  const auto& lhs_tab_name = Config::table;
  const auto& lhs_branch = Config::branch;
  auto& rhs_tab_name = Config::ref_table;
  const auto& rhs_branch = Config::ref_branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: DIFF_TABLE] ")
              << "Table: \"" << lhs_tab_name << "\", "
              << "Branch: \"" << lhs_branch << "\", "
              << "Table (2nd): \"" << rhs_tab_name << "\", "
              << "Branch (2nd): \"" << rhs_branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [](TableDiffIterator & it_diff) {
    if (Config::is_vert_list) {
      for (; !it_diff.end(); it_diff.next())
        std::cout << it_diff.key() << std::endl;
    } else {
      std::cout << BOLD_GREEN("[SUCCESS: DIFF_TABLE] ")
                << "Different Columns: ";
      Utils::PrintDiff(it_diff, false, true);
      std::cout << std::endl;
    }
  };
  const auto f_rpt_fail_get_lhs = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: GET_TABLE] ")
              << "Table: \"" << lhs_tab_name << "\", "
              << "Branch: \"" << lhs_branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  const auto f_rpt_fail_get_rhs = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: GET_TABLE] ")
              << "Table (2nd): \"" << rhs_tab_name << "\", "
              << "Branch (2nd): \"" << rhs_branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (lhs_tab_name.empty() || lhs_branch.empty() || rhs_branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (rhs_tab_name.empty()) rhs_tab_name = lhs_tab_name;
  Table lhs_tab;
  auto ec = cs_.GetTable(lhs_tab_name, lhs_branch, &lhs_tab);
  if (ec != ErrorCode::kOK) {
    f_rpt_fail_get_lhs(ec);
    return ec;
  }
  Table rhs_tab;
  ec = cs_.GetTable(rhs_tab_name, rhs_branch, &rhs_tab);
  if (ec != ErrorCode::kOK) {
    f_rpt_fail_get_rhs(ec);
    return ec;
  }
  auto it_diff = cs_.DiffTable(lhs_tab, rhs_tab);
  f_rpt_success(it_diff);
  return ErrorCode::kOK;
}

ErrorCode Command::ExecDiffColumn() {
  const auto& lhs_tab = Config::table;
  const auto& lhs_branch = Config::branch;
  const auto& lhs_col_name = Config::column;
  auto& rhs_tab = Config::ref_table;
  const auto& rhs_branch = Config::ref_branch;
  auto& rhs_col_name = Config::ref_column;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: DIFF_COLUMN] ")
              << "Table: \"" << lhs_tab << "\", "
              << "Branch: \"" << lhs_branch << "\", "
              << "Column: \"" << lhs_col_name << "\", "
              << "Table (2nd): \"" << rhs_tab << "\", "
              << "Branch (2nd): \"" << rhs_branch << "\", "
              << "Column (2nd): \"" << rhs_col_name << "\"" << std::endl;
  };
  const auto f_rpt_success = [](ColumnDiffIterator & it_diff) {
    if (Config::is_vert_list) {
      for (; !it_diff.end(); it_diff.next())
        std::cout << it_diff.index() << ": " << it_diff.lhs_value()
                  << " <<>> " << it_diff.rhs_value() << std::endl;
    } else {
      std::cout << BOLD_GREEN("[SUCCESS: DIFF_COLUMN] ")
                << "Different Values: ";
      // TODO(ruanpc): replace PrintListDiff with PrintDiff
      // Utils::PrintDiff(it_diff, true, true);
      Utils::PrintListDiff(it_diff, true, true);
      std::cout << std::endl;
    }
  };
  const auto f_rpt_fail_get_lhs = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: GET_COLUMN] ")
              << "Table: \"" << lhs_tab << "\", "
              << "Branch: \"" << lhs_branch << "\", "
              << "Column: \"" << lhs_col_name << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  const auto f_rpt_fail_get_rhs = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: GET_COLUMN] ")
              << "Table (2nd): \"" << rhs_tab << "\", "
              << "Branch (2nd): \"" << rhs_branch << "\", "
              << "Column (2nd): \"" << rhs_col_name << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (lhs_tab.empty() || lhs_col_name.empty() || lhs_branch.empty() ||
      rhs_branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (rhs_tab.empty()) rhs_tab = lhs_tab;
  if (rhs_col_name.empty()) rhs_col_name = lhs_col_name;
  Column lhs_col;
  auto ec = cs_.GetColumn(lhs_tab, lhs_branch, lhs_col_name, &lhs_col);
  if (ec != ErrorCode::kOK) {
    f_rpt_fail_get_lhs(ec);
    return ec;
  }
  Column rhs_col;
  ec = cs_.GetColumn(rhs_tab, rhs_branch, rhs_col_name, &rhs_col);
  if (ec != ErrorCode::kOK) {
    f_rpt_fail_get_rhs(ec);
    return ec;
  }
  auto it_diff = cs_.DiffColumn(lhs_col, rhs_col);
  f_rpt_success(it_diff);
  return ErrorCode::kOK;
}

ErrorCode Command::ExecExistsTable() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: EXISTS] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success_by_tab = [](const bool exist) {
    std::cout << BOLD_GREEN("[SUCCESS: EXISTS TABLE] ")
              << (exist ? "True" : "False") << std::endl;
  };
  const auto f_rpt_success_by_branch = [](const bool exist) {
    std::cout << BOLD_GREEN("[SUCCESS: EXISTS TABLE] ")
              << (exist ? "True" : "False") << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: EXISTS] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (branch.empty()) {
    bool exist;
    auto ec = cs_.ExistsTable(tab, &exist);
    ec == ErrorCode::kOK ? f_rpt_success_by_tab(exist) : f_rpt_fail(ec);
    return ec;
  } else {
    bool exist;
    auto ec = cs_.ExistsTable(tab, branch, &exist);
    ec == ErrorCode::kOK ? f_rpt_success_by_branch(exist) : f_rpt_fail(ec);
    return ec;
  }
}

ErrorCode Command::ExecExistsColumn() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  const auto& col_name = Config::column;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: EXISTS] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Column: \"" << col_name << "\"" << std::endl;
  };
  const auto f_rpt_success_by_col = [](const bool exist) {
    std::cout << BOLD_GREEN("[SUCCESS: EXISTS COLUMN] ")
              << (exist ? "True" : "False") << std::endl;
  };
  const auto f_rpt_success_by_branch = [](const bool exist) {
    std::cout << BOLD_GREEN("[SUCCESS: EXISTS COLUMN] ")
              << (exist ? "True" : "False") << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: EXISTS] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Column: \"" << col_name << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty() || col_name.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  if (branch.empty()) {
    bool exist;
    auto ec = cs_.ExistsColumn(tab, col_name, &exist);
    ec == ErrorCode::kOK ? f_rpt_success_by_col(exist) : f_rpt_fail(ec);
    return ec;
  } else {
    bool exist;
    auto ec = cs_.ExistsColumn(tab, branch, col_name, &exist);
    ec == ErrorCode::kOK ? f_rpt_success_by_branch(exist) : f_rpt_fail(ec);
    return ec;
  }
}

ErrorCode Command::ExecLoadCSV() {
  const auto& file_path = Config::file;
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  const auto& batch_size = Config::batch_size;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: LOAD_CSV] ")
              << "File: \"" << file_path << "\", "
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << BOLD_GREEN("[SUCCESS: LOAD_CSV] ")
              << "Table \"" << tab << "\" of Branch \"" << branch << "\" "
              << "has been populated" << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: LOAD_CSV] ")
              << "File: \"" << file_path << "\", "
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (file_path.empty() || tab.empty() || branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = cs_.LoadCSV(file_path, tab, branch, batch_size);
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecDumpCSV() {
  const auto& file_path = Config::file;
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: DUMP_CSV] ")
              << "File: \"" << file_path << "\", "
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\"" << std::endl;
  };
  const auto f_rpt_success = [&]() {
    std::cout << BOLD_GREEN("[SUCCESS: DUMP_CSV] ")
              << "Table \"" << tab << "\" of Branch \"" << branch
              << "\" " << "has been exported" << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: DUMP_CSV] ")
              << "File: \"" << file_path << "\", "
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (file_path.empty() || tab.empty() || branch.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = cs_.DumpCSV(file_path, tab, branch);
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecGetRow() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  const auto& ref_col = Config::ref_column;
  const auto& ref_val = Config::ref_value;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: GET_ROW] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Ref. Column: \"" << ref_col << "\", "
              << "Ref. Value: \"" << ref_val << "\"" << std::endl;
  };
  const auto f_rpt_success =
  [this](const std::unordered_map<size_t, Row>& rows) {
    DCHECK(!rows.empty());
    for (auto& i_r : rows) {
      if (Config::is_vert_list) {
        std::cout << "--- Row " << i_r.first << " ---" << std::endl;
        for (auto& field : i_r.second)
          std::cout << field.first << ": " << field.second << std::endl;
      } else {
        std::cout << BOLD_GREEN("[SUCCESS: GET_ROW] ")
                  << "Row " << i_r.first << ": ";
        Utils::Print(i_r.second, "{", "}", "|", " ", " ", ":", true);
        std::cout << std::endl;
      }
    }
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: GET_ROW] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Ref. Column: \"" << ref_col << "\", "
              << "Ref. Value: \"" << ref_val << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty() || branch.empty() || ref_col.empty() || ref_val.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  std::unordered_map<size_t, Row> rows;
  auto ec = cs_.GetRow(tab, branch, ref_col, ref_val, &rows);
  ec == ErrorCode::kOK ? f_rpt_success(rows) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ParseRowString(const std::string& row_str, Row* row) {
  row->clear();
  // TODO(linqian): use regular expression to parse the input value
  auto tokens = Utils::Tokenize(row_str, "{}:|,; ");

  for (auto it = tokens.begin(); it != tokens.end();) {
    auto& k = *it++;
    if (it == tokens.end()) return ErrorCode::kInvalidSchema;
    auto& v = *it++;
    row->emplace(k, v);
  }
  return ErrorCode::kOK;
}

ErrorCode Command::ExecInsert() {
  // redirection
  return ExecInsertRow();
}

ErrorCode Command::ExecInsertRow() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  const auto& val = Config::value;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: INSERT_ROW] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Row: \"" << val << "\"" << std::endl;
  };
  const auto f_rpt_success = [this]() {
    std::cout << BOLD_GREEN("[SUCCESS: INSERT_ROW] ")
              << "1 row has been inserted" << std::endl;
  };
  const auto f_rpt_fail = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: INSERT_ROW] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Row: \"" << val << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty() || branch.empty() || val.empty()) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  Row row;
  auto ec = ParseRowString(val, &row);
  if (ec != ErrorCode::kOK) {
    f_rpt_fail(ec);
    return ec;
  }
  ec = cs_.InsertRow(tab, branch, row);
  ec == ErrorCode::kOK ? f_rpt_success() : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecUpdate() {
  // redirection
  return ExecUpdateRow();
}

ErrorCode Command::ExecUpdateRow() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  const auto& ref_col = Config::ref_column;
  const auto& ref_val = Config::ref_value;
  const auto& row_idx = Config::position;
  const auto& val = Config::value;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: UPDATE_ROW] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Ref. Column: \"" << ref_col << "\", "
              << "Ref. Value: \"" << ref_val << "\", "
              << "Row Index: " << row_idx << ", "
              << "Row Update: \"" << val << "\"" << std::endl;
  };
  const auto f_rpt_success = [this](size_t n_rows_affected) {
    std::cout << BOLD_GREEN("[SUCCESS: UPDATE_ROW] ") << n_rows_affected
              << " row" << (n_rows_affected > 1 ? "s have" : " has")
              << " been updated" << std::endl;
  };
  const auto f_rpt_fail_by_idx = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: UPDATE_ROW] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Row Index: " << row_idx << ", "
              << "Row Update: \"" << val << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  const auto f_rpt_fail_by_col = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: UPDATE_ROW] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Ref. Column: \"" << ref_col << "\", "
              << "Ref. Value: \"" << ref_val << "\", "
              << "Row Update: \"" << val << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty() || branch.empty() || val.empty() ||
      (ref_col.empty() ^ ref_val.empty()) ||
      (!ref_col.empty() && row_idx >= 0)) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  Row row;
  auto ec = ParseRowString(val, &row);
  if (ec != ErrorCode::kOK) {
    row_idx >= 0 ? f_rpt_fail_by_idx(ec) : f_rpt_fail_by_col(ec);
    return ec;
  }
  if (row_idx >= 0) {
    ec = cs_.UpdateRow(tab, branch, row_idx, row);
    ec == ErrorCode::kOK ? f_rpt_success(1) : f_rpt_fail_by_idx(ec);
  } else {  // !ref_col.empty()
    size_t n_rows_affected;
    ec = cs_.UpdateRow(tab, branch, ref_col, ref_val, row, &n_rows_affected);
    ec == ErrorCode::kOK ?
    f_rpt_success(n_rows_affected) : f_rpt_fail_by_col(ec);
  }
  return ec;
}

ErrorCode Command::ExecDeleteRow() {
  const auto& tab = Config::table;
  const auto& branch = Config::branch;
  const auto& ref_col = Config::ref_column;
  const auto& ref_val = Config::ref_value;
  const auto& row_idx = Config::position;
  // screen printing
  const auto f_rpt_invalid_args = [&]() {
    std::cerr << BOLD_RED("[INVALID ARGS: DELETE_ROW] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Ref. Column: \"" << ref_col << "\", "
              << "Ref. Value: \"" << ref_val << "\", "
              << "Row Index: " << row_idx << std::endl;
  };
  const auto f_rpt_success = [this](size_t n_rows_deleted) {
    std::cout << BOLD_GREEN("[SUCCESS: DELETE_ROW] ") << n_rows_deleted
              << " row" << (n_rows_deleted > 1 ? "s have" : " has")
              << " been deleted" << std::endl;
  };
  const auto f_rpt_fail_by_idx = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: DELETE_ROW] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Row Index: " << row_idx
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  const auto f_rpt_fail_by_col = [&](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: DELETE_ROW] ")
              << "Table: \"" << tab << "\", "
              << "Branch: \"" << branch << "\", "
              << "Ref. Column: \"" << ref_col << "\", "
              << "Ref. Value: \"" << ref_val << "\""
              << RED(" --> Error(" << ec << "): " << Utils::ToString(ec))
              << std::endl;
  };
  // conditional execution
  if (tab.empty() || branch.empty() ||
      (ref_col.empty() ^ ref_val.empty()) ||
      (!ref_col.empty() && row_idx >= 0)) {
    f_rpt_invalid_args();
    return ErrorCode::kInvalidCommandArgument;
  }
  auto ec = ErrorCode::kUnknownOp;
  if (row_idx >= 0) {
    ec = cs_.DeleteRow(tab, branch, row_idx);
    ec == ErrorCode::kOK ? f_rpt_success(1) : f_rpt_fail_by_idx(ec);
  } else {  // !ref_col.empty()
    size_t n_rows_deleted;
    ec = cs_.DeleteRow(tab, branch, ref_col, ref_val, &n_rows_deleted);
    ec == ErrorCode::kOK ?
    f_rpt_success(n_rows_deleted) : f_rpt_fail_by_col(ec);
  }
  return ec;
}

ErrorCode Command::ExecInfo() {
  // screen printing
  const auto f_rpt_success = [](const std::vector<StoreInfo>& info) {
    auto n_workers = info.size();
    std::cout << BOLD_GREEN("[SUCCESS: INFO] ")
              << "UStore is running with " << n_workers << " worker instance"
              << (n_workers > 1 ? "s" : "") << std::endl << std::endl;
    for (auto& e : info) std::cout << e << std::endl;
  };
  const auto f_rpt_fail = [](const ErrorCode & ec) {
    std::cerr << BOLD_RED("[FAILED: INFO] ")
              << "--> ErrorCode: " << ec << std::endl;
  };
  // execution
  auto rst = odb_.GetStorageInfo();
  auto& ec = rst.stat;
  auto& info = rst.value;
  ec == ErrorCode::kOK ? f_rpt_success(info) : f_rpt_fail(ec);
  return ec;
}

ErrorCode Command::ExecMeta() {
  auto expected_type = UType::kUnknown;
  // conversion
  auto& tab = Config::table;
  if (!tab.empty() && Config::key.empty()) {
    auto& col = Config::column;
    if (col.empty()) {
      Config::key = tab;
      expected_type = UType::kMap;
    } else {
      Config::key = ColumnStore::GlobalKey(tab, col);
      expected_type = UType::kList;
    }
  }

  const auto f_manip_meta = [&expected_type](const VMeta & meta) {
    auto& ucell = meta.cell();
    // check for type consistency
    auto type = ucell.type();
    if (expected_type != UType::kUnknown && type != expected_type) {
      std::cerr << BOLD_RED("[FAILED: META] ")
                << "Inconsistent types: [Actual] \"" << type << "\", "
                << "[Expected] \"" << expected_type << "\"" << std::endl;
      return ErrorCode::kInconsistentType;
    }
    auto& is_vert_list = Config::is_vert_list;
    std::vector<Hash> prev_vers({ucell.preHash()});
    if (ucell.merged()) prev_vers.emplace_back(ucell.preHash(true));
    auto& os = std::cout;
    auto f_next_item = [&is_vert_list, &os]
    (const std::string & prefix, const std::string & name) -> std::ostream& {
      if (is_vert_list) {
        os << (prefix.empty() ? "" : "\n") << std::left << std::setw(7);
      } else {
        os << prefix;
      }
      os << name << ": ";
      return os;
    };
    os << (is_vert_list ? "" : BOLD_GREEN_STR("[SUCCESS: META] "));
    f_next_item("", "Type") << "\"" << ucell.type() << "\"";
    f_next_item(", ", "Version") << "\"" << ucell.hash() << "\"";
    f_next_item(", ", "Parents");
    Utils::Print(prev_vers, "[", "]", ", ", true);
    os << std::endl;
    return ErrorCode::kOK;
  };
  return ExecManipMeta(f_manip_meta);
}

#define PRINT_ALL(handler) do { \
  limit_print_elems = Utils::max_size_t; \
  auto ec = handler; \
  limit_print_elems = kDefaultLimitPrintElems; \
  return ec; \
} while (0)

ErrorCode Command::ExecGetAll() { PRINT_ALL(ExecGet()); }

ErrorCode Command::ExecListKeyAll() { PRINT_ALL(ExecListKey()); }

ErrorCode Command::ExecLatestAll() { PRINT_ALL(ExecLatest()); }

ErrorCode Command::ExecGetColumnAll() { PRINT_ALL(ExecGetColumn()); }

}  // namespace cli
}  // namespace ustore
