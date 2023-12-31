// Copyright (c) 2017 The Ustore Authors.

#include <iterator>
#include <utility>

#include <boost/algorithm/string.hpp>

#include "cli/config.h"

namespace ustore {
namespace cli {

bool Config::is_help;
std::string Config::command_options_help;
std::string Config::command;
std::string Config::script;
bool Config::ignore_fail = false;
std::string Config::expected_fail;
std::string Config::file;
bool Config::time_exec;
bool Config::is_vert_list;
UType Config::type;
std::string Config::key;
std::string Config::map_key;
std::string Config::value;
std::string Config::ref_value;
std::string Config::branch;
std::string Config::ref_branch;
std::string Config::version;
std::string Config::ref_version;
std::string Config::table;
std::string Config::ref_table;
std::string Config::column;
std::string Config::ref_column;
bool Config::distinct;
int64_t Config::position;
int64_t Config::ref_position;
size_t Config::num_elements;
size_t Config::batch_size;
bool Config::with_schema;
bool Config::overwrite_schema;
bool Config::show_meta_value;
size_t Config::bs_batch_size;

std::list<std::string> Config::history_vers_;

void Config::Reset() {
  is_help = false;
  command_options_help = "";
  command = "";
  script = "";
  expected_fail = "";
  file = "";
  time_exec = false;
  is_vert_list = false;
  type = UType::kBlob;
  key = "";
  map_key = "";
  value = "";
  ref_value = "";
  branch = "";
  ref_branch = "";
  version = "";
  ref_version = "";
  table = "";
  ref_table = "";
  column = "";
  ref_column = "";
  distinct = false;
  position = -1;
  ref_position = -1;
  num_elements = 1;
  batch_size = 5000;
  with_schema = false;
  overwrite_schema = false;
  show_meta_value = false;
  bs_batch_size = 800;
}

bool Config::ParseCmdArgs(int argc, char* argv[]) {
  Reset();
  po::variables_map vm;
  GUARD(ParseCmdArgs(argc, argv, &vm));
  try {
    command = vm["command"].as<std::string>();
    boost::to_upper(command);

    script = vm["script"].as<std::string>();
    if (!script.empty()) {
      ignore_fail = vm.count("ignore-fail") ? true : false;
    }
    expected_fail = vm["expect-fail"].as<std::string>();
    file = vm["file"].as<std::string>();

    time_exec = vm.count("time") ? true : false;
    is_vert_list = vm.count("vert-list") ? true : false;

    auto arg_type = vm["type"].as<std::string>();
    boost::to_lower(arg_type);
    type = Utils::ToUType(arg_type);
    GUARD(CheckArg(arg_type, type != UType::kUnknown, "Data type",
                   "<supported type>"));

    key = vm["key"].as<std::string>();
    map_key = vm["map-key"].as<std::string>();
    value = vm["value"].as<std::string>();
    ref_value = vm["ref-value"].as<std::string>();

    branch = vm["branch"].as<std::string>();
    ref_branch = vm["ref-branch"].as<std::string>();

    version = vm["version"].as<std::string>();
    ParseHistoryVersion(&version);
    GUARD(CheckArg(version.size(), version.empty() || version.size() == 32,
                   "Length of the operating version", "0 or 32"));

    ref_version = vm["ref-version"].as<std::string>();
    ParseHistoryVersion(&ref_version);
    GUARD(CheckArg(ref_version.size(),
                   ref_version.empty() || ref_version.size() == 32,
                   "Length of the referring version", "0 or 32"));

    table = vm["table"].as<std::string>();
    ref_table = vm["ref-table"].as<std::string>();
    distinct = vm.count("distinct") ? true : false;

    column = vm["column"].as<std::string>();
    ref_column = vm["ref-column"].as<std::string>();

    if (vm.count("position")) {
      position = vm["position"].as<int64_t>();
      GUARD(CheckArgGE(position, 0, "Operating positional index"));
    }
    if (vm.count("ref-position")) {
      ref_position = vm["ref-position"].as<int64_t>();
      CheckArgGE(ref_position, 0, "Referring positional index");
    }

    auto arg_num_elements = vm["num-elements"].as<int32_t>();
    GUARD(CheckArgGT(arg_num_elements, 0, "Number of elements"));
    num_elements = static_cast<size_t>(arg_num_elements);

    auto arg_batch_size = vm["batch-size"].as<int32_t>();
    GUARD(CheckArgGT(arg_batch_size, 0, "Batch size"));
    batch_size = static_cast<size_t>(arg_batch_size);

    if (vm.count("with-schema")) {
      with_schema = true;
      overwrite_schema = vm.count("overwrite-schema") ? true: false;
    }

    show_meta_value = vm.count("meta-value") ? true : false;

    auto arg_bs_batch_size = vm["blobstore-batch-size"].as<int32_t>();
    GUARD(CheckArgGT(arg_bs_batch_size, 0, "BlobStore Batch size"));
    bs_batch_size = static_cast<size_t>(arg_bs_batch_size);

    auto arg_msg_timeout = vm["msg-timeout"].as<int32_t>();
    GUARD(CheckArgGT(arg_msg_timeout, 0, "Message Timeout"));
    ustore::ClientZmqNet::msg_timeout = static_cast<size_t>(arg_msg_timeout);
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl;
    return false;
  }
  return true;
}

bool Config::ParseCmdArgs(int argc, char* argv[], po::variables_map* vm) {
  po::options_description utility(BLUE_STR("Utility Options"), 120);
  utility.add_options()
  ("help,?", "print usage message")
  ("script,S", po::value<std::string>()->default_value(""),
   "script of UStore commands")
  ("expect-fail,F", po::value<std::string>()->default_value(""),
   "description of expected failure")
  ("time,T", "show execution time of command")
  ("vert-list,1", "list one entry per line");

  po::options_description general(BLUE_STR("General Options"), 120);
  general.add_options()
  ("type,p", po::value<std::string>()->default_value("Blob"),
   "data type")
  ("key,k", po::value<std::string>()->default_value(""),
   "key of data")
  ("map-key,e", po::value<std::string>()->default_value(""),
   "key of map entry")
  ("value,x", po::value<std::string>()->default_value(""),
   "data value")
  ("ref-value,y", po::value<std::string>()->default_value(""),
   "the referring data value")
  ("branch,b", po::value<std::string>()->default_value(""),
   "the operating branch")
  ("ref-branch,c", po::value<std::string>()->default_value(""),
   "the referring branch")
  ("version,v", po::value<std::string>()->default_value(""),
   "the operating version")
  ("ref-version,u", po::value<std::string>()->default_value(""),
   "the referring version")
  ("table,t", po::value<std::string>()->default_value(""),
   "the operating table or dataset")
  ("ref-table,s", po::value<std::string>()->default_value(""),
   "the referring table or dataset")
  ("column,m", po::value<std::string>()->default_value(""),
   "the operating column or data entry")
  ("ref-column,n", po::value<std::string>()->default_value(""),
   "the referring column or data entry")
  ("position,i", po::value<int64_t>(), "the operating positional index")
  ("ref-position,j", po::value<int64_t>(), "the referring positional index")
  ("num-elements,d", po::value<int32_t>()->default_value(1),
   "number of elements")
  ("distinct", "enforcing the distinct constraint")
  ("batch-size", po::value<int32_t>()->default_value(5000),
   "batch size for data loading")
  ("with-schema", "input data containing schema at the 1st line")
  ("overwrite-schema", "overwrite schema");

  po::options_description backend("Hidden Options");
  backend.add_options()
  ("command", po::value<std::string>()->default_value(""),
   "UStore command")
  ("file", po::value<std::string>()->default_value(""),
   "path of input/output file")
  ("ignore-fail", "ignore command failure in script execution")
  ("meta-value", "show meta value")
  ("blobstore-batch-size", po::value<int32_t>()->default_value(800),
   "batch size for csv loading in blobstore")
  ("msg-timeout", po::value<int32_t>()->default_value(3000),
   "message timeout");

  po::positional_options_description pos_opts;
  pos_opts.add("command", 1);
  pos_opts.add("file", 1);

  po::options_description all("Allowed Options");
  all.add(utility).add(general).add(backend);

  po::options_description visible;
  visible.add(utility).add(general);

  try {
    po::store(po::command_line_parser(argc, argv).options(all)
              .style(po::command_line_style::unix_style)
              .positional(pos_opts).run(), *vm);
    if (vm->count("help")) {
      is_help = true;
      std::stringstream ss;
      ss << visible;
      command_options_help = ss.str();
    }
    po::notify(*vm);
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ") << e.what() << std::endl;
    return false;
  }
  return true;
}

bool Config::ParseCmdArgs(const std::vector<std::string>& args) {
  size_t argc = args.size() + 1;
  static char dummy_cmd[] = "ustore_cli";
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

bool Config::ParseHistoryVersion(std::string* ver) {
  if (!boost::starts_with(*ver, "@") || history_vers_.empty()) return false;
  try {
    auto idx = std::stoi(ver->substr(1));
    if (idx <= 0 || static_cast<size_t>(idx) > history_vers_.size()) {
      std::cerr << BOLD_RED("[ERROR] ")
                << "Index out of range: [Actual] " << *ver
                << ", [Expected] @1.." << history_vers_.size() << std::endl;
      return false;
    }
    std::cout << BOLD_GREEN("[CONVERTED] ") << *ver << ": ";
    *ver = idx == 1
           ? history_vers_.front()
           : *(std::next(history_vers_.begin(), idx - 1));
    std::cout << '\"' << *ver << '\"' << std::endl;
  } catch (std::exception& e) {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Failed to parse placeholder: " << *ver << std::endl;
    return false;
  }
  return true;
}

}  // namespace cli
}  // namespace ustore
