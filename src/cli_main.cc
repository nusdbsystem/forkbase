// Copyright (c) 2017 The Ustore Authors.

#include <chrono>
#include <thread>
#include "cluster/remote_client_service.h"

#include "cli/command.h"
#include "cli/config.h"
#include "cli/utils.h"

namespace ustore {
namespace cli {

constexpr int kInitForMs = 75;

int main(int argc, char* argv[]) {
  SetStderrLogging(ERROR);
  // connect to UStore servcie
  RemoteClientService ustore_svc("");
  ustore_svc.Init();
  std::thread ustore_svc_thread(&RemoteClientService::Start, &ustore_svc);
  std::this_thread::sleep_for(std::chrono::milliseconds(kInitForMs));
  auto client_db = ustore_svc.CreateClientDb();
  Command cmd(&client_db);
  // conditional execution
  auto ec = ErrorCode::kUnknownOp;
  if (argc == 1) {
    ec = cmd.ExecConsole();
  } else if (Config::ParseCmdArgs(argc, argv)) {
    if (!Config::command.empty() && Config::script.empty()) {
      ec = cmd.ExecCommand(Config::command);
    } else if (Config::command.empty() && !Config::script.empty()) {
      ec = cmd.ExecScript(Config::script);
    } else {
      std::cerr << BOLD_RED("[ERROR] ")
                << "Either UStore command or \"--script\" must be given, "
                << "but not both" << std::endl;
      ec = ErrorCode::kInvalidCommandArgument;
    }
  } else if (Config::is_help) {
    DLOG(INFO) << "Help messages have been printed";
    ec = ErrorCode::kOK;
  } else {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Found invalid command-line option" << std::endl;
    ec = ErrorCode::kInvalidCommandArgument;
  }
  // disconnect with UStore service
  ustore_svc.Stop();
  ustore_svc_thread.join();
  return static_cast<int>(ec);
}

}  // namespace cli
}  // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::cli::main(argc, argv);
}
