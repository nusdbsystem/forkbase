// Copyright (c) 2017 The Ustore Authors.

#include <chrono>
#include <thread>
#include "cluster/remote_client_service.h"

#include "cli/command.h"
#include "cli/config.h"
#include "cli/utils.h"

namespace ustore {
namespace cli {

constexpr int kWaitForSvcReadyInMs = 75;

int main(int argc, char* argv[]) {
  SetStderrLogging(WARNING);
  RemoteClientService ustore_svc("");
  ustore_svc.Init();
  std::thread ustore_svc_thread(&RemoteClientService::Start, &ustore_svc);
  std::this_thread::sleep_for(std::chrono::milliseconds(kWaitForSvcReadyInMs));
  auto client_db = ustore_svc.CreateClientDb();
  Command cmd(client_db);
  // conditional execution
  auto ec = ErrorCode::kUnknownOp;
  if (Config::ParseCmdArgs(argc, argv)) {
    ec = cmd.ExecCommand(Config::command);
  } else if (Config::is_help) {
    DLOG(INFO) << "Help messages have been printed";
    ec = ErrorCode::kOK;
  } else {
    std::cerr << "[FAILURE] Found invalid command-line option" << std::endl;
    ec = ErrorCode::kInvalidCommandArgument;
  }
  // clean and exit
  ustore_svc.Stop();
  ustore_svc_thread.join();
  return static_cast<int>(ec);
}

}  // namespace cli
}  // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::cli::main(argc, argv);
}
