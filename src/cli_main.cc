// Copyright (c) 2017 The Ustore Authors.

#include <chrono>
#include <thread>
#include "cluster/remote_client_service.h"

#include "cli/command.h"
#include "cli/config.h"
#include "cli/console.h"
#include "cli/utils.h"

namespace ustore {
namespace cli {

const int kInitForMs = 75;

int main(int argc, char* argv[]) {
  SetStderrLogging(ERROR);
  // connect to UStore servcie
  RemoteClientService ustore_svc("");
  ustore_svc.Init();
  std::thread ustore_svc_thread(&RemoteClientService::Start, &ustore_svc);
  std::this_thread::sleep_for(std::chrono::milliseconds(kInitForMs));
  auto db = ustore_svc.CreateClientDb();
  // conditional execution
  auto ec =
    argc == 1 ? Console(&db).Run() : Command(&db).Run(argc, argv);
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
