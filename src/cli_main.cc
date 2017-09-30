// Copyright (c) 2017 The Ustore Authors.

#include "cli/command.h"
#include "cli/console.h"
#include "cluster/worker_client_service.h"

namespace ustore {
namespace cli {

int main(int argc, char* argv[]) {
  SetStderrLogging(ERROR);
  // connect to UStore servcie
  WorkerClientService svc;
  svc.Run();
  auto db = svc.CreateWorkerClient();
  // conditional execution
  return static_cast<int>(
           argc == 1 ? Console(&db).Run() : Command(&db).Run(argc, argv));
}

}  // namespace cli
}  // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::cli::main(argc, argv);
}
