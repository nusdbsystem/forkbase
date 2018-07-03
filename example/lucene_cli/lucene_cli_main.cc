// Copyright (c) 2017 The Ustore Authors.

#include "cluster/worker_client_service.h"
#include "worker/worker.h"

#include "lucene_cli_arguments.h"
#include "lucene_client.h"

namespace ustore {
namespace example {
namespace lucene_cli {

int main(int argc, char* argv[]) {
  SetStderrLogging(ERROR);
  // parse command-line arguments
  LuceneCLIArguments args;
  if (!args.ParseCmdArgs(argc, argv)) {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Found invalid command-line option" << std::endl;
    return static_cast<int>(ErrorCode::kInvalidCommandArgument);
  }
  if (args.is_help) return static_cast<int>(ErrorCode::kOK);
  // execution
  WorkerClientService svc;
  svc.Run();
  auto db = svc.CreateWorkerClient();
  return static_cast<int>(LuceneClient(args, &db).Run());
}

}  // namespace lucene_cli
}  // namespace example
}  // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::example::lucene_cli::main(argc, argv);
}
