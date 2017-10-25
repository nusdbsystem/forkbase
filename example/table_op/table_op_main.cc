// Copyright (c) 2017 The Ustore Authors.

#include "cluster/worker_client_service.h"

#include "table_gen.h"
#include "table_op.h"
#include "table_op_arguments.h"

namespace ustore {
namespace example {
namespace table_op {

int main(int argc, char* argv[]) {
  SetStderrLogging(WARNING);
  // parse command-line arguments
  TableOpArguments args;
  if (!args.ParseCmdArgs(argc, argv)) {
    std::cerr << BOLD_RED("[ERROR] ")
              << "Found invalid command-line option" << std::endl;
    return static_cast<int>(ErrorCode::kInvalidCommandArgument);
  }
  if (args.is_help) return static_cast<int>(ErrorCode::kOK);
  // execution
  if (args.to_gen_data) {
    auto ec = TableGen(args).Run();
    if (ec == ErrorCode::kOK) {
      std::cout << BOLD_GREEN("[SUCCESS: Table Gen] ")
                << "Data: " << YELLOW(args.file)
                << ", Query: " << YELLOW(args.update_ref_val) << std::endl;
    } else {
      std::cout << BOLD_RED("[FAILED: Table Gen] ")
                << "Error(" << ec << "): " << Utils::ToString(ec) << std::endl;
    }
    return static_cast<int>(ec);
  } else {
    WorkerClientService svc;
    svc.Run();
    auto db = svc.CreateWorkerClient();
    auto ec = TableOp(args, &db).Run();
    if (ec != ErrorCode::kOK) {
      std::cout << BOLD_RED("[FAILED: Table Op] ")
                << "Error(" << ec << "): " << Utils::ToString(ec) << std::endl;
    }
    return static_cast<int>(ec);
  }
}

}  // namespace table_op
}  // namespace example
}  // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::example::table_op::main(argc, argv);
}
