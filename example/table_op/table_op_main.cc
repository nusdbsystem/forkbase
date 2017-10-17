// Copyright (c) 2017 The Ustore Authors.

#include "cluster/worker_client_service.h"
#include "table_op.h"

namespace ustore {
namespace example {
namespace table_op {

int main(int argc, char* argv[]) {
  SetStderrLogging(WARNING);
  // connect to UStore servcie
  WorkerClientService svc;
  svc.Run();
  auto db = svc.CreateWorkerClient();
  // execution
  return static_cast<int>(TableOp(&db).Run(argc, argv));
}

}  // namespace table_op
}  // namespace example
}  // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::example::table_op::main(argc, argv);
}
