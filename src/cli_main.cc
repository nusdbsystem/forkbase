// Copyright (c) 2017 The Ustore Authors.

#include "utils/service_context.h"
#include "cli/command.h"
#include "cli/console.h"

namespace ustore {
namespace cli {

int main(int argc, char* argv[]) {
  SetStderrLogging(ERROR);
  // connect to UStore servcie
  ServiceContext svc_ctx;
  auto db = svc_ctx.GetClientDb();
  // conditional execution
  return static_cast<int>(
           argc == 1 ? Console(&db).Run() : Command(&db).Run(argc, argv));
}

}  // namespace cli
}  // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::cli::main(argc, argv);
}
