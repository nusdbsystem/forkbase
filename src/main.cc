// Copyright (c) 2017 The Ustore Authors.

#include <gflags/gflags.h>
#include <cassert>
#include "utils/env.h"
#include "cluster/worker_service.h"
#include "cluster/remote_client_service.h"

using ustore::WorkerService;
using ustore::RemoteClientService;
using ustore::WARNING;

DEFINE_bool(client_service, false, "is a client service");
DEFINE_bool(worker, false, "is a worker");
DEFINE_string(node_id, "", "ip address of this node (ib0 interface)");
DEFINE_int32(loglevel, ustore::WARNING, "logging severity level");

int main(int argc, char **argv) {
  if (argc < 4) {
    std::cout << "Args: --client_service|--worker "
              << " --node_id ip:port\n";
    return 0;
  }

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // either client service or worker must be set
  assert((FLAGS_client_service || FLAGS_worker));

  // set the logging severity level
  ustore::SetStderrLogging(FLAGS_loglevel);

  if (FLAGS_worker) {  // start the worker
    WorkerService ws(FLAGS_node_id, "");
    ws.Init();
    ws.Start();
  } else {  // start the client service
    RemoteClientService cs(FLAGS_node_id, "");
    cs.Init();
    cs.Start();
  }

  return 0;
}

