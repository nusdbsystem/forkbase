// Copyright (c) 2017 The Ustore Authors.

#include <cstring>
#include <thread>
#include "utils/env.h"
#include "utils/logging.h"
#include "http/server.h"
#include "cluster/worker_client.h"
#include "cluster/worker_client_service.h"

namespace ustore {
namespace http {

int main(int argc, char* argv[]) {
  int port = Env::Instance()->config().http_port();
  int threads = 1;  // number of threads used by the server
  int elsize = 10000;  // event loop size (max concurrent connections supported)
  std::string bind_addr = "";

  // the first argument should be the program name
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--port") == 0) {
      port = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--threads") == 0) {
      threads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--connections") == 0) {
      elsize = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--bind_addr") == 0) {
      bind_addr = string(argv[++i]);
    } else if (strcmp(argv[i], "--help") == 0) {
      printf("Usage:\n./server\n"
          "[--port port (default: %d)]\n"
          "[--bind_addr (default: )]\n"
          "[--threads threads (default: %d)]\n"
          "[--connections supported_max_connections (default: %d)]\n"
          , port, threads, elsize);
      return -1;
    } else {
      fprintf(stderr, "Unrecognized option %s for benchmark\n", argv[i]);
    }
  }

  printf("Http client configuration: port: %d, "
      "bind_addr: %s, threads: %d, connections: %d\n",
      port, bind_addr.c_str(), threads, elsize);

  // launch clients
  WorkerClientService service;
  service.Run();

  WorkerClient client = service.CreateWorkerClient();
  HttpServer server(&client, port, bind_addr);  // create the HttpServer
  // set the max concurrent connections to support
  server.SetEventLoopSize(elsize);

  if (server.Start(threads) != ST_SUCCESS) {  // start the http server
    printf("start httpserver error\n");
  }

  // exit and cleanup
  service.Stop();
  return 0;
}

}  // namespace http
}  // namespace ustore

int main(int argc, char* argv[]) {
  return ustore::http::main(argc, argv);
}
