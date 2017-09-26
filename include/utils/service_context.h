// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_UTILS_SERVICE_CONTEXT_H_
#define USTORE_UTILS_SERVICE_CONTEXT_H_

#include <memory>
#include <thread>
#include "cluster/worker_client_service.h"
#include "utils/noncopyable.h"

namespace ustore {

class ServiceContext : private Noncopyable {
 public:
  ServiceContext() { Start(); }
  ~ServiceContext() { Stop(); }

  void Start();
  void Stop();

  inline WorkerClient GetWorkerClient() { return svc_.CreateWorkerClient(); }

 private:
  WorkerClientService svc_;
  std::unique_ptr<std::thread> svc_thread_;
};

}  // namespace ustore

#endif  // USTORE_UTILS_SERVICE_CONTEXT_H_
