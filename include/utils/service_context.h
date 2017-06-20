// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_UTILS_SERVICE_CONTEXT_H_
#define USTORE_UTILS_SERVICE_CONTEXT_H_

#include <boost/optional.hpp>
#include <chrono>
#include <thread>
#include "cluster/remote_client_service.h"

namespace ustore {

class ServiceContext : private Noncopyable {
 public:
  explicit ServiceContext(const node_id_t& master) : svc_(master) { Start(); }
  ServiceContext() : ServiceContext("") {}
  ~ServiceContext() { Stop(); }

  void Start();
  void Stop();

  inline ClientDb GetClientDb() { return svc_.CreateClientDb(); }

 private:
  static const int kInitForMs;
  RemoteClientService svc_;
  boost::optional<std::thread> svc_thread_opt_;
};

}  // namespace ustore

#endif  // USTORE_UTILS_SERVICE_CONTEXT_H_
