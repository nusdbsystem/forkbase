// Copyright (c) 2017 The Ustore Authors.

#include <chrono>
#include "utils/logging.h"
#include "utils/service_context.h"

namespace ustore {

void ServiceContext::Start() {
  if (svc_thread_) {
    LOG(WARNING) << "UStore service has been already started";
  } else {
    svc_thread_.reset(new std::thread(&WorkerClientService::Start, &svc_));
  }
}

void ServiceContext::Stop() {
  if (!svc_thread_) {
    LOG(WARNING) << "UStore service is not yet started";
  } else {
    svc_.Stop();
    svc_thread_->join();
    svc_thread_.reset(nullptr);
  }
}

}  // namespace ustore
