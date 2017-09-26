// Copyright (c) 2017 The Ustore Authors.

#include <chrono>
#include "utils/logging.h"
#include "utils/service_context.h"

namespace ustore {

constexpr int kInitForMs  = 75;

void ServiceContext::Start() {
  if (svc_thread_) {
    LOG(WARNING) << "UStore service has been already started";
  } else {
    svc_.Init();
    svc_thread_.reset(new std::thread(&WorkerClientService::Start, &svc_));
    std::this_thread::sleep_for(std::chrono::milliseconds(kInitForMs));
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
