// Copyright (c) 2017 The Ustore Authors.

#include "utils/logging.h"
#include "utils/service_context.h"

namespace ustore {

const int ServiceContext::kInitForMs = 75;

void ServiceContext::Start() {
  if (svc_thread_opt_) {
    LOG(WARNING) << "UStore service has been already started";
  } else {
    svc_.Init();
    svc_thread_opt_ = boost::optional<std::thread>(
                        std::thread(&RemoteClientService::Start, &svc_));
    std::this_thread::sleep_for(std::chrono::milliseconds(kInitForMs));
  }
}

void ServiceContext::Stop() {
  if (!svc_thread_opt_) {
    LOG(WARNING) << "UStore service is not yet started";
  } else {
    svc_.Stop();
    svc_thread_opt_->join();
    svc_thread_opt_ = boost::none;
  }
}

}  // namespace ustore
