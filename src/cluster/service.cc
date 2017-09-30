// Copyright (c) 2017 The Ustore Authors

#include "cluster/service.h"

#include <chrono>
#include "utils/env.h"

namespace ustore {

constexpr int kInitForMs = 75;

void Service::Init(std::unique_ptr<Net> net,
                   std::unique_ptr<CallBack> callback) {
  net_ = std::move(net);
  cb_ = std::move(callback);
  net_->RegisterRecv(cb_.get());
  is_init_ = true;
}

void Service::Start() {
  if (is_running_) {
    LOG(WARNING) << "service is already started";
    return;
  }
  if (!is_init_) Init();
  is_running_ = true;
  net_->Start();
}

void Service::Run() {
  if (thread_) {
    LOG(WARNING) << "service thread is already running";
    return;
  }
  if (!is_init_) Init();
  thread_.reset(new std::thread(&Service::Start, this));
  std::this_thread::sleep_for(std::chrono::milliseconds(kInitForMs));
}

void Service::Stop() {
  if (!is_running_) return;
  net_->Stop();
  if (thread_) {
    thread_->join();
    thread_.reset(nullptr);
  }
  is_running_ = false;
}

}  // namespace ustore
