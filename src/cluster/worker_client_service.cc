// Copyright (c) 2017 The Ustore Authors

#include "cluster/worker_client_service.h"

namespace ustore {

class WorkerClientServiceCallBack : public CallBack {
 public:
  explicit WorkerClientServiceCallBack(void* handler) : CallBack(handler) {}
  void operator()(const void *msg, int size, const node_id_t& source) override {
    (reinterpret_cast<WorkerClientService*>(handler_))->HandleResponse(
                                        msg, size, source);
  }
};

void WorkerClientService::Init() {
  CallBack* callback = new WorkerClientServiceCallBack(this);
  ClientService::Init(std::unique_ptr<CallBack>(callback));
  // start chunk client service as well
  if (ck_svc_) ck_svc_->Run();
}

WorkerClient WorkerClientService::CreateWorkerClient() {
  // adding a new response blob
  ResponseBlob* resblob = CreateResponseBlob();
  return WorkerClient(resblob, &ptt_, ck_svc_.get());
}

}  // namespace ustore
