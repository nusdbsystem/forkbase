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

ClientDb WorkerClientService::CreateClientDb() {
  // adding a new response blob
  ResponseBlob* resblob = CreateResponseBlob();
  return ClientDb(resblob, &ptt_);
}

CallBack* WorkerClientService::RegisterCallBack() {
  return new WorkerClientServiceCallBack(this);
}

}  // namespace ustore
