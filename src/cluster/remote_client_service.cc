// Copyright (c) 2017 The Ustore Authors

#include <thread>
#include "net/rdma_net.h"
#include "net/zmq_net.h"
#include "utils/env.h"
#include "utils/logging.h"
#include "cluster/remote_client_service.h"

namespace ustore {

using std::thread;

class ClientServiceCallBack : public CallBack {
 public:
  explicit ClientServiceCallBack(void* handler) : CallBack(handler) {}
  void operator()(const void *msg, int size, const node_id_t& source) override {
    (reinterpret_cast<RemoteClientService *>(handler_))->HandleResponse(
                                        msg, size, source);
  }
};

// for now, reads configuration from WORKER_FILE and CLIENTSERVICE_FILE
void RemoteClientService::Init() {
#ifdef USE_RDMA
  net_.reset(new RdmaNet("", Env::Instance()->config().recv_threads()));
#else
  net_.reset(new ClientZmqNet(Env::Instance()->config().recv_threads()));
#endif
}

void RemoteClientService::Start() {
  net_->CreateNetContexts(ptt_.workerAddrs());
  cb_.reset(new ClientServiceCallBack(this));
  net_->RegisterRecv(cb_.get());

  is_running_ = true;
  net_->Start();
}

void RemoteClientService::HandleResponse(const void *msg, int size,
                                   const node_id_t& source) {
  UMessage *ustore_msg = new UMessage();
  ustore_msg->ParseFromArray(msg, size);
  ResponseBlob* res_blob = responses_[ustore_msg->source()].get();

  std::unique_lock<std::mutex> lck(res_blob->lock);
  res_blob->message = ustore_msg;
  res_blob->has_msg = true;
  res_blob->condition.notify_all();
}

void RemoteClientService::Stop() {
  is_running_ = false;
  net_->Stop();
}

ClientDb RemoteClientService::CreateClientDb() {
  // adding a new response blob
  ResponseBlob* resblob = new ResponseBlob();
  responses_.emplace_back(resblob);
  return ClientDb(master_, nclients_++, net_.get(), resblob, &ptt_);
}

}  // namespace ustore
