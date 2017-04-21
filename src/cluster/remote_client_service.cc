// Copyright (c) 2017 The Ustore Authors
#include <algorithm>
#include <fstream>
#include <iostream>
#include <thread>
#include "net/rdma_net.h"
#include "net/zmq_net.h"
#include "utils/env.h"
#include "utils/logging.h"
#include "cluster/remote_client_service.h"

namespace ustore {

using std::thread;

class CSCallBack : public CallBack {
 public:
  explicit CSCallBack(void* handler) : CallBack(handler) {}
  void operator()(const void *msg, int size, const node_id_t& source) override {
    (reinterpret_cast<RemoteClientService *>(handler_))->HandleResponse(
                                        msg, size, source);
  }
};

RemoteClientService::~RemoteClientService() {
  delete cb_;
  delete workers_;
  delete net_;
}

int RemoteClientService::range_cmp(const RangeInfo& a, const RangeInfo& b) {
  return Slice(a.start()) < Slice(b.start());
}

// for now, reads configuration from WORKER_FILE and CLIENTSERVICE_FILE
void RemoteClientService::Init() {
  // init the network: connects to the workers
  std::ifstream fin(Env::Instance()->config()->worker_file());
  CHECK(fin) << "Cannot find worker file: "
             << Env::Instance()->config()->worker_file();
  node_id_t worker_addr;
  std::vector<RangeInfo> workers;
  Hash h;
  while (fin >> worker_addr) {
    RangeInfo rif;
    h.Compute((const byte_t*)worker_addr.data(), worker_addr.length());
    rif.set_start(h.ToBase32());
    rif.set_address(worker_addr);
    workers.push_back(rif);
    addresses_.push_back(worker_addr);
  }
  std::sort(workers.begin(), workers.end(), RemoteClientService::range_cmp);

#ifdef USE_RDMA
  net_ = new RdmaNet(node_addr_, Env::Instance()->config()->recv_threads());
#else
  net_ = new ZmqNet(node_addr_, Env::Instance()->config()->recv_threads());
#endif
  fin.close();

  // init worker list
  workers_ = new WorkerList(workers);
}

void RemoteClientService::Start() {
  net_->CreateNetContexts(addresses_);
  cb_ = new CSCallBack(this);
  net_->RegisterRecv(cb_);
#ifdef USE_RDMA
  new thread(&RdmaNet::Start, reinterpret_cast<RdmaNet *>(net_));
  sleep(1.0);
#else
  new thread(&ZmqNet::Start, reinterpret_cast<ZmqNet *>(net_));
#endif
  is_running_ = true;
}

void RemoteClientService::HandleResponse(const void *msg, int size,
                                   const node_id_t& source) {
  UStoreMessage *ustore_msg = new UStoreMessage();
  ustore_msg->ParseFromArray(msg, size);
  ResponseBlob *res_blob = responses_[ustore_msg->source()];

  res_blob->message = ustore_msg;
  std::unique_lock<std::mutex> lck(res_blob->lock);
  res_blob->has_msg = true;
  (res_blob->condition).notify_all();
}

void RemoteClientService::Stop() {
  net_->Stop();
  is_running_ = false;
}

ClientDb* RemoteClientService::CreateClientDb() {
  // adding a new response blob
  ResponseBlob *resblob = new ResponseBlob();
  responses_.push_back(resblob);
  return new ClientDb(master_, nclients_++, net_, resblob, workers_);
}

}  // namespace ustore
