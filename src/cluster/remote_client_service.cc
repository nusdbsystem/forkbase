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

int RemoteClientService::range_cmp(const RangeInfo& a, const RangeInfo& b) {
  return Slice(a.start()) < Slice(b.start());
}

// for now, reads configuration from WORKER_FILE and CLIENTSERVICE_FILE
void RemoteClientService::Init() {
  // init the network: connects to the workers
  std::ifstream fin(Env::Instance()->config().worker_file());
  CHECK(fin) << "Cannot find worker file: "
             << Env::Instance()->config().worker_file();
  node_id_t worker_addr;
  std::vector<RangeInfo> workers;
  Hash h;
  while (fin >> worker_addr) {
    RangeInfo rif;
    h = Hash::ComputeFrom((const byte_t*)worker_addr.data(),
                          worker_addr.length());
    rif.set_start(h.ToBase32());
    rif.set_address(worker_addr);
    workers.push_back(rif);
    addresses_.push_back(worker_addr);
  }
  std::sort(workers.begin(), workers.end(), RemoteClientService::range_cmp);

#ifdef USE_RDMA
  net_.reset(new RdmaNet(node_addr_, Env::Instance()->config().recv_threads()));
#else
  // net_ = new ZmqNet(node_addr_, Env::Instance()->config().recv_threads());
  net_.reset(new ClientZmqNet(Env::Instance()->config().recv_threads()));
#endif
  fin.close();

  // init worker list
  workers_.Update(workers);
}

void RemoteClientService::Start() {
  net_->CreateNetContexts(addresses_);
  cb_.reset(new CSCallBack(this));
  net_->RegisterRecv(cb_.get());
  // zh: make the start behavior consistent with the worker service
  is_running_ = true;
  net_->Start();
// #ifdef USE_RDMA
//   new thread(&RdmaNet::Start, reinterpret_cast<RdmaNet *>(net_));
//   sleep(1.0);
// #else
//   new thread(&ZmqNet::Start, reinterpret_cast<ZmqNet *>(net_));
// #endif
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
  // net_thread_->join();
}

ClientDb RemoteClientService::CreateClientDb() {
  // adding a new response blob
  ResponseBlob* resblob = new ResponseBlob();
  responses_.push_back(std::unique_ptr<ResponseBlob>(resblob));
  return ClientDb(master_, nclients_++, net_.get(), resblob, &workers_);
}

}  // namespace ustore
