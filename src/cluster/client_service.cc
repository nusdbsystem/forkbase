// Copyright (c) 2017 The Ustore Authors
#include <iostream>
#include <thread>
#include <fstream>
#include <algorithm>
#include "cluster/client_service.h"
#include "utils/config.h"
#include "net/zmq_net.h"
#include "net/rdma_net.h"
#include "utils/logging.h"

using std::sort;
using std::thread;
using std::unique_lock;
using std::ifstream;

namespace ustore {

class CSCallBack: public CallBack {
 public:
  CSCallBack(void* handler): CallBack(handler) {};
  void operator()(const void *msg, int size, const node_id_t& source) override {
    (reinterpret_cast<ClientService *>(handler_))->HandleResponse(
                                        msg, size, source);
  }
};

ClientService::~ClientService() {
  delete cb_;
  delete workers_;
  delete workload_;
  delete net_;
}

int ClientService::range_cmp(RangeInfo a, RangeInfo b) {
  return Slice(a.start()) < Slice(b.start());
}

// for now, reads configuration from WORKER_FILE and CLIENTSERVICE_FILE
void ClientService::Init() {
  // init the network: connects to the workers
  ifstream fin(Config::WORKER_FILE);
  CHECK(fin);
  node_id_t worker_addr;
  vector<RangeInfo> workers;
  Hash h;
  while (fin >> worker_addr) {
    RangeInfo rif;
    h.Compute((const byte_t*)worker_addr.data(), worker_addr.length());
    rif.set_start(h.ToBase32());
    rif.set_address(worker_addr);
    workers.push_back(rif);
    addresses_.push_back(worker_addr);
  }

  sort(workers.begin(), workers.end(), ClientService::range_cmp);

#ifdef USE_RDMA
  net_ = new RdmaNet(node_addr_, Config::RECV_THREADS);
#else
  net_ = new ZmqNet(node_addr_, Config::RECV_THREADS);
#endif
  fin.close();

  // init worker list
  workers_ = new WorkerList(workers);

  // init response queue
  for (int i = 0; i < Config::SERVICE_THREADS; i++)
    responses_.push_back(new ResponseBlob());
}

void ClientService::Start() {
  vector<thread> client_threads;

  net_->CreateNetContexts(addresses_);

  cb_ = new CSCallBack(this);
  net_->RegisterRecv(cb_);
#ifdef USE_RDMA
  new thread(&RdmaNet::Start, reinterpret_cast<RdmaNet *>(net_));
  sleep(1.0);
#else
  new thread(&ZmqNet::Start, reinterpret_cast<ZmqNet *>(net_));
#endif

  for (int i = 0; i < Config::SERVICE_THREADS; i++)
    client_threads.push_back(thread(&ClientService::ClientThread, this,
                                      master_, i));
  is_running_ = true;
  for (int i=0; i < Config::SERVICE_THREADS; i++)
    client_threads[i].join();
}

void ClientService::HandleResponse(const void *msg, int size,
                                   const node_id_t& source) {
  UStoreMessage *ustore_msg = new UStoreMessage();
  ustore_msg->ParseFromArray(msg, size);
  ResponseBlob *res_blob = responses_[ustore_msg->source()];

  res_blob->message = ustore_msg;
  unique_lock<mutex> lck(res_blob->lock);
  res_blob->has_msg = true;
  (res_blob->condition).notify_all();
}

void ClientService::Stop() {
  net_->Stop();
  is_running_ = false;
}

void ClientService::ClientThread(const node_id_t& master, int thread_id) {
  RequestHandler *reqhl = new RequestHandler(master, thread_id, net_,
                                         responses_[thread_id], workers_);

  while (is_running_ && workload_->NextRequest(reqhl)) {}
}
}  // namespace ustore
