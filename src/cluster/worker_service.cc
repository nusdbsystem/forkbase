// Copyright (c) 2017 The Ustore Authors

#include "cluster/worker_service.h"

#include <algorithm>
#include <iostream>
#include <fstream>
#include "utils/env.h"
#include "spec/slice.h"
#include "hash/hash.h"
#include "net/zmq_net.h"
#include "net/rdma_net.h"
#include "worker/worker.h"
#include "utils/logging.h"

namespace ustore {

class WSCallBack : public CallBack {
 public:
  explicit WSCallBack(void* handler) : CallBack(handler) {}
  void operator()(const void *msg, int size, const node_id_t& source) override {
    (reinterpret_cast<WorkerService *>(handler_))->HandleRequest(
                                        msg, size, source);
  }
};

WorkerService::~WorkerService() {
  delete net_;
  delete cb_;
  delete worker_;
}

int WorkerService::range_cmp(const RangeInfo& a, const RangeInfo& b) {
  return Slice(a.start()) < Slice(b.start());
}

// for now, reads configuration from WORKER_FILE and CLIENTSERVICE_FILE
void WorkerService::Init() {
  // init the network: connects to the workers
  std::ifstream fin(Env::Instance()->config()->worker_file(),
                    std::ifstream::in);
  CHECK(fin);
  node_id_t worker_addr;
  Hash h;
  int worker_id = 0;
  int idx = 0;
  while (fin >> worker_addr) {
    if (worker_addr != node_addr_) {
      RangeInfo rif;
      h.Compute((const byte_t*)worker_addr.data(), worker_addr.length());
      rif.set_start(h.ToBase32());
      rif.set_address(worker_addr);
      ranges_.push_back(rif);
      addresses_.push_back(worker_addr);
    } else {
      worker_id = idx;
    }
    idx++;
  }
  fin.close();

  std::sort(ranges_.begin(), ranges_.end(), range_cmp);
  // add address of the client service
  std::ifstream fin_cs(Env::Instance()->config()->clientservice_file(),
                       std::ifstream::in);
  CHECK(fin_cs);
  node_id_t cs_addr;
  while (fin_cs >> cs_addr)
    addresses_.push_back(cs_addr);
  fin_cs.close();

// TODO(zhanghao): define a static function in net.cc to create net instance,
// instead of directly use USE_RDMA flag everywhere
#ifdef USE_RDMA
  net_ = new RdmaNet(node_addr_, Env::Instance()->config()->recv_threads());
#else
  net_ = new ZmqNet(node_addr_, Env::Instance()->config()->recv_threads());
#endif

  fin.close();
  // init worker
#ifdef MOCK_TEST
  worker_ = new MockWorker(worker_id);
#else
  worker_ = new Worker(worker_id);
#endif
}

void WorkerService::Start() {
  net_->CreateNetContexts(addresses_);
  cb_ = new WSCallBack(this);
  net_->RegisterRecv(cb_);

  net_->Start();
}

void WorkerService::HandleRequest(const void *msg, int size,
                                   const node_id_t& source) {
  // parse the request
  UStoreMessage *ustore_msg = new UStoreMessage();
  ustore_msg->ParseFromArray(msg, size);
  UStoreMessage *response = new UStoreMessage();
  response->set_source(ustore_msg->source());

  ErrorCode error_code;
  CHECK(ustore_msg->has_branch() || ustore_msg->has_version());
  CHECK(!(ustore_msg->has_branch() && ustore_msg->has_version()));
  switch (ustore_msg->type()) {
    case UStoreMessage::PUT_REQUEST:
    {
      UStoreMessage::PutRequestPayload payload =
                      ustore_msg->put_request_payload();
      Hash new_version;
      error_code = ustore_msg->has_branch()
        ? worker_->Put(Slice(ustore_msg->key()),
          Value(Blob((const byte_t*)(payload.value().data()),
                      (payload.value().length()))),
          Slice(ustore_msg->branch()), &new_version)
        : worker_->Put(Slice(ustore_msg->key()),
          Value(Blob((const byte_t*)(payload.value().data()),
                      (payload.value().length()))),
          Hash((const byte_t*)((ustore_msg->version()).data())),
          &new_version);
      UStoreMessage::PutResponsePayload *res_payload =
                      response->mutable_put_response_payload();
      res_payload->set_new_version(new_version.value(), Hash::kByteLength);
      break;
    }
    case UStoreMessage::GET_REQUEST:
    {
      Value *val = new Value();
      error_code = ustore_msg->has_branch()
           ? worker_->Get(Slice(ustore_msg->key()),
                          Slice(ustore_msg->branch()), val)
           : worker_->Get(Slice(ustore_msg->key()),
              Hash((const byte_t*)((ustore_msg->version()).data())), val);

      UStoreMessage::GetResponsePayload *payload =
              response->mutable_get_response_payload();
      payload->set_value((val->blob()).data(), (val->blob()).size());
      delete val;
      break;
    }
    case UStoreMessage::BRANCH_REQUEST:
    {
      UStoreMessage::BranchRequestPayload payload =
                    ustore_msg->branch_request_payload();

      error_code = ustore_msg->has_branch()
        ? worker_->Branch(Slice(ustore_msg->key()),
                      Slice(ustore_msg->branch()),
                      Slice(payload.new_branch()))
        : worker_->Branch(Slice(ustore_msg->key()),
              Hash((const byte_t*)((ustore_msg->version()).data())),
              Slice(payload.new_branch()));

      break;
    }
    case UStoreMessage::MOVE_REQUEST:
    {
      UStoreMessage::MoveRequestPayload payload =
                    (ustore_msg->move_request_payload());
      error_code = worker_->Rename(Slice(ustore_msg->key()),
          Slice(ustore_msg->branch()), Slice(payload.new_branch()));
      break;
    }
    case UStoreMessage::MERGE_REQUEST:
    {
      UStoreMessage::MergeRequestPayload payload =
                    (ustore_msg->merge_request_payload());
      Hash new_version;

      error_code = ustore_msg->has_branch()
        ? worker_->Merge(Slice(ustore_msg->key()),
              Value(Blob((const byte_t*)(payload.value().data()),
                          (payload.value()).length())),
              Slice(payload.target_branch()), Slice(payload.ref_branch()),
              &new_version)
        :  worker_->Merge(Slice(ustore_msg->key()),
              Value(Blob((const byte_t*)(payload.value().data()),
                          (payload.value()).length())),
              Slice(payload.target_branch()),
              Hash((const byte_t*)((ustore_msg->version())).data()),
              &new_version);

      UStoreMessage::MergeResponsePayload *res_payload =
                    response->mutable_merge_response_payload();
      res_payload->set_new_version(new_version.value(), Hash::kByteLength);
      break;
    }
    default:
      break;
  }

  // add Status
  switch (error_code) {
    case ErrorCode::kOK:
      response->set_status(UStoreMessage::SUCCESS);
      break;
    case ErrorCode::kInvalidRange:
      response->set_status(UStoreMessage::INVALID_RANGE);
      break;
    default:
      response->set_status(UStoreMessage::FAILED);
      break;
  }
  // send response back
  byte_t *serialized = new byte_t[response->ByteSize()];
  response->SerializeToArray(serialized, response->ByteSize());
  net_->GetNetContext(source)->Send((const byte_t*)serialized,
                                    (size_t)response->ByteSize());

  // clean up
  delete ustore_msg;
  delete[] serialized;
  delete response;
}

void WorkerService::Stop() {
  net_->Stop();
}
}  // namespace ustore
