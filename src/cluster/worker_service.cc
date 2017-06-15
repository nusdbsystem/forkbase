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

using std::vector;
using std::string;

class WSCallBack : public CallBack {
 public:
  explicit WSCallBack(void *handler) : CallBack(handler) {}
  void operator()(const void *msg, int size, const node_id_t &source) override {
    (reinterpret_cast<WorkerService *>(handler_))
        ->HandleRequest(msg, size, source);
  }
};

int WorkerService::range_cmp(const RangeInfo& a, const RangeInfo& b) {
  return Slice(a.start()) < Slice(b.start());
}

// for now, reads configuration from WORKER_FILE and CLIENTSERVICE_FILE
void WorkerService::Init() {
  // init the network: connects to the workers
  std::ifstream fin(Env::Instance()->config().worker_file(), std::ifstream::in);
  CHECK(fin);
  node_id_t worker_addr;
  Hash h;
  int worker_id = 0;
  int idx = 0;
  while (fin >> worker_addr) {
    if (worker_addr != node_addr_) {
      RangeInfo rif;
      h = Hash::ComputeFrom(reinterpret_cast<const byte_t*>(worker_addr.data()),
                            worker_addr.length());
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

  // TODO(zhanghao): check why letting this after RdmaNet make NetContext
  //                 destroyed unexpected?
  worker_.reset(new Worker(worker_id, persist_));

// TODO(zhanghao): define a static function in net.cc to create net instance,
// instead of directly use USE_RDMA flag everywhere
#ifdef USE_RDMA
  // net_ = new RdmaNet(node_addr_, Env::Instance()->config().recv_threads());
  net_.reset(new RdmaNet(node_addr_, 1));
#else
  // net_ = new ZmqNet(node_addr_, Env::Instance()->config()->recv_threads());
  // net_ = new ServerZmqNet(node_addr_,
  // Env::Instance()->config().recv_threads());
  net_.reset(new ServerZmqNet(node_addr_, 1));
#endif
}

void WorkerService::Start() {
  net_->CreateNetContexts(addresses_);
  cb_.reset(new WSCallBack(this));
  net_->RegisterRecv(cb_.get());
  net_->Start();
}

void WorkerService::Stop() { net_->Stop(); }

void WorkerService::HandleRequest(const void *msg, int size,
                                  const node_id_t &source) {
  // parse the request
  UMessage umsg;
  umsg.ParseFromArray(msg, size);
  // init response
  UMessage response;
  response.set_type(UMessage::RESPONSE);
  response.set_source(umsg.source());
  // execute request
  switch (umsg.type()) {
    case UMessage::PUT_REQUEST:
      HandlePutRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::GET_REQUEST:
      HandleGetRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::MERGE_REQUEST:
      HandleMergeRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::LIST_REQUEST:
      HandleListRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::EXISTS_REQUEST:
      HandleExistsRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::GET_BRANCH_HEAD_REQUEST:
      HandleGetBranchHeadRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::IS_BRANCH_HEAD_REQUEST:
      HandleIsBranchHeadRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::GET_LATEST_VERSION_REQUEST:
      HandleGetLatestVersionRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::IS_LATEST_VERSION_REQUEST:
      HandleIsLatestVersionRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::BRANCH_REQUEST:
      HandleBranchRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::RENAME_REQUEST:
      HandleRenameRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::DELETE_REQUEST:
      HandleDeleteRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::GET_CHUNK_REQUEST:
      HandleGetChunkRequest(umsg, response.mutable_response_payload());
      break;
    default:
      LOG(WARNING) << "Unrecognized request type: " << umsg.type();
      break;
  }
  // send response back
  byte_t *serialized = new byte_t[response.ByteSize()];
  response.SerializeToArray(serialized, response.ByteSize());
  net_->GetNetContext(source)->Send(serialized,
                                    static_cast<size_t>(response.ByteSize()));
  // clean up
  delete[] serialized;
}

Value WorkerService::ValueFromRequest(const ValuePayload& payload) {
  Value val;
  val.type = static_cast<UType>(payload.type());
  val.base = payload.has_base() ? ToHash(payload.base()) : Hash::kNull;
  val.pos = payload.pos();
  val.dels = payload.dels();
  // TODO(anh): a lot of memory copy in the following
  // comment(wangsh): create Slice is cheap, so it should ok
  int vals_size = payload.values_size();
  // even though Slice only take pointer, the pointer
  // to keys and values will persist till the end of HandleRequest
  for (int i = 0; i < vals_size; i++)
    val.vals.push_back(Slice(payload.values(i)));
  int keys_size = payload.keys_size();
  for (int i = 0; i < keys_size; i++)
    val.keys.push_back(Slice(payload.keys(i)));
  return val;
}

void WorkerService::HandlePutRequest(const UMessage& umsg,
                                     ResponsePayload* response) {
  auto request = umsg.request_payload();
  Value value = ValueFromRequest(umsg.value_payload());
  Hash new_version;
  ErrorCode code = request.has_branch()
    ? worker_->Put(Slice(request.key()), value, Slice(request.branch()),
                   &new_version)
    : worker_->Put(Slice(request.key()), value, ToHash(request.version()),
                   &new_version);
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_value(new_version.value(), Hash::kByteLength);
}

void WorkerService::HandleGetRequest(const UMessage& umsg,
                                     ResponsePayload* response) {
  auto request = umsg.request_payload();
  UCell val;
  ErrorCode code = request.has_branch()
    ? worker_->Get(Slice(request.key()), Slice(request.branch()), &val)
    : worker_->Get(Slice(request.key()), ToHash(request.version()), &val);
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_value(val.chunk().head(), val.chunk().numBytes());
}

void WorkerService::HandleMergeRequest(const UMessage& umsg,
                                       ResponsePayload* response) {
  auto request = umsg.request_payload();
  Value value = ValueFromRequest(umsg.value_payload());
  Hash new_version;
  ErrorCode code = request.has_version()
    ? worker_->Merge(Slice(request.key()), value, ToHash(request.version()),
                     ToHash(request.ref_version()), &new_version)
    : (request.has_ref_version()
        ? worker_->Merge(Slice(request.key()), value, Slice(request.branch()),
          ToHash(request.ref_version()), &new_version)
        : worker_->Merge(Slice(request.key()), value, Slice(request.branch()),
          Slice(request.ref_branch()), &new_version));
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_value(new_version.value(), Hash::kByteLength);
}

void WorkerService::HandleListRequest(const UMessage& umsg,
                                      ResponsePayload* response) {
  auto request = umsg.request_payload();
  vector<string> vals;
  ErrorCode code = request.has_key()
    ? worker_->ListBranches(Slice(request.key()), &vals)
    : worker_->ListKeys(&vals);
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  for (auto& v : vals)
    response->add_lvalue(v.data(), v.length());
}

void WorkerService::HandleExistsRequest(const UMessage& umsg,
                                        ResponsePayload* response) {
  auto request = umsg.request_payload();
  bool exists;
  ErrorCode code = request.has_branch()
    ? worker_->Exists(Slice(request.key()), Slice(request.branch()), &exists)
    : worker_->Exists(Slice(request.key()), &exists);
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_bvalue(exists);
}

void WorkerService::HandleGetBranchHeadRequest(const UMessage& umsg,
                                               ResponsePayload* response) {
  auto request = umsg.request_payload();
  Hash version;
  ErrorCode code = worker_->GetBranchHead(Slice(request.key()),
                                          Slice(request.branch()), &version);
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_value(version.value(), Hash::kByteLength);
}

void WorkerService::HandleIsBranchHeadRequest(const UMessage& umsg,
                                              ResponsePayload* response) {
  auto request = umsg.request_payload();
  bool is_head;
  ErrorCode code = worker_->IsBranchHead(Slice(request.key()),
      Slice(request.branch()), ToHash(request.version()), &is_head);
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_bvalue(is_head);
}

void WorkerService::HandleGetLatestVersionRequest(const UMessage& umsg,
                                                  ResponsePayload* response) {
  auto request = umsg.request_payload();
  vector<Hash> versions;
  ErrorCode code = worker_->GetLatestVersions(Slice(request.key()), &versions);
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  for (auto& k : versions)
    response->add_lvalue(k.value(), Hash::kByteLength);
}

void WorkerService::HandleIsLatestVersionRequest(const UMessage& umsg,
                                                 ResponsePayload* response) {
  auto request = umsg.request_payload();
  bool is_latest;
  ErrorCode code = worker_->IsLatestVersion(Slice(request.key()),
      ToHash(request.version()), &is_latest);
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_bvalue(is_latest);
}

void WorkerService::HandleBranchRequest(const UMessage& umsg,
                                        ResponsePayload* response) {
  auto request = umsg.request_payload();
  ErrorCode code = request.has_ref_branch()
    ? worker_->Branch(Slice(request.key()), Slice(request.ref_branch()),
                      Slice(request.branch()))
    : worker_->Branch(Slice(request.key()), ToHash(request.ref_version()),
                      Slice(request.branch()));
  response->set_stat(static_cast<int>(code));
}

void WorkerService::HandleRenameRequest(const UMessage& umsg,
                                        ResponsePayload* response) {
  auto request = umsg.request_payload();
  ErrorCode code = worker_->Rename(Slice(request.key()),
      Slice(request.ref_branch()), Slice(request.branch()));
  response->set_stat(static_cast<int>(code));
}

void WorkerService::HandleDeleteRequest(const UMessage& umsg,
                                        ResponsePayload* response) {
  auto request = umsg.request_payload();
  ErrorCode code = worker_->Delete(Slice(request.key()),
                                   Slice(request.branch()));
  response->set_stat(static_cast<int>(code));
}

void WorkerService::HandleGetChunkRequest(const UMessage& umsg,
                                          ResponsePayload* response) {
  auto request = umsg.request_payload();
  Chunk c;
  ErrorCode code = worker_->GetChunk(Slice(request.key()),
                                     ToHash(request.version()), &c);
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_value(c.head(), c.numBytes());
}

}  // namespace ustore
