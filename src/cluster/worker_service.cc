// Copyright (c) 2017 The Ustore Authors

#include "cluster/worker_service.h"

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

// for now, reads configuration from WORKER_FILE and CLIENTSERVICE_FILE
void WorkerService::Init() {
// TODO(zhanghao): define a static function in net.cc to create net instance,
// instead of directly use USE_RDMA flag everywhere
#ifdef USE_RDMA
  // net_ = new RdmaNet(node_addr_, Env::Instance()->config().recv_threads());
  net_.reset(new RdmaNet(node_addr_, Env::Instance()->config().recv_threads()));
#else
  // net_ = new ZmqNet(node_addr_, Env::Instance()->config()->recv_threads());
  // net_ = new ServerZmqNet(node_addr_,
  // Env::Instance()->config().recv_threads());
  net_.reset(new ServerZmqNet(node_addr_,
             Env::Instance()->config().recv_threads()));
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
    case UMessage::GET_INFO_REQUEST:
      HandleGetInfoRequest(umsg, &response);
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
  val.base = payload.has_base() ? ToHash(payload.base()) : Hash();
  val.pos = payload.pos();
  val.dels = payload.dels();
  // TODO(anh): a lot of memory copy in the following
  // comment(wangsh): create Slice is cheap, so it should ok
  int vals_size = payload.values_size();
  // even though Slice only take pointer, the pointer
  // to keys and values will persist till the end of HandleRequest
  for (int i = 0; i < vals_size; i++)
    val.vals.emplace_back(payload.values(i));
  int keys_size = payload.keys_size();
  for (int i = 0; i < keys_size; i++)
    val.keys.emplace_back(payload.keys(i));
  return val;
}

void WorkerService::HandlePutRequest(const UMessage& umsg,
                                     ResponsePayload* response) {
  auto request = umsg.request_payload();
  Value value = ValueFromRequest(umsg.value_payload());
  Hash new_version;
  lock_.lock();
  ErrorCode code = request.has_branch()
    ? worker_.Put(Slice(request.key()), value, Slice(request.branch()),
                   &new_version)
    : worker_.Put(Slice(request.key()), value, ToHash(request.version()),
                   &new_version);
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_value(new_version.value(), Hash::kByteLength);
}

void WorkerService::HandleGetRequest(const UMessage& umsg,
                                     ResponsePayload* response) {
  auto request = umsg.request_payload();
  UCell val;
  lock_.lock();
  ErrorCode code = request.has_branch()
    ? worker_.Get(Slice(request.key()), Slice(request.branch()), &val)
    : worker_.Get(Slice(request.key()), ToHash(request.version()), &val);
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_value(val.chunk().head(), val.chunk().numBytes());
}

void WorkerService::HandleMergeRequest(const UMessage& umsg,
                                       ResponsePayload* response) {
  auto request = umsg.request_payload();
  Value value = ValueFromRequest(umsg.value_payload());
  Hash new_version;
  lock_.lock();
  ErrorCode code = request.has_version()
    ? worker_.Merge(Slice(request.key()), value, ToHash(request.version()),
                     ToHash(request.ref_version()), &new_version)
    : (request.has_ref_version()
        ? worker_.Merge(Slice(request.key()), value, Slice(request.branch()),
          ToHash(request.ref_version()), &new_version)
        : worker_.Merge(Slice(request.key()), value, Slice(request.branch()),
          Slice(request.ref_branch()), &new_version));
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_value(new_version.value(), Hash::kByteLength);
}

void WorkerService::HandleListRequest(const UMessage& umsg,
                                      ResponsePayload* response) {
  auto request = umsg.request_payload();
  vector<string> vals;
  lock_.lock();
  ErrorCode code = request.has_key()
    ? worker_.ListBranches(Slice(request.key()), &vals)
    : worker_.ListKeys(&vals);
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  for (auto& v : vals)
    response->add_lvalue(v.data(), v.length());
}

void WorkerService::HandleExistsRequest(const UMessage& umsg,
                                        ResponsePayload* response) {
  auto request = umsg.request_payload();
  bool exists;
  lock_.lock();
  ErrorCode code = request.has_branch()
    ? worker_.Exists(Slice(request.key()), Slice(request.branch()), &exists)
    : worker_.Exists(Slice(request.key()), &exists);
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_bvalue(exists);
}

void WorkerService::HandleGetBranchHeadRequest(const UMessage& umsg,
                                               ResponsePayload* response) {
  auto request = umsg.request_payload();
  Hash version;
  lock_.lock();
  ErrorCode code = worker_.GetBranchHead(Slice(request.key()),
                                          Slice(request.branch()), &version);
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_value(version.value(), Hash::kByteLength);
}

void WorkerService::HandleIsBranchHeadRequest(const UMessage& umsg,
                                              ResponsePayload* response) {
  auto request = umsg.request_payload();
  bool is_head;
  lock_.lock();
  ErrorCode code = worker_.IsBranchHead(Slice(request.key()),
      Slice(request.branch()), ToHash(request.version()), &is_head);
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_bvalue(is_head);
}

void WorkerService::HandleGetLatestVersionRequest(const UMessage& umsg,
                                                  ResponsePayload* response) {
  auto request = umsg.request_payload();
  vector<Hash> versions;
  lock_.lock();
  ErrorCode code = worker_.GetLatestVersions(Slice(request.key()), &versions);
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  for (auto& k : versions)
    response->add_lvalue(k.value(), Hash::kByteLength);
}

void WorkerService::HandleIsLatestVersionRequest(const UMessage& umsg,
                                                 ResponsePayload* response) {
  auto request = umsg.request_payload();
  bool is_latest;
  lock_.lock();
  ErrorCode code = worker_.IsLatestVersion(Slice(request.key()),
      ToHash(request.version()), &is_latest);
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_bvalue(is_latest);
}

void WorkerService::HandleBranchRequest(const UMessage& umsg,
                                        ResponsePayload* response) {
  auto request = umsg.request_payload();
  lock_.lock();
  ErrorCode code = request.has_ref_branch()
    ? worker_.Branch(Slice(request.key()), Slice(request.ref_branch()),
                      Slice(request.branch()))
    : worker_.Branch(Slice(request.key()), ToHash(request.ref_version()),
                      Slice(request.branch()));
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
}

void WorkerService::HandleRenameRequest(const UMessage& umsg,
                                        ResponsePayload* response) {
  auto request = umsg.request_payload();
  lock_.lock();
  ErrorCode code = worker_.Rename(Slice(request.key()),
      Slice(request.ref_branch()), Slice(request.branch()));
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
}

void WorkerService::HandleDeleteRequest(const UMessage& umsg,
                                        ResponsePayload* response) {
  auto request = umsg.request_payload();
  lock_.lock();
  ErrorCode code = worker_.Delete(Slice(request.key()),
                                   Slice(request.branch()));
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
}

void WorkerService::HandleGetChunkRequest(const UMessage& umsg,
                                          ResponsePayload* response) {
  auto request = umsg.request_payload();
  Chunk c;
  lock_.lock();
  ErrorCode code = worker_.GetChunk(Slice(request.key()),
                                     ToHash(request.version()), &c);
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_value(c.head(), c.numBytes());
}

void WorkerService::HandleGetInfoRequest(const UMessage& umsg,
                                         UMessage* res) {
  auto response = res->mutable_response_payload();
  std::vector<StoreInfo> store;
  lock_.lock();
  ErrorCode code = worker_.GetStorageInfo(&store);
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  auto info = res->mutable_info_payload();
  info->set_chunks(store[0].chunks);
  info->set_chunk_bytes(store[0].chunkBytes);
  info->set_valid_chunks(store[0].validChunks);
  info->set_valid_chunk_bytes(store[0].validChunkBytes);
  info->set_max_segments(store[0].maxSegments);
  info->set_alloc_segments(store[0].allocSegments);
  info->set_free_segments(store[0].freeSegments);
  info->set_used_segments(store[0].usedSegments);
  for (auto& kv : store[0].chunksPerType) {
    info->add_chunk_types(static_cast<int>(kv.first));
    info->add_chunks_per_type(store[0].chunksPerType.at(kv.first));
    info->add_bytes_per_type(store[0].bytesPerType.at(kv.first));
  }
  // add node id
  info->set_node_id(node_addr_);
}

}  // namespace ustore
