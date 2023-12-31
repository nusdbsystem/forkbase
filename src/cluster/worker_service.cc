// Copyright (c) 2017 The Ustore Authors

#include "cluster/worker_service.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include "hash/hash.h"
#include "net/net.h"
#include "spec/slice.h"
#include "utils/message_parser.h"
#include "utils/logging.h"

namespace ustore {

class WorkerServiceCallBack : public CallBack {
 public:
  explicit WorkerServiceCallBack(void *handler) : CallBack(handler) {}
  void operator()(const void *msg, int size, const node_id_t& source) override {
    (reinterpret_cast<WorkerService *>(handler_))
        ->HandleRequest(msg, size, source);
  }
};

void WorkerService::Init() {
  CallBack* callback = new WorkerServiceCallBack(this);
  HostService::Init(std::unique_ptr<CallBack>(callback));
  // start chunk service as well
  if (ck_svc_) ck_svc_->Run();
}

void WorkerService::HandleRequest(const void *msg, int size,
                                  const node_id_t &source) {
  // parse the request
  UMessage umsg;
  MessageParser::Parse(msg, size, &umsg);
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
    case UMessage::PUT_UNKEYED_REQUEST:
      HandlePutUnkeyedRequest(umsg, response.mutable_response_payload());
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
  Send(source, serialized, response.ByteSize());
  // clean up
  delete[] serialized;
}

Value WorkerService::ValueFromRequest(const ValuePayload& payload) {
  Value val;
  val.type = static_cast<UType>(payload.type());
  val.base = payload.has_base() ? Hash(payload.base()) : Hash();
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
  val.ctx = Slice(payload.ctx());
  return val;
}

// used for access log
static const std::string empty = "null";

void WorkerService::HandlePutRequest(const UMessage& umsg,
                                     ResponsePayload* response) {
  auto request = umsg.request_payload();
  Value value = ValueFromRequest(umsg.value_payload());
  Hash new_version;
  lock_.lock();
  ErrorCode code = request.has_branch()
    ? worker_.Put(Slice(request.key()), value, Slice(request.branch()),
                  &new_version)
    : worker_.Put(Slice(request.key()), value, Hash(request.version()),
                  &new_version);
  lock_.unlock();
  access_.Append("PUT", request.key(), request.has_branch() ? request.branch()
      : Hash(request.version()).ToBase32(), new_version.ToBase32());
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
    : worker_.Get(Slice(request.key()), Hash(request.version()), &val);
  lock_.unlock();
  access_.Append("GET", request.key(), request.has_branch() ? request.branch()
      : Hash(request.version()).ToBase32(), empty);
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
    ? worker_.Merge(Slice(request.key()), value,
                    Hash(request.version()),
                    Hash(request.ref_version()), &new_version)
    : (request.has_ref_version()
        ? worker_.Merge(Slice(request.key()), value, Slice(request.branch()),
          Hash(request.ref_version()), &new_version)
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
  std::vector<std::string> vals;
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
      Slice(request.branch()), Hash(request.version()), &is_head);
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_bvalue(is_head);
}

void WorkerService::HandleGetLatestVersionRequest(const UMessage& umsg,
                                                  ResponsePayload* response) {
  auto request = umsg.request_payload();
  std::vector<Hash> versions;
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
      Hash(request.version()), &is_latest);
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
    : worker_.Branch(Slice(request.key()), Hash(request.ref_version()),
                     Slice(request.branch()));
  lock_.unlock();
  access_.Append("BRANCH", request.key(), request.has_ref_branch()
      ? request.ref_branch() : Hash(request.ref_version()).ToBase32(),
      request.branch());
  response->set_stat(static_cast<int>(code));
}

void WorkerService::HandleRenameRequest(const UMessage& umsg,
                                        ResponsePayload* response) {
  auto request = umsg.request_payload();
  lock_.lock();
  ErrorCode code = worker_.Rename(Slice(request.key()),
      Slice(request.ref_branch()), Slice(request.branch()));
  lock_.unlock();
  access_.Append("RENAME", request.key(), request.ref_branch(),
      request.branch());
  response->set_stat(static_cast<int>(code));
}

void WorkerService::HandleDeleteRequest(const UMessage& umsg,
                                        ResponsePayload* response) {
  auto request = umsg.request_payload();
  lock_.lock();
  ErrorCode code = worker_.Delete(Slice(request.key()),
                                  Slice(request.branch()));
  lock_.unlock();
  access_.Append("DELETE", request.key(), request.branch(), empty);
  response->set_stat(static_cast<int>(code));
}

void WorkerService::HandlePutUnkeyedRequest(const UMessage& umsg,
                                            ResponsePayload* response) {
  Value value = ValueFromRequest(umsg.value_payload());
  Hash new_version;
  lock_.lock();
  ErrorCode code = worker_.PutUnkeyed(Slice(), value, &new_version);
  lock_.unlock();
  response->set_stat(static_cast<int>(code));
  if (code != ErrorCode::kOK) return;
  response->set_value(new_version.value(), Hash::kByteLength);
}

void WorkerService::HandleGetChunkRequest(const UMessage& umsg,
                                          ResponsePayload* response) {
  auto request = umsg.request_payload();
  Chunk c;
  lock_.lock();
  ErrorCode code = worker_.GetChunk(Slice(), Hash(request.version()), &c);
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
