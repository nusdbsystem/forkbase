// Copyright (c) 2017 The Ustore Authors

#include "cluster/chunk_service.h"
#include "utils/env.h"
namespace ustore {

class ChunkServiceCallBack : public CallBack {
 public:
  explicit ChunkServiceCallBack(void *handler) : CallBack(handler) {}
  void operator()(const void *msg, int size, const node_id_t &source) override {
    (reinterpret_cast<ChunkService *>(handler_))
        ->HandleRequest(msg, size, source);
  }
};

void ChunkService::Init() {
  CallBack* callback = new ChunkServiceCallBack(this);
  HostService::Init(std::unique_ptr<CallBack>(callback));
}

void ChunkService::HandleRequest(const void *msg, int size,
                                 const node_id_t &source) {
  UMessage umsg;
  umsg.ParseFromArray(msg, size);
  // init response
  UMessage response;
  response.set_type(UMessage::RESPONSE);
  response.set_source(umsg.source());
  // execute request
  switch (umsg.type()) {
    case UMessage::PUT_CHUNK_REQUEST:
      HandlePutChunkRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::GET_CHUNK_REQUEST:
      HandleGetChunkRequest(umsg, response.mutable_response_payload());
      break;
    case UMessage::EXISTS_CHUNK_REQUEST:
      HandleExistChunkRequest(umsg, response.mutable_response_payload());
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

void ChunkService::HandlePutChunkRequest(const UMessage& umsg,
                                         ResponsePayload* response) {
  auto request = umsg.request_payload();
  auto value = umsg.value_payload().base();

  Hash hash = Hash::Convert(request.version());
  Chunk c(reinterpret_cast<const byte_t*>(value.data()));
  if (store_->Put(hash, c))
    response->set_stat(static_cast<int>(ErrorCode::kOK));
  else
    response->set_stat(static_cast<int>(ErrorCode::kFailedCreateChunk));
}

void ChunkService::HandleGetChunkRequest(const UMessage& umsg,
                                         ResponsePayload* response) {
  auto request = umsg.request_payload();
  Hash hash = Hash::Convert(request.version());
  Chunk c = store_->Get(hash);
  if (c.empty()) {
    response->set_stat(static_cast<int>(ErrorCode::kChunkNotExists));
  } else {
    response->set_stat(static_cast<int>(ErrorCode::kOK));
    response->set_value(c.head(), c.numBytes());
  }
}

void ChunkService::HandleExistChunkRequest(const UMessage& umsg,
                                           ResponsePayload* response) {
  auto request = umsg.request_payload();
  Hash hash = Hash::Convert(request.version());
  response->set_stat(static_cast<int>(ErrorCode::kOK));
  response->set_bvalue(store_->Exists(hash));
}

}  // namespace ustore
