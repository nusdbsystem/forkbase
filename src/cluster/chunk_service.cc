// Copyright (c) 2017 The Ustore Authors

#include "cluster/chunk_service.h"
#include "utils/env.h"
namespace ustore {

class CSCallBack : public CallBack {
 public:
  explicit CSCallBack(void *handler) : CallBack(handler) {}
  void operator()(const void *msg, int size, const node_id_t &source) override {
    (reinterpret_cast<ChunkService *>(handler_))
        ->HandleRequest(msg, size, source);
  }
};

void ChunkService::Start() {
  store_ = store::GetChunkStore();

  net_->CreateNetContexts(addresses_);
  cb_.reset(new CSCallBack(this));
  net_->RegisterRecv(cb_.get());
  net_->Start();
}

void ChunkService::HandleRequest(const void *msg, int size,
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
  net_->GetNetContext(source)->Send(serialized,
                                    static_cast<size_t>(response.ByteSize()));
  // clean up
  delete[] serialized;
}

void ChunkService::HandlePutChunkRequest(const UMessage& umsg,
                                     ResponsePayload* response) {
  auto request = umsg.request_payload();
  auto value = umsg.value_payload().base();

  Hash hash = Hash(reinterpret_cast<const byte_t*>(request.key().data())).Clone();
  
  std::unique_ptr<byte_t[]> buf(new byte_t[value.length()]);
  std::memcpy(buf.get(), value.data(), value.length());

  lock_.lock();
  Chunk c(std::move(buf));
  store_->Put(hash, c);
  lock_.unlock();

  response->set_stat(static_cast<int>(ErrorCode::kOK));
  return;
}

void ChunkService::HandleGetChunkRequest(const UMessage& umsg,
                                     ResponsePayload* response) {
  auto request = umsg.request_payload();
  Chunk chunk;

  Hash hash = Hash(reinterpret_cast<const byte_t*>(request.key().data())).Clone();
  if (!store_->Exists(hash)) {
    response->set_stat(static_cast<int>(ErrorCode::kChunkNotExists));
    return;
  }

  lock_.lock();
  chunk = store_->Get(hash);
  lock_.unlock();

  response->set_stat(static_cast<int>(ErrorCode::kOK));
  response->set_value(chunk.head(), chunk.numBytes());
  return;
}

void ChunkService::HandleExistChunkRequest(const UMessage& umsg,
                                     ResponsePayload* response) {
  auto request = umsg.request_payload();

  Hash hash = Hash(reinterpret_cast<const byte_t*>(request.key().data())).Clone();
  response->set_stat(static_cast<int>(ErrorCode::kOK));
  response->set_bvalue(store_->Exists(hash));
  return;
}

}  // namespace ustore
