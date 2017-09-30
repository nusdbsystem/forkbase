// Copyright (c) 2017 The Ustore Authors

#include "cluster/chunk_client_service.h"

namespace ustore {

class ChunkClientServiceCallBack : public CallBack {
 public:
  explicit ChunkClientServiceCallBack(void* handler) : CallBack(handler) {}
  void operator()(const void *msg, int size, const node_id_t& source) override {
    (reinterpret_cast<ChunkClientService*>(handler_))->HandleResponse(
                                        msg, size, source);
  }
};

void ChunkClientService::Init() {
  CallBack* callback = new ChunkClientServiceCallBack(this);
  ClientService::Init(std::unique_ptr<CallBack>(callback));
}

ChunkClient ChunkClientService::CreateChunkClient() {
  // adding a new response blob
  ResponseBlob* resblob = CreateResponseBlob();
  return ChunkClient(resblob, &ptt_);
}

}  // namespace ustore
