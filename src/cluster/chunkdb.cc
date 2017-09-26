// Copyright (c) 2017 The Ustore Authors.

#include "cluster/chunkdb.h"
#include "proto/messages.pb.h"
#include "utils/logging.h"

namespace ustore {

void ChunkDb::CreateChunkRequest(const Hash& hash, UMessage *msg) {
  msg->set_source(id_);
  // request
  auto request = msg->mutable_request_payload();
  request->set_key(hash.value(), Hash::kByteLength);
}

ErrorCode ChunkDb::Get(const Hash& hash, Chunk* chunk) {
  // create get request
  UMessage msg;
  msg.set_type(UMessage::GET_CHUNK_REQUEST);
  CreateChunkRequest(hash, &msg);

  Send(msg, ptt_->GetWorkerAddr(hash));

  auto response = (WaitForResponse())->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  if (err == ErrorCode::kOK) {
    std::unique_ptr<byte_t[]> buf(new byte_t[response.value().length()]);
    std::memcpy(buf.get(), response.value().data(), response.value().length());
    *chunk = Chunk(std::move(buf));
  }
  return err;
}

ErrorCode ChunkDb::Put(const Hash& hash, const Chunk& chunk) {
  // create get request
  UMessage msg;
  msg.set_type(UMessage::PUT_CHUNK_REQUEST);
  CreateChunkRequest(hash, &msg);

  auto payload = msg.mutable_value_payload();
  payload->set_base(chunk.head(), chunk.numBytes());

  Send(msg, ptt_->GetWorkerAddr(hash));
  return GetEmptyResponse();
}

ErrorCode ChunkDb::Exists(const Hash& hash, bool* exist) {
  UMessage msg;
  // header
  msg.set_type(UMessage::EXISTS_CHUNK_REQUEST);
  CreateChunkRequest(hash, &msg);

  // send
  Send(msg, ptt_->GetWorkerAddr(hash));
  return GetBoolResponse(exist);
}

}  // namespace ustore
