// Copyright (c) 2017 The Ustore Authors.

#include "cluster/chunk_client.h"
#include "proto/messages.pb.h"
#include "utils/logging.h"

namespace ustore {

void ChunkClient::CreateChunkMessage(const Hash& hash, UMessage *msg) const {
  // request
  auto request = msg->mutable_request_payload();
  request->set_version(hash.value(), Hash::kByteLength);
}

ErrorCode ChunkClient::Get(const Hash& hash, Chunk* chunk) const {
  UMessage msg;
  // header
  msg.set_type(UMessage::GET_CHUNK_REQUEST);
  // request
  CreateChunkMessage(hash, &msg);
  // send
  Send(&msg, ptt_->GetDestAddr(hash));
  return GetChunkResponse(chunk);
}

ErrorCode ChunkClient::Get(const Slice& key, const Hash& hash, Chunk* chunk)
    const {
  UMessage msg;
  // header
  msg.set_type(UMessage::GET_CHUNK_REQUEST);
  // request
  CreateChunkMessage(hash, &msg);
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetChunkResponse(chunk);
}

ErrorCode ChunkClient::Put(const Hash& hash, const Chunk& chunk) {
  UMessage msg;
  // header
  msg.set_type(UMessage::PUT_CHUNK_REQUEST);
  // request
  CreateChunkMessage(hash, &msg);
  auto payload = msg.mutable_value_payload();
  payload->set_base(chunk.head(), chunk.numBytes());
  // send
  Send(&msg, ptt_->GetDestAddr(hash));
  return GetEmptyResponse();
}

ErrorCode ChunkClient::Exists(const Hash& hash, bool* exist) const {
  UMessage msg;
  // header
  msg.set_type(UMessage::EXISTS_CHUNK_REQUEST);
  // request
  CreateChunkMessage(hash, &msg);
  // send
  Send(&msg, ptt_->GetDestAddr(hash));
  return GetBoolResponse(exist);
}

}  // namespace ustore
