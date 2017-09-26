// Copyright (c) 2017 The Ustore Authors.

#include "cluster/client.h"
#include "utils/logging.h"

namespace ustore {

using std::string;
using std::vector;

std::unique_ptr<UMessage> Client::WaitForResponse() const {
  std::unique_lock<std::mutex> lck(res_blob_->lock);
  // res_blob_->has_msg = false;
  while (!(res_blob_->has_msg))
    (res_blob_->condition).wait(lck);
  CHECK(res_blob_->message);
  return std::unique_ptr<UMessage>(
      dynamic_cast<UMessage*>(res_blob_->message));
}

bool Client::Send(UMessage* msg, const node_id_t& node_id) const {
  // set source id
  msg->set_source(id_);
  // serialize and send
  int msg_size = msg->ByteSize();
  byte_t *serialized = new byte_t[msg_size];
  msg->SerializeToArray(serialized, msg_size);
  std::unique_lock<std::mutex> lck(res_blob_->lock);
  CHECK(net_->GetNetContext(node_id));
  net_->GetNetContext(node_id)->Send(serialized, msg_size);
  res_blob_->has_msg = false;
  delete[] serialized;
  return true;
}

ErrorCode Client::GetEmptyResponse() const {
  auto msg = WaitForResponse();
  auto response = msg->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  return err;
}

ErrorCode Client::GetVersionResponse(Hash* version) const {
  auto msg = WaitForResponse();
  auto response = msg->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  if (err == ErrorCode::kOK) {
    *version = Hash::Convert(response.value()).Clone();
  }
  return err;
}

ErrorCode Client::GetUCellResponse(UCell* meta) const {
  auto msg = WaitForResponse();
  auto response = msg->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  if (err == ErrorCode::kOK) {
    // make a copy of the Slice object
    std::unique_ptr<byte_t[]> buf(new byte_t[response.value().length()]);
    std::memcpy(buf.get(), response.value().data(), response.value().length());
    Chunk c(std::move(buf));
    *meta = UCell(std::move(c));
  }
  return err;
}

ErrorCode Client::GetChunkResponse(Chunk* chunk) const {
  auto msg = WaitForResponse();
  auto response = msg->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  if (err == ErrorCode::kOK) {
    // make a copy of the Slice object
    std::unique_ptr<byte_t[]> buf(new byte_t[response.value().length()]);
    std::memcpy(buf.get(), response.value().data(), response.value().length());
    *chunk = Chunk(std::move(buf));
  }
  return err;
}

ErrorCode Client::GetStringListResponse(vector<string>* vals) const {
  auto msg = WaitForResponse();
  auto response = msg->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  if (err == ErrorCode::kOK) {
    size_t size = response.lvalue_size();
    for (size_t i = 0; i < size; i++)
      vals->push_back(response.lvalue(i));
  }
  return err;
}

ErrorCode Client::GetVersionListResponse(vector<Hash>* versions) const {
  auto msg = WaitForResponse();
  auto response = msg->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  if (err == ErrorCode::kOK) {
    size_t size = response.lvalue_size();
    for (size_t i = 0; i < size; i++)
      versions->push_back(Hash::Convert(response.lvalue(i)).Clone());
  }
  return err;
}

ErrorCode Client::GetBoolResponse(bool *value) const {
  auto msg = WaitForResponse();
  auto response = msg->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  if (err == ErrorCode::kOK) {
    *value = response.bvalue();
  }
  return err;
}

ErrorCode Client::GetInfoResponse(std::vector<StoreInfo>* stores) const {
  auto msg = WaitForResponse();
  auto response = msg->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  if (err == ErrorCode::kOK) {
    auto info = msg->info_payload();
    StoreInfo v;
    v.chunks = info.chunks();
    v.chunkBytes = info.chunk_bytes();
    v.validChunks = info.valid_chunks();
    v.validChunkBytes = info.valid_chunk_bytes();
    v.maxSegments = info.max_segments();
    v.allocSegments = info.alloc_segments();
    v.freeSegments = info.free_segments();
    v.usedSegments = info.used_segments();
    size_t size = info.chunk_types_size();
    for (size_t i = 0; i < size; ++i) {
      v.chunksPerType.emplace(static_cast<ChunkType>(info.chunk_types(i)),
                              info.chunks_per_type(i));
      v.bytesPerType.emplace(static_cast<ChunkType>(info.chunk_types(i)),
                             info.bytes_per_type(i));
    }
    // set node id
    v.nodeId = info.node_id();
    stores->push_back(std::move(v));
  }
  return err;
}

}  // namespace ustore
