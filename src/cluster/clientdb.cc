// Copyright (c) 2017 The Ustore Authors.

#include "cluster/clientdb.h"
#include "proto/messages.pb.h"
#include "utils/logging.h"

namespace ustore {

using std::vector;
using std::string;

std::unique_ptr<UMessage> ClientDb::WaitForResponse() {
  std::unique_lock<std::mutex> lck(res_blob_->lock);
  // res_blob_->has_msg = false;
  while (!(res_blob_->has_msg))
    (res_blob_->condition).wait(lck);
  CHECK(res_blob_->message);
  return std::unique_ptr<UMessage>(
      dynamic_cast<UMessage*>(res_blob_->message));
}

bool ClientDb::Send(const Message& msg, const node_id_t& node_id) {
  // serialize and send
  int msg_size = msg.ByteSize();
  byte_t *serialized = new byte_t[msg_size];
  msg.SerializeToArray(serialized, msg_size);
  std::unique_lock<std::mutex> lck(res_blob_->lock);
  CHECK(net_->GetNetContext(node_id));
  net_->GetNetContext(node_id)->Send(serialized, msg_size);
  res_blob_->has_msg = false;
  delete[] serialized;
  return true;
}

void ClientDb::CreatePutMessage(const Slice &key, const Value &value,
                                UMessage* msg) {
  // header
  msg->set_type(UMessage::PUT_REQUEST);
  msg->set_source(id_);
  // request
  auto request = msg->mutable_request_payload();
  request->set_key(key.data(), key.len());
  auto payload = msg->mutable_value_payload();
  payload->set_type(static_cast<int>(value.type));
  payload->set_base(value.base.value(), Hash::kByteLength);
  payload->set_pos(value.pos);
  payload->set_dels(value.dels);
  for (const Slice& s : value.vals)
    payload->add_values(s.data(), s.len());
  for (const Slice& s : value.keys)
    payload->add_keys(s.data(), s.len());
}

ErrorCode ClientDb::Put(const Slice& key, const Value& value,
                        const Hash& pre_version, Hash* version) {
  UMessage msg;
  CreatePutMessage(key, value, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_version(pre_version.value(), Hash::kByteLength);
  // send
  Send(msg, workers_->GetWorker(key));
  return GetVersionResponse(version);
}

ErrorCode ClientDb::Put(const Slice& key, const Value& value,
                        const Slice& branch, Hash* version) {
  UMessage msg;
  CreatePutMessage(key, value, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_branch(branch.data(), branch.len());
  // send
  Send(msg, workers_->GetWorker(key));
  return GetVersionResponse(version);
}

void ClientDb::CreateGetMessage(const Slice &key, UMessage* msg) {
  // header
  msg->set_type(UMessage::GET_REQUEST);
  msg->set_source(id_);
  // request
  auto request = msg->mutable_request_payload();
  request->set_key(key.data(), key.len());
}

ErrorCode ClientDb::Get(const Slice& key, const Slice& branch, UCell* meta) {
  UMessage msg;
  CreateGetMessage(key, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_branch(branch.data(), branch.len());
  // send
  Send(msg, workers_->GetWorker(key));
  return GetUCellResponse(meta);
}

ErrorCode ClientDb::Get(const Slice& key, const Hash& version, UCell* meta) {
  UMessage msg;
  CreateGetMessage(key, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_version(version.value(), Hash::kByteLength);
  // send
  Send(msg, workers_->GetWorker(key));
  return GetUCellResponse(meta);
}

ErrorCode ClientDb::GetChunk(const Slice& key, const Hash& version,
                             Chunk* chunk) {
  UMessage msg;
  // header
  msg.set_type(UMessage::GET_CHUNK_REQUEST);
  msg.set_source(id_);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_version(version.value(), Hash::kByteLength);
  // send
  Send(msg, workers_->GetWorker(key));
  return GetChunkResponse(chunk);
}

ErrorCode ClientDb::GetStorageInfo(std::vector<StoreInfo>* info) {
  UMessage msg;
  // header
  msg.set_type(UMessage::GET_INFO_REQUEST);
  msg.set_source(id_);
  // go through all workers to retrieve keys
  for (const auto& dest : workers_->GetWorkerIds()) {
    Send(msg, dest);
    ErrorCode err = GetInfoResponse(info);
    if (err != ErrorCode::kOK) return err;
  }
  return ErrorCode::kOK;
}

void ClientDb::CreateBranchMessage(const Slice &key, const Slice &new_branch,
                                   UMessage* msg) {
  // header
  msg->set_type(UMessage::BRANCH_REQUEST);
  msg->set_source(id_);
  // request
  auto request = msg->mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_branch(new_branch.data(), new_branch.len());
}

ErrorCode ClientDb::Branch(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) {
  UMessage msg;
  CreateBranchMessage(key, new_branch, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_ref_branch(old_branch.data(), old_branch.len());
  // send
  Send(msg, workers_->GetWorker(key));
  return GetEmptyResponse();
}

ErrorCode ClientDb::Branch(const Slice& key, const Hash& version,
                           const Slice& new_branch) {
  UMessage msg;
  CreateBranchMessage(key, new_branch, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_ref_version(version.value(), Hash::kByteLength);
  // send
  Send(msg, workers_->GetWorker(key));
  return GetEmptyResponse();
}

ErrorCode ClientDb::Rename(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) {
  UMessage msg;
  // header
  msg.set_type(UMessage::RENAME_REQUEST);
  msg.set_source(id_);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_ref_branch(old_branch.data(), old_branch.len());
  request->set_branch(new_branch.data(), new_branch.len());
  // send
  Send(msg, workers_->GetWorker(key));
  return GetEmptyResponse();
}

void ClientDb::CreateMergeMessage(const Slice &key, const Value &value,
                                  UMessage* msg) {
  // header
  msg->set_type(UMessage::MERGE_REQUEST);
  msg->set_source(id_);
  // request
  auto request = msg->mutable_request_payload();
  request->set_key(key.data(), key.len());
  auto payload = msg->mutable_value_payload();
  payload->set_type(static_cast<int>(value.type));
  payload->set_base(value.base.value(), Hash::kByteLength);
  payload->set_pos(value.pos);
  payload->set_dels(value.dels);
  for (const Slice& s : value.vals)
    payload->add_values(s.data(), s.len());
  for (const Slice& s : value.keys)
    payload->add_keys(s.data(), s.len());
}

ErrorCode ClientDb::Merge(const Slice& key, const Value& value,
                          const Slice& tgt_branch, const Slice& ref_branch,
                          Hash* version) {
  UMessage msg;
  CreateMergeMessage(key, value, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_branch(tgt_branch.data(), tgt_branch.len());
  request->set_ref_branch(ref_branch.data(), ref_branch.len());
  // send
  Send(msg, workers_->GetWorker(key));
  return GetVersionResponse(version);
}

ErrorCode ClientDb::Merge(const Slice& key, const Value& value,
                          const Slice& tgt_branch, const Hash& ref_version,
                          Hash* version) {
  UMessage msg;
  CreateMergeMessage(key, value, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_branch(tgt_branch.data(), tgt_branch.len());
  request->set_ref_version(ref_version.value(), Hash::kByteLength);
  // send
  Send(msg, workers_->GetWorker(key));
  return GetVersionResponse(version);
}

ErrorCode ClientDb::Merge(const Slice& key, const Value& value,
                          const Hash& ref_version1, const Hash& ref_version2,
                          Hash* version) {
  UMessage msg;
  CreateMergeMessage(key, value, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_version(ref_version1.value(), Hash::kByteLength);
  request->set_ref_version(ref_version2.value(), Hash::kByteLength);
  // send
  Send(msg, workers_->GetWorker(key));
  return GetVersionResponse(version);
}

ErrorCode ClientDb::ListKeys(std::vector<std::string>* keys) {
  UMessage msg;
  // header
  msg.set_type(UMessage::LIST_REQUEST);
  msg.set_source(id_);
  // go through all workers to retrieve keys
  for (const auto& dest : workers_->GetWorkerIds()) {
    Send(msg, dest);
    ErrorCode err = GetStringListResponse(keys);
    if (err != ErrorCode::kOK) return err;
  }
  return ErrorCode::kOK;
}

ErrorCode ClientDb::ListBranches(const Slice& key,
                                 std::vector<std::string>* branches) {
  UMessage msg;
  // header
  msg.set_type(UMessage::LIST_REQUEST);
  msg.set_source(id_);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  // send
  Send(msg, workers_->GetWorker(key));
  return GetStringListResponse(branches);
}

ErrorCode ClientDb::Exists(const Slice& key, bool* exist) {
  UMessage msg;
  // header
  msg.set_type(UMessage::EXISTS_REQUEST);
  msg.set_source(id_);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  // send
  Send(msg, workers_->GetWorker(key));
  return GetBoolResponse(exist);
}

ErrorCode ClientDb::Exists(const Slice& key, const Slice& branch, bool* exist) {
  UMessage msg;
  // header
  msg.set_type(UMessage::EXISTS_REQUEST);
  msg.set_source(id_);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  // send
  Send(msg, workers_->GetWorker(key));
  return GetBoolResponse(exist);
}

ErrorCode ClientDb::GetBranchHead(const Slice& key, const Slice& branch,
                                  Hash* version) {
  UMessage msg;
  // header
  msg.set_type(UMessage::GET_BRANCH_HEAD_REQUEST);
  msg.set_source(id_);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  // send
  Send(msg, workers_->GetWorker(key));
  return GetVersionResponse(version);
}

ErrorCode ClientDb::IsBranchHead(const Slice& key, const Slice& branch,
                         const Hash& version, bool* isHead) {
  UMessage msg;
  // header
  msg.set_type(UMessage::IS_BRANCH_HEAD_REQUEST);
  msg.set_source(id_);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  request->set_version(version.value(), Hash::kByteLength);
  // send
  Send(msg, workers_->GetWorker(key));
  return GetBoolResponse(isHead);
}

ErrorCode ClientDb::GetLatestVersions(const Slice& key,
                                      std::vector<Hash>* versions) {
  UMessage msg;
  // header
  msg.set_type(UMessage::GET_LATEST_VERSION_REQUEST);
  msg.set_source(id_);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  // send
  Send(msg, workers_->GetWorker(key));
  return GetVersionListResponse(versions);
}

ErrorCode ClientDb::IsLatestVersion(const Slice& key, const Hash& version,
                            bool* isLatest) {
  UMessage msg;
  // header
  msg.set_type(UMessage::IS_LATEST_VERSION_REQUEST);
  msg.set_source(id_);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_version(version.value(), Hash::kByteLength);
  // send
  Send(msg, workers_->GetWorker(key));
  return GetBoolResponse(isLatest);
}

ErrorCode ClientDb::Delete(const Slice& key, const Slice& branch) {
  UMessage msg;
  // header
  msg.set_type(UMessage::DELETE_REQUEST);
  msg.set_source(id_);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  // send
  Send(msg, workers_->GetWorker(key));
  return GetEmptyResponse();
}

ErrorCode ClientDb::GetEmptyResponse() {
  auto msg = WaitForResponse();
  auto response = msg->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  return err;
}

ErrorCode ClientDb::GetVersionResponse(Hash* version) {
  auto msg = WaitForResponse();
  auto response = msg->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  if (err == ErrorCode::kOK) {
    *version = ToHash(response.value()).Clone();
  }
  return err;
}

ErrorCode ClientDb::GetUCellResponse(UCell* meta) {
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

ErrorCode ClientDb::GetChunkResponse(Chunk* chunk) {
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

ErrorCode ClientDb::GetStringListResponse(vector<string>* vals) {
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

ErrorCode ClientDb::GetVersionListResponse(vector<Hash>* versions) {
  auto msg = WaitForResponse();
  auto response = msg->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  if (err == ErrorCode::kOK) {
    size_t size = response.lvalue_size();
    for (size_t i = 0; i < size; i++)
      versions->push_back(ToHash(response.lvalue(i)).Clone());
  }
  return err;
}

ErrorCode ClientDb::GetBoolResponse(bool *value) {
  auto msg = WaitForResponse();
  auto response = msg->response_payload();
  ErrorCode err = static_cast<ErrorCode>(response.stat());
  if (err == ErrorCode::kOK) {
    *value = response.bvalue();
  }
  return err;
}

ErrorCode ClientDb::GetInfoResponse(std::vector<StoreInfo>* stores) {
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
    v.segments = info.segments();
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

WorkerList::WorkerList(const std::vector<RangeInfo> &workers) {
  Update(workers);
}

bool WorkerList::Update(const std::vector<RangeInfo> &workers) {
  workers_.clear();
  for (const RangeInfo& ri : workers)
    workers_.push_back(ri);
  return true;
}

node_id_t WorkerList::GetWorker(const Slice& key) {
// TODO(wangsh): consider partition based on content later,
//               as this will affect latency
//   string h = Hash::ComputeFrom(key.data(), key.len()).ToBase32();
//   Slice hk = Slice(h.data(), h.length());
//
//   for (const RangeInfo& ri : workers_)
//     if (Slice(ri.start()) > hk) {
//       return ri.address();
//     }
//   return workers_[0].address();
  size_t idx = MurmurHash(key.data(), key.len()) % workers_.size();
  return workers_[idx].address();
}

vector<node_id_t> WorkerList::GetWorkerIds() {
  vector<node_id_t> ids;
  for (auto ri : workers_)
    ids.push_back(ri.address());
  return ids;
}

}  // namespace ustore
