// Copyright (c) 2017 The Ustore Authors.

#include "cluster/worker_client.h"
#include "proto/messages.pb.h"
#include "utils/env.h"
#include "utils/logging.h"

namespace ustore {

using std::vector;
using std::string;

void FillValuePayload(const Value& value, ValuePayload* payload) {
  payload->set_type(static_cast<int>(value.type));
  if (!value.base.empty())
    payload->set_base(value.base.value(), Hash::kByteLength);
  payload->set_pos(value.pos);
  payload->set_dels(value.dels);
  for (const Slice& s : value.vals)
    payload->add_values(s.data(), s.len());
  for (const Slice& s : value.keys)
    payload->add_keys(s.data(), s.len());
  payload->set_ctx(value.ctx.data(), value.ctx.len());
}

void WorkerClient::CreatePutMessage(const Slice &key, const Value &value,
                                    UMessage* msg) const {
  // header
  msg->set_type(UMessage::PUT_REQUEST);
  // request
  auto request = msg->mutable_request_payload();
  request->set_key(key.data(), key.len());
  auto payload = msg->mutable_value_payload();
  FillValuePayload(value, payload);
}

ErrorCode WorkerClient::Put(const Slice& key, const Value& value,
                            const Hash& pre_version, Hash* version) {
  UMessage msg;
  CreatePutMessage(key, value, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_version(pre_version.value(), Hash::kByteLength);
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetVersionResponse(version);
}

ErrorCode WorkerClient::Put(const Slice& key, const Value& value,
                            const Slice& branch, Hash* version) {
  UMessage msg;
  CreatePutMessage(key, value, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_branch(branch.data(), branch.len());
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetVersionResponse(version);
}

//TODO
ErrorCode WorkerClient::Put(const Slice& key, const Value& value,
    std::istream* is, const Hash& pre_version, Hash* version) {
  return SplitAndPut(key, value, is, Slice(), pre_version, version);
}

ErrorCode WorkerClient::Put(const Slice& key, const Value& value,
    std::istream* is, const Slice& branch, Hash* version) {
  return SplitAndPut(key, value, is, branch, Hash(), version);
}

ErrorCode WorkerClient::SplitAndPut(const Slice& key, const Value& value,
                                    std::istream* is, const Slice& branch,
                                    const Hash& pre_version, Hash* version) {
  ErrorCode code;
  Value value_part {
    value.type, value.base.Clone(), value.pos, value.dels,
    {}, value.keys, value.ctx
  };

  // Flag to record the global offset of blob
  size_t done = 0;
  while (!is->eof()) {
    // Set base not empty to imply update operation
    UCell meta;
    // Find the original verion of the branch
    if (done == 0 && !branch.empty()) {
      auto get_code = Get(key, branch, &meta);
      if (get_code == ErrorCode::kOK) {
        *version = meta.hash();
      } else if (!(get_code == ErrorCode::kKeyNotExists ||
                   get_code == ErrorCode::kBranchNotExists)) {
        return get_code;
      }
    } else if (done > 0) {
      // Get base of last modified blob
      auto get_code = Get(key, *version, &meta);
      if (get_code != ErrorCode::kOK) {
        return get_code;
      }
      value_part.base = meta.dataHash().Clone();
    }
    value_part.pos = value.pos + done;
    value_part.vals.clear();
    char buf[max_blob_size_];
    is->read(buf, max_blob_size_);
    size_t transport_len = static_cast<size_t>(is->gcount());

    // Get blob part from original blob
    value_part.vals.push_back(
        Slice(buf, transport_len));
    // Generate and send the request
    UMessage msg;
    CreatePutMessage(key, value_part, &msg);
    auto request = msg.mutable_request_payload();
    if (!branch.empty() && ((done == 0 && version->empty()) || is->eof())) {
      request->set_branch(branch.data(), branch.len());
    } else {
      request->set_version(version->value(), Hash::kByteLength);
    }

    // Send request
    Send(&msg, ptt_->GetDestAddr(key));
    code = GetVersionResponse(version);
    if (code != ErrorCode::kOK) {
      return code;
    }
    // Update copied length
    done += transport_len;
  }
  return code;
}

void WorkerClient::CreateGetMessage(const Slice &key, UMessage* msg) const {
  // header
  msg->set_type(UMessage::GET_REQUEST);
  // request
  auto request = msg->mutable_request_payload();
  request->set_key(key.data(), key.len());
}

ErrorCode WorkerClient::Get(const Slice& key, const Slice& branch, UCell* meta)
    const {
  UMessage msg;
  CreateGetMessage(key, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_branch(branch.data(), branch.len());
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetUCellResponse(meta);
}

ErrorCode WorkerClient::Get(const Slice& key, const Hash& version, UCell* meta)
    const {
  UMessage msg;
  CreateGetMessage(key, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_version(version.value(), Hash::kByteLength);
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetUCellResponse(meta);
}

void WorkerClient::CreateBranchMessage(const Slice &key,
    const Slice &new_branch, UMessage* msg) const {
  // header
  msg->set_type(UMessage::BRANCH_REQUEST);
  // request
  auto request = msg->mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_branch(new_branch.data(), new_branch.len());
}

ErrorCode WorkerClient::Branch(const Slice& key, const Slice& old_branch,
                               const Slice& new_branch) {
  UMessage msg;
  CreateBranchMessage(key, new_branch, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_ref_branch(old_branch.data(), old_branch.len());
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetEmptyResponse();
}

ErrorCode WorkerClient::Branch(const Slice& key, const Hash& version,
                               const Slice& new_branch) {
  UMessage msg;
  CreateBranchMessage(key, new_branch, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_ref_version(version.value(), Hash::kByteLength);
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetEmptyResponse();
}

ErrorCode WorkerClient::Rename(const Slice& key, const Slice& old_branch,
                               const Slice& new_branch) {
  UMessage msg;
  // header
  msg.set_type(UMessage::RENAME_REQUEST);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_ref_branch(old_branch.data(), old_branch.len());
  request->set_branch(new_branch.data(), new_branch.len());
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetEmptyResponse();
}

void WorkerClient::CreateMergeMessage(const Slice &key, const Value &value,
                                      UMessage* msg) const {
  // header
  msg->set_type(UMessage::MERGE_REQUEST);
  // request
  auto request = msg->mutable_request_payload();
  request->set_key(key.data(), key.len());
  auto payload = msg->mutable_value_payload();
  FillValuePayload(value, payload);
}

ErrorCode WorkerClient::Merge(const Slice& key, const Value& value,
                              const Slice& tgt_branch, const Slice& ref_branch,
                              Hash* version) {
  UMessage msg;
  CreateMergeMessage(key, value, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_branch(tgt_branch.data(), tgt_branch.len());
  request->set_ref_branch(ref_branch.data(), ref_branch.len());
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetVersionResponse(version);
}

ErrorCode WorkerClient::Merge(const Slice& key, const Value& value,
                              const Slice& tgt_branch, const Hash& ref_version,
                              Hash* version) {
  UMessage msg;
  CreateMergeMessage(key, value, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_branch(tgt_branch.data(), tgt_branch.len());
  request->set_ref_version(ref_version.value(), Hash::kByteLength);
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetVersionResponse(version);
}

ErrorCode WorkerClient::Merge(const Slice& key, const Value& value,
    const Hash& ref_version1, const Hash& ref_version2, Hash* version) {
  UMessage msg;
  CreateMergeMessage(key, value, &msg);
  // request
  auto request = msg.mutable_request_payload();
  request->set_version(ref_version1.value(), Hash::kByteLength);
  request->set_ref_version(ref_version2.value(), Hash::kByteLength);
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetVersionResponse(version);
}

ErrorCode WorkerClient::ListKeys(std::vector<std::string>* keys) const {
  UMessage msg;
  // header
  msg.set_type(UMessage::LIST_REQUEST);
  // go through all workers to retrieve keys
  for (const auto& dest : ptt_->destAddrs()) {
    Send(&msg, dest);
    ErrorCode err = GetStringListResponse(keys);
    if (err != ErrorCode::kOK) return err;
  }
  return ErrorCode::kOK;
}

ErrorCode WorkerClient::ListBranches(const Slice& key,
                                     std::vector<std::string>* branches) const {
  UMessage msg;
  // header
  msg.set_type(UMessage::LIST_REQUEST);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetStringListResponse(branches);
}

ErrorCode WorkerClient::Exists(const Slice& key, bool* exist) const {
  UMessage msg;
  // header
  msg.set_type(UMessage::EXISTS_REQUEST);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetBoolResponse(exist);
}

ErrorCode WorkerClient::Exists(const Slice& key, const Slice& branch,
    bool* exist) const {
  UMessage msg;
  // header
  msg.set_type(UMessage::EXISTS_REQUEST);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetBoolResponse(exist);
}

ErrorCode WorkerClient::GetBranchHead(const Slice& key, const Slice& branch,
                                      Hash* version) const {
  UMessage msg;
  // header
  msg.set_type(UMessage::GET_BRANCH_HEAD_REQUEST);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetVersionResponse(version);
}

ErrorCode WorkerClient::IsBranchHead(const Slice& key, const Slice& branch,
                                     const Hash& version, bool* isHead) const {
  UMessage msg;
  // header
  msg.set_type(UMessage::IS_BRANCH_HEAD_REQUEST);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  request->set_version(version.value(), Hash::kByteLength);
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetBoolResponse(isHead);
}

ErrorCode WorkerClient::GetLatestVersions(const Slice& key,
                                          std::vector<Hash>* versions) const {
  UMessage msg;
  // header
  msg.set_type(UMessage::GET_LATEST_VERSION_REQUEST);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetVersionListResponse(versions);
}

ErrorCode WorkerClient::IsLatestVersion(const Slice& key, const Hash& version,
                                        bool* isLatest) const {
  UMessage msg;
  // header
  msg.set_type(UMessage::IS_LATEST_VERSION_REQUEST);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_version(version.value(), Hash::kByteLength);
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetBoolResponse(isLatest);
}

ErrorCode WorkerClient::Delete(const Slice& key, const Slice& branch) {
  UMessage msg;
  // header
  msg.set_type(UMessage::DELETE_REQUEST);
  // request
  auto request = msg.mutable_request_payload();
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  // send
  Send(&msg, ptt_->GetDestAddr(key));
  return GetEmptyResponse();
}

ErrorCode WorkerClient::PutUnkeyed(const Slice& route_key, const Value& value,
                                   Hash* version) {
  UMessage msg;
  // header
  msg.set_type(UMessage::PUT_UNKEYED_REQUEST);
  // value
  auto payload = msg.mutable_value_payload();
  FillValuePayload(value, payload);
  // send
  Send(&msg, ptt_->GetDestAddr(route_key));
  return GetVersionResponse(version);
}

ErrorCode WorkerClient::GetChunk(const Slice& route_key, const Hash& version,
                                 Chunk* chunk) const {
  // NOTE(wangsh):
  // With local chunk store - ask worker computed from key
  // With partitioned chunk store - ask worker computed from version
  static bool dist = Env::Instance()->config().enable_dist_store();
  // NOTE(wangsh):
  // Can bypass worker if chunk cli is available
  static bool bypass = ck_cli_.size();

  if (bypass) {  // Get via chunk service
    if (dist) return ck_cli_[0].Get(version, chunk);
    return ck_cli_[0].Get(route_key, version, chunk);
  } else {  // Get via worker service
    UMessage msg;
    // header
    msg.set_type(UMessage::GET_CHUNK_REQUEST);
    // request
    auto request = msg.mutable_request_payload();
    request->set_version(version.value(), Hash::kByteLength);
    // send
    Send(&msg, dist ? ptt_->GetDestAddr(version)
                    : ptt_->GetDestAddr(route_key));
    return GetChunkResponse(chunk);
  }
}

ErrorCode WorkerClient::GetStorageInfo(std::vector<StoreInfo>* info) const {
  UMessage msg;
  // header
  msg.set_type(UMessage::GET_INFO_REQUEST);
  // go through all workers to retrieve keys
  for (const auto& dest : ptt_->destAddrs()) {
    Send(&msg, dest);
    ErrorCode err = GetInfoResponse(info);
    if (err != ErrorCode::kOK) return err;
  }
  return ErrorCode::kOK;
}

}  // namespace ustore
