// Copyright (c) 2017 The Ustore Authors.

#include "cluster/clientdb.h"
#include "proto/messages.pb.h"
#include "utils/logging.h"

namespace ustore {

Message* ClientDb::WaitForResponse() {
  std::unique_lock<std::mutex> lck(res_blob_->lock);
  res_blob_->has_msg = false;
  while (!(res_blob_->has_msg))
    (res_blob_->condition).wait(lck);
  CHECK(res_blob_->message);
  return res_blob_->message;
}

bool ClientDb::Send(const Message *msg, const node_id_t& node_id) {
  // serialize and send
  int msg_size = msg->ByteSize();
  byte_t *serialized = new byte_t[msg_size];
  msg->SerializeToArray(serialized, msg_size);
  CHECK(net_->GetNetContext(node_id));
  net_->GetNetContext(node_id)->Send(serialized, msg_size);
  delete[] serialized;
  return true;
}

UStoreMessage *ClientDb::CreatePutRequest(const Slice &key,
                                      const Value &value) {
  UStoreMessage *request = new UStoreMessage();
  // header
  request->set_type(UStoreMessage::PUT_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_source(id_);

  // payload;
  UStoreMessage::PutRequestPayload *payload =
      request->mutable_put_request_payload();
  payload->set_value(value.blob().data(), value.blob().size());
  return request;
}

ErrorCode ClientDb::Put(const Slice& key, const Value& value,
                        const Hash& pre_version, Hash* version) {
  // use the heap, since the message can be big
  UStoreMessage *request = CreatePutRequest(key, value);
  // header
  request->set_version(pre_version.value(), Hash::kByteLength);
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetVersionResponse(version);
}

ErrorCode ClientDb::Put(const Slice& key, const Value& value,
                        const Slice& branch, Hash* version) {
  // use the heap, since the message can be big
  UStoreMessage *request = CreatePutRequest(key, value);
  // header
  request->set_branch(branch.data(), branch.len());
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetVersionResponse(version);
}

UStoreMessage *ClientDb::CreateGetRequest(const Slice &key) {
  UStoreMessage *request = new UStoreMessage();
  // header
  request->set_type(UStoreMessage::GET_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_source(id_);
  return request;
}

ErrorCode ClientDb::Get(const Slice& key, const Slice& branch,
                        Value* value) {
  UStoreMessage *request = CreateGetRequest(key);
  // header
  request->set_branch(branch.data(), branch.len());
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetValueResponse(value);
}

ErrorCode ClientDb::Get(const Slice& key, const Hash& version,
                        Value* value) {
  UStoreMessage *request = CreateGetRequest(key);
  // header
  request->set_version(version.value(), Hash::kByteLength);
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetValueResponse(value);
}

UStoreMessage *ClientDb::CreateBranchRequest(const Slice &key,
    const Slice &new_branch) {
  UStoreMessage *request = new UStoreMessage();
  // header
  request->set_type(UStoreMessage::BRANCH_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_source(id_);
  // payload;
  UStoreMessage::BranchRequestPayload *payload =
                      request->mutable_branch_request_payload();
  payload->set_new_branch(new_branch.data(), new_branch.len());
  return request;
}

ErrorCode ClientDb::Branch(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) {
  UStoreMessage *request = CreateBranchRequest(key, new_branch);
  request->set_branch(old_branch.data(), old_branch.len());
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetEmptyResponse();
}

ErrorCode ClientDb::Branch(const Slice& key, const Hash& version,
                           const Slice& new_branch) {
  UStoreMessage *request = CreateBranchRequest(key, new_branch);
  request->set_version(version.value(), Hash::kByteLength);
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetEmptyResponse();
}

ErrorCode ClientDb::Rename(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) {
  // find worker
  node_id_t dest = workers_->GetWorker(key);
  UStoreMessage request;
  // header
  request.set_type(UStoreMessage::RENAME_REQUEST);
  request.set_key(key.data(), key.len());
  request.set_branch(old_branch.data(), old_branch.len());
  request.set_source(id_);

  // payload;
  UStoreMessage::RenameRequestPayload *payload =
                        request.mutable_rename_request_payload();
  payload->set_new_branch(new_branch.data(), new_branch.len());

  // send
  Send(&request, dest);
  return GetEmptyResponse();
}

UStoreMessage *ClientDb::CreateMergeRequest(const Slice &key,
    const Value &value, const Slice &target_branch) {
  UStoreMessage *request = new UStoreMessage();
  // header
  request->set_type(UStoreMessage::MERGE_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_source(id_);

  UStoreMessage::MergeRequestPayload *payload =
    request->mutable_merge_request_payload();
  payload->set_target_branch(target_branch.data(), target_branch.len());
  payload->set_value(value.blob().data(), value.blob().size());
  return request;
}


ErrorCode ClientDb::Merge(const Slice& key, const Value& value,
                          const Slice& tgt_branch, const Slice& ref_branch,
                          Hash* version) {
  UStoreMessage *request = CreateMergeRequest(key, value, tgt_branch);
  // payload;
  UStoreMessage::MergeRequestPayload *payload =
                                request->mutable_merge_request_payload();
  payload->set_ref_branch(ref_branch.data(), ref_branch.len());
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetVersionResponse(version);
}

ErrorCode ClientDb::Merge(const Slice& key, const Value& value,
                          const Slice& tgt_branch, const Hash& ref_version,
                          Hash* version) {
  UStoreMessage *request = CreateMergeRequest(key, value, tgt_branch);
  request->set_version(ref_version.value(), Hash::kByteLength);

  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetVersionResponse(version);
}

ErrorCode ClientDb::Merge(const Slice& key, const Value& value,
                          const Hash& ref_version1, const Hash& ref_version2,
                          Hash* version) {
  UStoreMessage *request = new UStoreMessage();
  // header
  request->set_type(UStoreMessage::MERGE_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_source(id_);

  UStoreMessage::MergeRequestPayload *payload =
    request->mutable_merge_request_payload();
  payload->set_value(value.blob().data(), value.blob().size());

  request->set_version(ref_version1.value(), Hash::kByteLength);
  payload->set_ref_version(ref_version2.value(), Hash::kByteLength);

  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetVersionResponse(version);
}

ErrorCode ClientDb::GetEmptyResponse() {
  UStoreMessage *response
    = reinterpret_cast<UStoreMessage *>(WaitForResponse());
  ErrorCode err = static_cast<ErrorCode>(response->status());
  delete response;
  return err;
}

ErrorCode ClientDb::GetVersionResponse(Hash* version) {
  UStoreMessage *response
    = reinterpret_cast<UStoreMessage *>(WaitForResponse());
  *version = Hash(reinterpret_cast<const byte_t *>(
    response->put_response_payload().new_version().data())).Clone();
  ErrorCode err = static_cast<ErrorCode>(response->status());
  delete response;
  return err;
}

ErrorCode ClientDb::GetValueResponse(Value* value) {
  UStoreMessage *response
    = reinterpret_cast<UStoreMessage *>(WaitForResponse());
  std::string *tmp = new string(response->get_response_payload().value());
  *value = Value(Blob(reinterpret_cast<const byte_t *>(
    tmp->data()), response->get_response_payload().value().length()));
  ErrorCode err = static_cast<ErrorCode>(response->status());
  delete response;
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
  for (const RangeInfo& ri : workers_)
    if (Slice(ri.start()) > key)
      return ri.address();

  return workers_[0].address();
}

ClientDb::~ClientDb() {
  if (net_)
    delete net_;
  if (workers_)
    delete workers_;
  if (res_blob_)
    delete res_blob_;
}
}  // namespace ustore
