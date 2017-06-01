// Copyright (c) 2017 The Ustore Authors.

#include "cluster/clientdb.h"
#include "proto/messages.pb.h"
#include "utils/logging.h"

using std::vector;
using std::string;
namespace ustore {

Message* ClientDb::WaitForResponse() {
  std::unique_lock<std::mutex> lck(res_blob_->lock);
  // res_blob_->has_msg = false;
  while (!(res_blob_->has_msg))
    (res_blob_->condition).wait(lck);
  CHECK(res_blob_->message);
  return res_blob_->message;
}

bool ClientDb::Send(const Message *msg, const node_id_t& node_id) {
  // serialize and send
  int msg_size = msg->ByteSize();
  std::unique_lock<std::mutex> lck(res_blob_->lock);
  byte_t *serialized = new byte_t[msg_size];
  msg->SerializeToArray(serialized, msg_size);
  CHECK(net_->GetNetContext(node_id));
  net_->GetNetContext(node_id)->Send(serialized, msg_size);

  res_blob_->has_msg = false;
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
  ValuePayload *payload =
      request->mutable_put_request_payload()->mutable_value();
  payload->set_type(static_cast<int>(value.type));
  payload->set_base(value.base.value(), Hash::kByteLength);
  payload->set_pos(value.pos);
  payload->set_dels(value.dels);
  for (const Slice& s : value.vals)
    payload->add_values(s.data(), s.len());
  for (const Slice& s : value.keys)
    payload->add_keys(s.data(), s.len());
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
  return GetPutResponse(version);
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
  return GetPutResponse(version);
}

UStoreMessage *ClientDb::CreateGetRequest(const Slice &key) {
  UStoreMessage *request = new UStoreMessage();
  // header
  request->set_type(UStoreMessage::GET_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_source(id_);
  return request;
}

ErrorCode ClientDb::Get(const Slice& key, const Slice& branch, UCell* meta) {
  UStoreMessage *request = CreateGetRequest(key);
  // header
  request->set_branch(branch.data(), branch.len());
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetUCellResponse(meta);
}

ErrorCode ClientDb::Get(const Slice& key, const Hash& version, UCell* meta) {
  UStoreMessage *request = CreateGetRequest(key);
  // header
  request->set_version(version.value(), Hash::kByteLength);
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetUCellResponse(meta);
}

UStoreMessage *ClientDb::CreateGetChunkRequest(const Slice &key) {
  UStoreMessage *request = new UStoreMessage();
  // header
  request->set_type(UStoreMessage::GET_CHUNK_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_source(id_);
  return request;
}


Chunk ClientDb::GetChunk(const Slice& key, const Hash& version) {
  UStoreMessage *request = CreateGetChunkRequest(key);
  // header
  request->set_version(version.value(), Hash::kByteLength);
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;

  return GetChunkResponse();
}

UStoreMessage *ClientDb::CreateBranchRequest(const Slice &key,
    const Slice &new_branch) {
  UStoreMessage *request = new UStoreMessage();
  // header
  request->set_type(UStoreMessage::BRANCH_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_source(id_);
  // payload;
  BranchRequestPayload *payload =
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
  RenameRequestPayload *payload =
                        request.mutable_rename_request_payload();
  payload->set_new_branch(new_branch.data(), new_branch.len());
  // send
  Send(&request, dest);
  return GetEmptyResponse();
}

UStoreMessage *ClientDb::CreateMergeRequest(const Slice &key,
                                            const Value &value) {
  UStoreMessage *request = new UStoreMessage();
  // header
  request->set_type(UStoreMessage::MERGE_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_source(id_);
  MergeRequestPayload *pl = request->mutable_merge_request_payload();
  ValuePayload *payload = pl->mutable_value();
  payload->set_type(static_cast<int>(value.type));
  payload->set_base(value.base.value(), Hash::kByteLength);
  payload->set_pos(value.pos);
  payload->set_dels(value.dels);
  for (const Slice& s : value.vals)
    payload->add_values(s.data(), s.len());
  for (const Slice& s : value.keys)
    payload->add_keys(s.data(), s.len());
  return request;
}

ErrorCode ClientDb::Merge(const Slice& key, const Value& value,
                          const Slice& tgt_branch, const Slice& ref_branch,
                          Hash* version) {
  UStoreMessage *request = CreateMergeRequest(key, value);
  // payload
  MergeRequestPayload *payload = request->mutable_merge_request_payload();
  request->set_branch(tgt_branch.data(), tgt_branch.len());
  payload->set_ref_branch(ref_branch.data(), ref_branch.len());
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetMergeResponse(version);
}

ErrorCode ClientDb::Merge(const Slice& key, const Value& value,
                          const Slice& tgt_branch, const Hash& ref_version,
                          Hash* version) {
  UStoreMessage *request = CreateMergeRequest(key, value);
  // payload
  MergeRequestPayload *payload = request->mutable_merge_request_payload();
  request->set_branch(tgt_branch.data(), tgt_branch.len());
  payload->set_ref_version(ref_version.value(), Hash::kByteLength);
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetMergeResponse(version);
}

ErrorCode ClientDb::Merge(const Slice& key, const Value& value,
                          const Hash& ref_version1, const Hash& ref_version2,
                          Hash* version) {
  UStoreMessage *request = CreateMergeRequest(key, value);
  // payload
  MergeRequestPayload *payload = request->mutable_merge_request_payload();
  request->set_version(ref_version1.value(), Hash::kByteLength);
  payload->set_ref_version(ref_version2.value(), Hash::kByteLength);
  // send
  node_id_t dest = workers_->GetWorker(key);
  Send(request, dest);
  delete request;
  return GetMergeResponse(version);
}

ErrorCode ClientDb::GetEmptyResponse() {
  UStoreMessage *response
    = reinterpret_cast<UStoreMessage *>(WaitForResponse());
  ErrorCode err = static_cast<ErrorCode>(response->status());
  delete response;
  return err;
}

ErrorCode ClientDb::GetPutResponse(Hash* version) {
  UStoreMessage *response
    = reinterpret_cast<UStoreMessage *>(WaitForResponse());
  ErrorCode err = static_cast<ErrorCode>(response->status());
  if (err == ErrorCode::kOK) {
    *version = Hash(reinterpret_cast<const byte_t *>(
               response->put_response_payload().new_version().data())).Clone();
  }
  delete response;
  return err;
}

ErrorCode ClientDb::GetMergeResponse(Hash* version) {
  UStoreMessage *response
    = reinterpret_cast<UStoreMessage *>(WaitForResponse());
  ErrorCode err = static_cast<ErrorCode>(response->status());
  if (err == ErrorCode::kOK) {
    *version = Hash(reinterpret_cast<const byte_t *>(
             response->merge_response_payload().new_version().data())).Clone();
  }
  delete response;
  return err;
}

ErrorCode ClientDb::GetUCellResponse(UCell* meta) {
  UStoreMessage *response
    = reinterpret_cast<UStoreMessage *>(WaitForResponse());
  ErrorCode err = static_cast<ErrorCode>(response->status());
  if (err == ErrorCode::kOK) {
    UCellPayload *tmp = response->mutable_get_response_payload()
                        ->mutable_meta();
    // make a copy of the Slice object
    // comment(wangsh):
    //   SHOULD use unique_ptr instead of raw pointer to avoid memory leak
    std::unique_ptr<byte_t[]> buf(new byte_t[tmp->value().length()]);
    std::memcpy(buf.get(), tmp->value().data(), tmp->value().length());
    Chunk c(std::move(buf));
    *meta = UCell(std::move(c));
  }
  delete response;
  return err;
}

Chunk ClientDb::GetChunkResponse() {
  UStoreMessage *response
    = reinterpret_cast<UStoreMessage *>(WaitForResponse());
  // const std::string &tmp = response->get_response_payload().value();
  UCellPayload *tmp = response->mutable_get_response_payload()
                      ->mutable_meta();
  // make a copy of the Slice object
  // comment(wangsh):
  //   SHOULD use unique_ptr instead of raw pointer to avoid memory leak
  std::unique_ptr<byte_t[]> buf(new byte_t[tmp->value().length()]);
  std::memcpy(buf.get(), tmp->value().data(), tmp->value().length());
  delete response;
  return Chunk(std::move(buf));
}

ErrorCode ClientDb::GetStringList(vector<string> *vals) {
  UStoreMessage *response
    = reinterpret_cast<UStoreMessage *>(WaitForResponse());
  ErrorCode err = static_cast<ErrorCode>(response->status());
  if (err == ErrorCode::kOK) {
    MultiVersionResponsePayload *payload =
      response->mutable_multi_version_response_payload();
    for (int i = 0; i < payload->versions_size(); i++)
      vals->push_back(string(payload->versions(i).data(),
                      payload->versions(i).length()));
  }
  delete response;
  return err;
}

ErrorCode ClientDb::GetVersionList(vector<Hash> *versions) {
  UStoreMessage *response
    = reinterpret_cast<UStoreMessage *>(WaitForResponse());
  ErrorCode err = static_cast<ErrorCode>(response->status());
  if (err == ErrorCode::kOK) {
    MultiVersionResponsePayload *payload =
      response->mutable_multi_version_response_payload();
    for (int i=0; i < payload->versions_size(); i++)
      versions->push_back(Hash(reinterpret_cast<const byte_t *>(
                          payload->versions(i).data())).Clone());
  }
  delete response;
  return err;
}

ErrorCode ClientDb::GetBranchHeadVersion(Hash *version) {
  UStoreMessage *response
    = reinterpret_cast<UStoreMessage *>(WaitForResponse());
  ErrorCode err = static_cast<ErrorCode>(response->status());
  if (err == ErrorCode::kOK) {
    BranchVersionResponsePayload *payload =
      response->mutable_branch_version_response_payload();
    *version = Hash(reinterpret_cast<const byte_t *>(
                    payload->version().data())).Clone();
  }
  delete response;
  return err;
}

ErrorCode ClientDb::GetBool(bool *value) {
  UStoreMessage *response
    = reinterpret_cast<UStoreMessage *>(WaitForResponse());
  ErrorCode err = static_cast<ErrorCode>(response->status());
  if (err == ErrorCode::kOK) {
    BoolResponsePayload *payload = response->mutable_bool_response_payload();
    *value = payload->value();
  }
  delete response;
  return err;
}

ErrorCode ClientDb::ListKeys(std::vector<std::string>* keys) {
  UStoreMessage *request = new UStoreMessage();
  request->set_type(UStoreMessage::LIST_KEY_REQUEST);
  request->set_source(id_);
  ErrorCode err = ErrorCode::kOK;
  // go through all workers to retrieve keys
  for (node_id_t dest : workers_->GetWorkerIds()) {
    Send(request, dest);
    err = GetStringList(keys);
    if (err != ErrorCode::kOK) {
      delete request;
      return err;
    }
  }
  delete request;
  return err;
}

ErrorCode ClientDb::ListBranches(const Slice& key,
                                 std::vector<std::string>* branches) {
  UStoreMessage *request = new UStoreMessage();
  request->set_type(UStoreMessage::LIST_BRANCH_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_source(id_);
  Send(request, workers_->GetWorker(key));
  delete request;
  return GetStringList(branches);
}

ErrorCode ClientDb::Exists(const Slice& key, bool* exist) {
  UStoreMessage *request = new UStoreMessage();
  request->set_type(UStoreMessage::EXISTS_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_source(id_);
  Send(request, workers_->GetWorker(key));
  delete request;
  return GetBool(exist);
}

ErrorCode ClientDb::Exists(const Slice& key, const Slice& branch, bool* exist) {
  UStoreMessage *request = new UStoreMessage();
  request->set_type(UStoreMessage::EXISTS_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  request->set_source(id_);
  Send(request, workers_->GetWorker(key));
  delete request;
  return GetBool(exist);
}

ErrorCode ClientDb::GetBranchHead(const Slice& key, const Slice& branch,
                                  Hash* version) {
  UStoreMessage *request = new UStoreMessage();
  request->set_type(UStoreMessage::GET_BRANCH_HEAD_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  request->set_source(id_);
  Send(request, workers_->GetWorker(key));
  delete request;
  return GetBranchHeadVersion(version);
}

ErrorCode ClientDb::IsBranchHead(const Slice& key, const Slice& branch,
                         const Hash& version, bool* isHead) {
  UStoreMessage *request = new UStoreMessage();
  request->set_type(UStoreMessage::IS_BRANCH_HEAD_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  request->set_version(version.value(), Hash::kByteLength);
  request->set_source(id_);
  Send(request, workers_->GetWorker(key));
  delete request;
  return GetBool(isHead);
}

ErrorCode ClientDb::GetLatestVersions(const Slice& key,
                                      std::vector<Hash>* versions) {
  UStoreMessage *request = new UStoreMessage();
  request->set_type(UStoreMessage::GET_LATEST_VERSION_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_source(id_);
  Send(request, workers_->GetWorker(key));
  delete request;
  return GetVersionList(versions);
}

ErrorCode ClientDb::IsLatestVersion(const Slice& key, const Hash& version,
                            bool* isLatest) {
  UStoreMessage *request = new UStoreMessage();
  request->set_type(UStoreMessage::IS_LATEST_VERSION_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_version(version.value(), Hash::kByteLength);
  request->set_source(id_);
  Send(request, workers_->GetWorker(key));
  delete request;
  return GetBool(isLatest);
}

ErrorCode ClientDb::Delete(const Slice& key, const Slice& branch) {
  UStoreMessage *request = new UStoreMessage();
  request->set_type(UStoreMessage::DELETE_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  request->set_source(id_);
  Send(request, workers_->GetWorker(key));
  delete request;
  return GetEmptyResponse();
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

vector<node_id_t> WorkerList::GetWorkerIds() {
  vector<node_id_t> ids;
  for (auto ri : workers_)
    ids.push_back(ri.address());
  return ids;
}

ClientDb::~ClientDb() {
  // zhanghao: bug here? RemoteClientService destructor already free these data
//  if (net_)
//    delete net_;
//  if (workers_)
//    delete workers_;
  if (res_blob_)
    delete res_blob_;
}
}  // namespace ustore
