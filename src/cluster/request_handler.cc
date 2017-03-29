// Copyright (c) 2017 The Ustore Authors.
#include <mutex>
#include "cluster/request_handler.h"
#include "proto/messages.pb.h"
#include "utils/logging.h"
using std::unique_lock;
namespace ustore {

Message* RequestHandler::WaitForResponse() {
  unique_lock<mutex> lck(res_blob_->lock);
  res_blob_->has_msg = false;
  while (!(res_blob_->has_msg))
    (res_blob_->condition).wait(lck);
  CHECK(res_blob_->message);
  return res_blob_->message;
}

bool RequestHandler::Send(const Message *msg, const node_id_t& node_id) {
  // serialize and send
  int msg_size = msg->ByteSize();
  byte_t *serialized = new byte_t[msg_size];
  msg->SerializeToArray(serialized, msg_size);
  CHECK(net_->GetNetContext(node_id));
  net_->GetNetContext(node_id)->Send(serialized, msg_size);
  delete[] serialized;
  return true;
}

Message* RequestHandler::Put(const Slice &key, const Slice &value,
                  const Slice &branch, const Hash &version,
                  bool forward, bool force) {
  // find worker
  node_id_t dest = workers_->get_worker(key);
  // use the heap, since the message can be big
  UStoreMessage *request = new UStoreMessage();
  // header
  request->set_type(UStoreMessage::PUT_REQUEST);
  request->set_key(key.data(), key.len());
  request->set_branch(branch.data(), branch.len());
  request->set_version(version.value(), Hash::kByteLength);
  request->set_source(id_);

  // payload;
  UStoreMessage::PutRequestPayload *payload =
      request->mutable_put_request_payload();
  payload->set_value(value.data(), value.len());
  if (forward)
    payload->set_forward(true);
  if (force)
    payload->set_force(true);

  // send
  Send(request, dest);
  delete request;
  return WaitForResponse();
}

Message* RequestHandler::Get(const Slice &key, const Slice &branch,
                                              const Hash &version) {
  // find worker
  node_id_t dest = workers_->get_worker(key);
  UStoreMessage request;
  // header
  request.set_type(UStoreMessage::GET_REQUEST);
  request.set_key(key.data(), key.len());
  request.set_branch(branch.data(), branch.len());
  request.set_version(version.value(), Hash::kByteLength);
  request.set_source(id_);

  // send
  Send(&request, dest);
  return WaitForResponse();
}

Message* RequestHandler::Branch(const Slice &key, const Slice &old_branch,
                  const Hash &version, const Slice &new_branch) {
  // find worker
  node_id_t dest = workers_->get_worker(key);
  UStoreMessage request;
  // header
  request.set_type(UStoreMessage::BRANCH_REQUEST);
  request.set_key(key.data(), key.len());
  request.set_branch(old_branch.data(), old_branch.len());
  request.set_version(version.value(), Hash::kByteLength);
  request.set_source(id_);

  // payload;
  UStoreMessage::BranchRequestPayload *payload =
                      request.mutable_branch_request_payload();
  payload->set_new_branch(new_branch.data(), new_branch.len());

  // send
  Send(&request, dest);
  return WaitForResponse();
}

Message* RequestHandler::Move(const Slice &key, const Slice &old_branch,
                const Slice &new_branch) {
  // find worker
  node_id_t dest = workers_->get_worker(key);
  UStoreMessage request;
  // header
  request.set_type(UStoreMessage::MOVE_REQUEST);
  request.set_key(key.data(), key.len());
  request.set_branch(old_branch.data(), old_branch.len());
  request.set_source(id_);

  // payload;
  UStoreMessage::MoveRequestPayload *payload =
                        request.mutable_move_request_payload();
  payload->set_new_branch(new_branch.data(), new_branch.len());

  // send
  Send(&request, dest);
  return WaitForResponse();
}
Message* RequestHandler::Merge(const Slice &key, const Slice &value,
                 const Slice &target_branch, const Slice &ref_branch,
                 const Hash &ref_version, bool forward,
                 bool force) {
  // find worker
  node_id_t dest = workers_->get_worker(key);
  UStoreMessage request;
  // header
  request.set_type(UStoreMessage::MERGE_REQUEST);
  request.set_key(key.data(), key.len());
  request.set_version(ref_version.value(), Hash::kByteLength);
  request.set_source(id_);

  // payload;
  UStoreMessage::MergeRequestPayload *payload =
                                request.mutable_merge_request_payload();
  payload->set_target_branch(target_branch.data(), target_branch.len());
  payload->set_ref_branch(ref_branch.data(), ref_branch.len());
  payload->set_value(value.data(), value.len());
  if (forward)
    payload->set_forward(true);
  if (force)
    payload->set_force(true);
  // send
  Send(&request, dest);
  return WaitForResponse();
}


WorkerList::WorkerList(const vector<RangeInfo> &workers) {
  Update(workers);
}

bool WorkerList::Update(const vector<RangeInfo> &workers) {
  workers_.clear();
  for (RangeInfo ri : workers)
    workers_.push_back(ri);
  return true;
}

node_id_t WorkerList::get_worker(const Slice& key) {
  for (RangeInfo ri : workers_)
    if (Slice(ri.start()) > key)
      return ri.address();

  return workers_[0].address();
}

RequestHandler::~RequestHandler() {
  if (net_)
    delete net_;
  if (workers_)
    delete workers_;
  if (res_blob_)
    delete res_blob_;
}

// do nothing for now
bool RandomWorkload::NextRequest(RequestHandler *reqhl) {
  return true;
}
}  // namespace ustore
