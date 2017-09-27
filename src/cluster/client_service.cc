// Copyright (c) 2017 The Ustore Authors

#include "cluster/client_service.h"

#include "utils/env.h"
#include "utils/logging.h"

namespace ustore {

void ClientService::Init() {
  net_.reset(net::CreateClientNetwork(
             Env::Instance()->config().recv_threads()));
  net_->CreateNetContexts(ptt_->destAddrs());
  cb_.reset(RegisterCallBack());
  net_->RegisterRecv(cb_.get());
}

void ClientService::Start() {
  is_running_ = true;
  net_->Start();
}

void ClientService::HandleResponse(const void *msg, int size,
                                   const node_id_t& source) {
  UMessage *ustore_msg = new UMessage();
  ustore_msg->ParseFromArray(msg, size);
  ResponseBlob* res_blob = responses_[ustore_msg->source()].get();

  std::unique_lock<std::mutex> lck(res_blob->lock);
  res_blob->message = ustore_msg;
  res_blob->has_msg = true;
  res_blob->condition.notify_all();
}

void ClientService::Stop() {
  is_running_ = false;
  net_->Stop();
}

ResponseBlob* ClientService::CreateResponseBlob() {
  ResponseBlob* ret = new ResponseBlob();
  responses_.emplace_back(ret);
  for (size_t i = 0; i < responses_.size(); ++i)
    if (responses_[i].get() == ret) {
      ret->id = i;
      ret->net = net_.get();
      return ret;
    }
  LOG(FATAL) << "Failed to find response blob id";
  return nullptr;
}

}  // namespace ustore
