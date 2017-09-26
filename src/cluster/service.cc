// Copyright (c) 2017 The Ustore Authors

#include "cluster/service.h"
#include "utils/env.h"

namespace ustore {

void Service::Start() {
  net_.reset(net::CreateServerNetwork(node_addr_,
             Env::Instance()->config().recv_threads()));
  cb_.reset(RegisterCallBack());
  net_->RegisterRecv(cb_.get());
  net_->Start();
}

void Service::Stop() {
  net_->Stop();
}

}  // namespace ustore
