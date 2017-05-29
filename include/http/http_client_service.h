// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_USTORE_HTTP_HTTP_CLIENT_SERVICE_H_
#define USTORE_USTORE_HTTP_HTTP_CLIENT_SERVICE_H_

#include <unistd.h>
#include <thread>
#include "spec/object_db.h"
#include "cluster/remote_client_service.h"
#include "cluster/clientdb.h"
#include "utils/singleton.h"

using std::thread;

namespace ustore {

class HttpClientService: public Singleton<HttpClientService> {
 public:
  HttpClientService() {
    // launch clients
    service_ = new RemoteClientService("");
    service_->Init();
    ct_ = new thread(&RemoteClientService::Start, service_);
    usleep(1000000);

    client_ = service_->CreateClientDb();
    db_ = new ObjectDB(client_);
  }

  ~HttpClientService() {
    service_->Stop();
    ct_->join();
    delete service_;
    usleep(1000000);
    delete client_;
    delete db_;
  }

  inline ClientDb* GetClientDb() {
    return client_;
  }

  inline ObjectDB* GetDB() {
    return db_;
  }

 private:
  RemoteClientService *service_;
  thread* ct_;
  ClientDb* client_;
  ObjectDB* db_;
};

}  // namespace ustore

#endif  // USTORE_USTORE_HTTP_HTTP_CLIENT_SERVICE_H_
