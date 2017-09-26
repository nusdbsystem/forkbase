// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_CLIENT_SERVICE_H_
#define USTORE_CLUSTER_CLIENT_SERVICE_H_

#include <memory>
#include <vector>
#include "cluster/partitioner.h"
#include "cluster/response_blob.h"
#include "net/net.h"
#include "utils/noncopyable.h"

namespace ustore {

/**
 * ClientService is an abstracted class to handle response from server.
 * A ClientService receives responses from Server and invokes corresponding
 * classes to process the message.
 */
class ClientService : private Noncopyable {
 public:
  explicit ClientService(const Partitioner* ptt)
    : is_running_(false), nclients_(0), ptt_(ptt) {}
  virtual ~ClientService() = default;

  void Init();
  void Start();
  void Stop();

  void HandleResponse(const void *msg, int size, const node_id_t& source);

 protected:
  ResponseBlob* CreateResponseBlob();

  // allocate a net::CallBack instance
  virtual CallBack* RegisterCallBack() = 0;

 private:
  volatile bool is_running_;  // volatile to avoid caching old value
  int nclients_;  // how many RequestHandler thread it uses
  std::vector<std::unique_ptr<ResponseBlob>> responses_;  // the response queue
  std::unique_ptr<CallBack> cb_;
  std::unique_ptr<Net> net_;
  const Partitioner* const ptt_;
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_CLIENT_SERVICE_H_
