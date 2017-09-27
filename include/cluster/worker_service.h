// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_WORKER_SERVICE_H_
#define USTORE_CLUSTER_WORKER_SERVICE_H_

#include <mutex>
#include "cluster/partitioner.h"
#include "cluster/service.h"
#include "proto/messages.pb.h"
#include "utils/env.h"
#include "worker/worker.h"

namespace ustore {

/**
 * The WorkerService receives requests from ClientService and invokes
 * the Worker to process the message.
 */
class WorkerService : public Service {
 public:
  WorkerService(const node_id_t& addr, bool persist)
    : Service(addr, false), ptt_(Env::Instance()->config().worker_file(), addr),
      // TODO(wangsh): pass real partitioner to worker when partitioned
      worker_(ptt_.id(), nullptr, persist) {}
  ~WorkerService() = default;

  void HandleRequest(const void *msg, int size, const node_id_t& source)
    override;

 protected:
  CallBack* RegisterCallBack() override;

 private:
  Value ValueFromRequest(const ValuePayload& payload);
  void HandlePutRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleGetRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleMergeRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleListRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleExistsRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleGetBranchHeadRequest(const UMessage& umsg,
                                  ResponsePayload* response);
  void HandleIsBranchHeadRequest(const UMessage& umsg,
                                 ResponsePayload* response);
  void HandleGetLatestVersionRequest(const UMessage& umsg,
                                     ResponsePayload* response);
  void HandleIsLatestVersionRequest(const UMessage& umsg,
                                    ResponsePayload* response);
  void HandleBranchRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleRenameRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleDeleteRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleGetChunkRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleGetInfoRequest(const UMessage& umsg, UMessage* response);

  const ChunkPartitioner ptt_;
  Worker worker_;  // where the logic happens
  std::mutex lock_;
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_WORKER_SERVICE_H_
