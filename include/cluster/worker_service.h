// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_WORKER_SERVICE_H_
#define USTORE_CLUSTER_WORKER_SERVICE_H_

#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "net/net.h"
#include "proto/messages.pb.h"
#include "spec/value.h"
#include "types/ucell.h"
#include "worker/worker.h"

namespace ustore {

/**
 * The WorkerService receives requests from ClientService and invokes
 * the Worker to process the message.
 * Basically a simple version of ClientService. It only has as many
 * processing threads as provided by the Net implementation.
 */
class WorkerService {
 public:
    // Dispatching requests from the network.
    // Basically call this->HandleRequest that invoke Worker methods.
    static void RequestDispatch(const void *msg, int size, void *handler,
                                const node_id_t& source);

    static int range_cmp(const RangeInfo& a, const RangeInfo& b);

    WorkerService(const node_id_t& addr, const node_id_t& master, bool persist)
      : node_addr_(addr), master_(master), persist_(persist) {}
    virtual ~WorkerService() = default;

    // initialize the network, the worker and register callback
    virtual void Init();
    virtual void Start();
    virtual void Stop();

    /**
     * Handle requests:
     * 1. It parse msg into a UStoreMessage
     * 2. Invoke the processing logic from Worker.
     * 3. Construct a response and send back to source.
     */
    virtual void HandleRequest(const void *msg, int size,
                               const node_id_t& source);

 private:
    // helper methods for parsing request/response
    inline Hash ToHash(const std::string& request) {
      return Hash(reinterpret_cast<const byte_t*>(request.data()));
    }
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

    node_id_t master_;  // master node
    node_id_t node_addr_;  // this node's address
    std::vector<RangeInfo> ranges_;  // global knowledge about key ranges
    std::vector<node_id_t> addresses_;  // worker addresses
    std::unique_ptr<Worker> worker_;  // where the logic happens
    std::unique_ptr<Net> net_;
    std::unique_ptr<CallBack> cb_ = nullptr;
    std::mutex lock_;
    bool persist_;
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_WORKER_SERVICE_H_
