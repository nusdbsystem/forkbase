// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_WORKER_SERVICE_H_
#define USTORE_CLUSTER_WORKER_SERVICE_H_

#include "net/net.h"

namespace ustore {

class Worker;

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

    explicit WorkerService(const node_id_t& master): master_(master) {}
    ~WorkerService();

    // initialize the network, the worker and register callback
    virtual void Init();
    /**
     * Handle requests:
     * 1. It parse msg into a UStoreMessage
     * 2. Invoke the processing logic from Worker.
     * 3. Construct a response and send back to source.
     */
    virtual void HandleRequest(const void *msg, int size,
                               const node_id_t& source);

 private:
    node_id_t master_;  // master node
    Net *net_;
    Worker* worker_;  // where the logic happens
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_WORKER_SERVICE_H_
