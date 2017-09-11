// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_MASTER_SERVICE_H_
#define USTORE_CLUSTER_MASTER_SERVICE_H_

#include <string>
#include "cluster/worker_service.h"
#include "net/net.h"

namespace ustore {

using std::string;
class Master;

/**
 * The MasterService receives requests from ClientService about key
 * range changes (RangeInfo). It then invokes Master to return
 * the ranges.
 * Basically a simplified version of WorkerService.
 */
class MasterService: public WorkerService {
 public:
    // Dispatch requests (RangeInfo, etc.) from the network.
    // Basically call this->HandleRequest that invoke Master methods.
    static void ResponseDispatch(const void *msg, int size, void *handler,
                                 const node_id_t& source);

    explicit MasterService(const node_id_t& id, const string& config_path)
        : WorkerService(id), config_path_(config_path) {}
    ~MasterService();

    // initialize the network, the worker and register callback
    void Init();
    /**
     * Handle requests:
     * 1. It parse msg into a message (RangeRequest, e.g)
     * 2. Invoke the processing logic from Master.
     * 3. Construct a response (RangeResponse, e.g.) and send back.
     */
    void HandleRequest(const void *msg, int size, const node_id_t& source);

 private:
    string config_path_;  // the master may read from a global config file
    Master* master_;  // where the logic happens
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_MASTER_SERVICE_H_
