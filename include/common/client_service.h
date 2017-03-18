// Copyright (c) 2017 The Ustore Authors.

#ifndef INCLUDE_COMMON_CLIENT_SERVICE_H_
#define INCLUDE_COMMON_CLIENT_SERVICE_H_
#include <vector>
#include "proto/messages.pb.h"
#include "net/net.h"
#include "utils/logging.h"
#include "common/request_handler.h"
using std::condition_variable;
using std::mutex;
namespace ustore {

/**
 * The client sends requests to this ClientService, which then forwards 
 * them to the storage.  
 * 
 * The client threads (ClientThread) pulls request from NextRequest(.) method
 * which is modelled as a shared request queue.  
 *
 * The current design assumes the client is the same as the ClientService,
 * i.e. the ClientService issues the request themselves. Thus, NextRequest(.)
 * simply generates a new request everytime it is called.
 *
 */
class ClientService {
 public:
    explicit ClientService(const node_id_t& master): master_(master) {}
    ~ClientService();

    // initialize the network, register callback, spawn ClientThreads
    virtual void Init();

    // Dispatching responses from the network.
    // Basically call this->HandleResponse() that wakes up client threads.
    static void ResponseDispatch(const void *msg, int size,
        void *handler, const node_id_t& source);

    /**
     * Handle a response from workers:
     * 1. It parse msg into a UStoreMessage
     * 2. Wake up the appropriate ResponseBlob, based on the source 
     *    field, which is in [0..nthreads). No lock is needed here, because
     *    clients are synchronous (there cannot be two response to the same 
     *    client.
     * 3. Hand-off the message to whatever thread is waiting for it.
     */
    virtual void HandleResponse(const void *msg, int size, const node_id_t& source);

    /**
     * Inside this thread a RequestHandle is initialized, using the following
     * information:
     *  + master
     *  + Network object (shared with other threads)
     *  + id
     *  + ResponseBlob, which is responses_[id]
     */
    void ClienThread(const node_id_t& master, int id);

    /**
     * Called from ClientThread (in a loop) to process next request.
     * When the client is decoupled from ClientService, this method should
     * have access to a shared queue of requests. 
     */
    virtual void NextRequest(RequestHandler *handler);

 private:
    int nthreads_;  // how many RequestHandler thread it uses
    node_id_t master_;  // master node
    Net *net_;
    vector<ResponseBlob*> responses_;  // the response queue
};
}  // namespace ustore
#endif  // INCLUDE_COMMON_CLIENT_SERVICE_H_
