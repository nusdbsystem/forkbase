// Copyright (c) 2017 The Ustore Authors.

#include "net/zmq_net.h"

#include <czmq.h>
#include <gflags/gflags.h>
#include <string>
#include <thread>
#include <set>
#include "utils/logging.h"

namespace ustore {

/**
 * Implementation of Network communication via ZeroMQ
 */

#define POLL_TIMEOUT 10

using std::string;
using std::vector;

// server thread to process messages
void ServerThread(void *args);

Net::~Net() {
  for (auto val : netmap_) 
    delete val.second;
}

void Net::CreateNetContexts(const vector<node_id_t>& nodes) {
  std::set<node_id_t> tmp;
  for (size_t i = 0; i < nodes.size(); i++)
    tmp.insert(nodes[i]);
  for (auto val = netmap_.begin(); val != netmap_.end(); val++)
    if (tmp.find(val->first) == tmp.end())
      delete val->second;
    else
      tmp.erase(val->first);

  for (auto val = tmp.begin(); val != tmp.end(); val++) {
    if (*val != cur_node_)
      CreateNetContext(*val);
  }
}

ZmqNet::ZmqNet(const node_id_t& id, int nthreads)
    : Net(id), nthreads_(nthreads) {
  // start router socket
  recv_sock_ = zsock_new(ZMQ_ROUTER);
  string host = "tcp://" + id;
  inproc_ep_ = "inproc://" + id;
  CHECK(zsock_bind((zsock_t *)recv_sock_, "%s", host.c_str()));
  backend_sock_ = zsock_new(ZMQ_DEALER);
  CHECK_EQ(zsock_bind((zsock_t *)backend_sock_, "%s", inproc_ep_.c_str()), 0);
  // start backend thread
  for (int i = 0; i < nthreads_; i++)
    backend_threads_.push_back(std::thread(ServerThread, this));

  is_running_ = true;
}

NetContext* ZmqNet::CreateNetContext(const node_id_t& id) {
  ZmqNetContext* ctx = nullptr;
  if (!netmap_.count(id)) {
    ctx = new ZmqNetContext(cur_node_, id);
    netmap_[id] = ctx;
  }
  CHECK(netmap_[id]) << "Creating netcontext failed";
  return netmap_[id];
}

// forwarding messages from router to dealer socket
void ZmqNet::Start() {
  zpoller_t *zpoller = zpoller_new(recv_sock_, NULL);
  while (is_running_) {
    void *sock = zpoller_wait(zpoller, POLL_TIMEOUT);
    if (!sock && !zpoller_expired(zpoller)) 
      break;
    if (sock) {
      zmsg_t *msg = zmsg_recv(sock);
      if (!msg) 
        break;
      // send to backend
      zmsg_send(&msg, backend_sock_);
    }
  }
  // Stop when ^C
  zsock_destroy((zsock_t **)&recv_sock_);
  zsock_destroy((zsock_t **)&backend_sock_);
  zpoller_destroy(&zpoller);
  for (int i=0; i < backend_threads_.size(); i++)
    backend_threads_[i].join();
}

void ZmqNet::Stop() {
  is_running_ = false;
}

void ServerThread(void *args) {
  ZmqNet *net = static_cast<ZmqNet*>(args);
  // create and connect to the frontend's dealer socket
  void *frontend = zsock_new(ZMQ_DEALER);
  CHECK_EQ(zsock_connect((zsock_t *)frontend, "%s", net->get_inproc_ep().c_str()), 0);
  zpoller_t *zpoller = zpoller_new(frontend, NULL);
  while (net->IsRunning()) {
    void *sock = zpoller_wait(zpoller, POLL_TIMEOUT);
    if (!sock && !zpoller_expired(zpoller))
      break;
    if (sock) {
      zmsg_t *msg = zmsg_recv(sock);
      if (!msg)
        break;

      // message processing
      // first frame is the identity (from router)
      // next contain connection ID
      // final frame is the message itself
      zframe_t *identity = zmsg_pop(msg);
      char *connection_id = zmsg_popstr(msg);

      zframe_t *content = zmsg_pop(msg);
      net->Dispatch(string(connection_id),
          zframe_data(content), zframe_size(content));
      zframe_destroy(&identity);
      zframe_destroy(&content);
      free(connection_id);
    }
  }
  zsock_destroy((zsock_t **)&frontend);
}

ZmqNetContext::ZmqNetContext(const node_id_t& src, const node_id_t& dest)
    : NetContext(src, dest) {
  // start connection to the remote host
  send_sock_ = zsock_new(ZMQ_DEALER);
  string host = "tcp://" + dest_id_;
  CHECK_EQ(zsock_connect((zsock_t *)send_sock_, "%s", host.c_str()), 0);
}

ZmqNetContext::~ZmqNetContext() {
  zsock_destroy((zsock_t **)&send_sock_);
}

ssize_t ZmqNetContext::Send(const void *ptr, size_t len, CallBack* func) {
  zframe_t* frame = zframe_new(ptr, len);
  // free((char*) ptr);
  zframe_t* id = zframe_new(src_id_.c_str(), src_id_.length());
  zmsg_t* msg = zmsg_new();
  zmsg_append(msg, &id);
  zmsg_append(msg, &frame);

  send_lock_.lock();
  int st = zmsg_send(&msg, (zsock_t *)send_sock_) == 0 ? len : -1;
  send_lock_.unlock();
  return st;
}

void ZmqNet::Dispatch(const node_id_t& source, const void *msg, int size) {
  // recv_lock_.lock();
  (*this->cb_)(msg, size, source);
  // recv_lock_.unlock();
}

ssize_t NetContext::SyncSend(const void *ptr, size_t len) {
  // not implemented for now
  return 0;
}

ssize_t NetContext::SyncRecv(const void *ptr, size_t len) {
  // not implemented for now
  return 0;
}
}  // namespace ustore
