/*
 * zmq_net.cc
 *
 *  Created on: Mar 13, 2017
 *      Author: zhanghao
 */

#include <string>
#include <set>
#include <czmq.h>
#include <gflags/gflags.h>
//#include <glog/logging.h>
#include "utils/logging.h"
#include "net/zmq_net.h"

using std::string;
using std::vector;

/**
 * Implementation of Network communication via ZeroMQ
 */
DEFINE_string(inproc, "inproc://test", "");
#define POLL_TIMEOUT 10
namespace ustore {

// server thread to process messages
void ServerThread(void *args, zctx_t *ctx, void *pipe);

Net::~Net() {
  for (auto val = netmap_.begin(); val != netmap_.end(); val++)
    delete val->second;
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
  socket_ctx_ = zctx_new();
  recv_sock_ = zsocket_new((zctx_t*) socket_ctx_, ZMQ_ROUTER);
  string host = "tcp://" + id;
  CHECK(zsocket_bind(recv_sock_, "%s", host.c_str()));
  backend_sock_ = zsocket_new((zctx_t*) socket_ctx_, ZMQ_DEALER);
  CHECK(zsocket_bind(backend_sock_, "%s", FLAGS_inproc.c_str()) == 0);

  // start backend thread
  for (int i = 0; i < nthreads_; i++)
    zthread_fork((zctx_t*) socket_ctx_, ServerThread, this);

  is_running_ = true;
}

NetContext* ZmqNet::CreateNetContext(const node_id_t& id) {
  ZmqNetContext* ctx = nullptr;
  if (!netmap_.count(id)) {
    ctx = new ZmqNetContext(cur_node_, id);
    netmap_[id] = ctx;
  }
  return netmap_[id];
}

void ZmqNet::RegisterRecv(CallBackProc* func, void* handler) {
  cb_ = func;
  upstream_handle_ = handler;
}

// forwarding messages from router to dealer socket
void ZmqNet::Start() {
  while (is_running_) {
    zmq_pollitem_t items[1];
    items[0] = {recv_sock_, 0, ZMQ_POLLIN, 0};
    int rc = zmq_poll(items, 1, POLL_TIMEOUT);
    if (rc < 0) {
      break;
    }

    if (items[0].revents & ZMQ_POLLIN) {
      zmsg_t *msg = zmsg_recv(recv_sock_);
      if (!msg)
        break;
      //send to backend
      zmsg_send(&msg, backend_sock_);
    }
  }
  //Stop when ^C
  zsocket_destroy((zctx_t*) socket_ctx_, recv_sock_);
  zsocket_destroy((zctx_t*) socket_ctx_, backend_sock_);
  zctx_destroy((zctx_t**) (&socket_ctx_));
}

void ZmqNet::Stop() {
  is_running_ = false;
}

void ServerThread(void *args, zctx_t *ctx, void *pipe) {
  ZmqNet *net = static_cast<ZmqNet*>(args);

  //create and connect to the frontend's dealer socket
  void *frontend = zsocket_new(ctx, ZMQ_DEALER);
  CHECK(zsocket_connect(frontend, "%s", FLAGS_inproc.c_str()) == 0);

  while (net->IsRunning()) {
    zmsg_t *msg = zmsg_recv(frontend);
    if (!msg)
      break;

    // message processing
    // first frame is the identity (from router)
    // next contain connection ID
    // final frame is the message itself
    zframe_t *identity = zmsg_pop(msg);
    char *connection_id = zmsg_popstr(msg);

    zframe_t *content = zmsg_pop(msg);
    net->Dispatch(string(connection_id), zframe_data(content), zframe_size(content));
    zframe_destroy(&identity);
    zframe_destroy(&content);
    free(connection_id);
  }
  zsocket_destroy(ctx, frontend);
}

ZmqNetContext::ZmqNetContext(const node_id_t& src, const node_id_t& dest)
    : NetContext(src, dest) {
  // start connection to the remote host
  send_ctx_ = zctx_new();
  send_sock_ = zsocket_new((zctx_t*) send_ctx_, ZMQ_DEALER);
  string host = "tcp://" + dest_id_;
  CHECK(zsocket_connect(send_sock_, "%s", host.c_str()) == 0);
  LOG(ERROR)<< "Connected to " << host;
}

ZmqNetContext::~ZmqNetContext() {
  zsocket_destroy((zctx_t*) send_ctx_, send_sock_);
  zctx_destroy((zctx_t**) (&send_ctx_));
}

ssize_t ZmqNetContext::Send(const void *ptr, size_t len, CallBackProc* func,
                            void* data) {

  zframe_t* frame = zframe_new(ptr, len);
  //free((char*) ptr);
  zframe_t* id = zframe_new(src_id_.c_str(), src_id_.length());
  zmsg_t* msg = zmsg_new();
  zmsg_append(msg, &id);
  zmsg_append(msg, &frame);

  send_lock_.lock();
  int st = zmsg_send(&msg, send_sock_) == 0 ? len : -1;
  send_lock_.unlock();

  return st;
}

void ZmqNet::Dispatch(const node_id_t& source, const void *msg, int size) {
  //recv_lock_.lock();
  this->cb_(msg, size, this->upstream_handle_, source);
  //recv_lock_.unlock();
}

ssize_t NetContext::SyncSend(const void *ptr, size_t len) {
  // not implemented for now
  return 0;
}

ssize_t NetContext::SyncRecv(const void *ptr, size_t len) {
  // not implemented for now
  return 0;
}
}
