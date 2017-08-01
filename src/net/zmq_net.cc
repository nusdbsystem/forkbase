// Copyright (c) 2017 The Ustore Authors.

#include "net/zmq_net.h"

#include <czmq.h>
#include <gflags/gflags.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <cstring>
#include <string>
#include <set>
#include <thread>
#include "utils/logging.h"

namespace ustore {

/**
 * Implementation of Network communication via ZeroMQ
 */

// total wait time for socket: kWaitInterval*kPollRetries
constexpr int kWaitInterval = 10;
constexpr int kPollRetries = 2000;
constexpr int kSocketBindTimeout = 1;
constexpr int kSocketTrials = 5;

using std::string;
using std::vector;
using std::unordered_map;
// server thread to process messages
void ServerThread(void *args);

void ZmqNet::Stop() {
  is_running_ = false;
}

ZmqNetContext::ZmqNetContext(const node_id_t& src, const node_id_t& dest,
    ClientZmqNet *net) : NetContext(src, dest), client_net_(net) {
  // start connection to the remote host
  send_sock_ = zsock_new(ZMQ_DEALER);
  zsock_connect((zsock_t *)send_sock_, "%s",
      client_net_->request_ep_.c_str());
}

ZmqNetContext::~ZmqNetContext() {
  // Send(kCloseMsg, std::strlen(kCloseMsg));
  zsock_destroy((zsock_t **)&send_sock_);
}

ssize_t ZmqNetContext::Send(const void *ptr, size_t len, CallBack* func) {
  zframe_t* frame = zframe_new(ptr, len);
  // free((char*) ptr);
  zframe_t* id = zframe_new(src_id_.c_str(), src_id_.length());
  zmsg_t* msg = zmsg_new();
  zmsg_pushstr(msg, dest_id_.c_str());
  zmsg_append(msg, &id);
  zmsg_append(msg, &frame);
  send_lock_.lock();
  int st = zmsg_send(&msg, (zsock_t *)send_sock_) == 0 ? len : -1;
  send_lock_.unlock();
  return st;
}

void ZmqNet::Dispatch(const node_id_t& source, const void *msg, int size) {
  // recv_lock_.lock();
  /*
  if (memcmp(kCloseMsg, msg, size) == 0) {
    DLOG(WARNING) << "Close ZmqNetContext " << source;
    DeleteNetContext(source);
    return;
  }
  */
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


// ClientZMQ constructor
ClientZmqNet::ClientZmqNet(int nthreads) : ZmqNet("", nthreads) {
  int status;
  int ntries = kSocketTrials;
  srand(time(NULL));
  int port = rand() % 0xffffff;
  inproc_ep_ = "inproc://"  + std::to_string(port);

  // make sure that the ipc socket binds successfully
  backend_sock_ = zsock_new(ZMQ_DEALER);
  ntries = kSocketTrials;
  while ((status = zsock_bind((zsock_t *)backend_sock_, "%s",
          inproc_ep_.c_str())) && ntries--) {
    sleep(kSocketBindTimeout);
    port = rand() & 0xffffff;
    inproc_ep_ = "inproc://"  + std::to_string(port);
  }
  CHECK_EQ(status, 0);

  // request_sock_
  port = rand() % 0xffffff;
  request_ep_ = "inproc://"  + std::to_string(port);

  // make sure that the ipc socket binds successfully
  request_sock_ = zsock_new(ZMQ_ROUTER);
  ntries = kSocketTrials;
  while ((status = zsock_bind((zsock_t *)request_sock_, "%s",
          request_ep_.c_str())) && ntries--) {
    sleep(kSocketBindTimeout);
    port = rand() & 0xffffff;
    request_ep_ = "inproc://"  + std::to_string(port);
  }
  CHECK_EQ(status, 0);

  is_running_ = true;
  timeout_counter_ = kPollRetries;
  request_counter_ = 0;
}

NetContext* ZmqNet::CreateNetContext(const node_id_t& id) {
  ZmqNetContext* ctx = nullptr;
  if (!netmap_.count(id)) {
    ctx = new ZmqNetContext(cur_node_, id,
                      reinterpret_cast<ClientZmqNet *>(this));
    netmap_[id] = ctx;
  }
  CHECK(netmap_[id]) << "Creating netcontext failed";
  return netmap_[id];
}

void ClientZmqNet::Start() {
  // start backend thread
  for (int i = 0; i < nthreads_; i++)
    backend_threads_.push_back(
              std::thread(&ClientZmqNet::ClientThread, this));

  int pollsize = netmap_.size() + 1;
  zmq_pollitem_t items[pollsize];
  items[0] = {zsock_resolve(request_sock_), 0, ZMQ_POLLIN, 0};
  // start DEALER socket to other
  unordered_map<node_id_t, void*> out_socks;
  int counter = 0;
  for (auto n : netmap_) {
    void *sock_ = zsock_new(ZMQ_DEALER);
    string host = "tcp://" + n.first;
    CHECK_EQ(zsock_connect((zsock_t *)sock_, "%s", host.c_str()), 0);
    out_socks[n.first] = sock_;
    // zpoller_add(zpoller, sock_);
    items[++counter] = {zsock_resolve(sock_), 0, ZMQ_POLLIN, 0};
  }

  while (is_running_) {
    int rc = zmq_poll(items, pollsize, kWaitInterval);
    // error
    if (rc < 0) break;

    // expired
    if (rc == 0 && is_running_ && !(timeout_counter_--)
        && request_counter_)
       LOG(FATAL) << "Connection timed out. Server may not be ready yet!";

    // all well
    if (items[0].revents & ZMQ_POLLIN) {
      zmsg_t *msg = zmsg_recv(request_sock_);
      if (!msg) break;
      zmsg_pop(msg);
      char *dest_id = zmsg_popstr(msg);
      zmsg_send(&msg, out_socks[dest_id]);
      request_counter_++;
      timeout_counter_ = kPollRetries;
    }
    for (int i = 1; i < pollsize; i++) {
      if (items[i].revents & ZMQ_POLLIN) {
        zmsg_t *msg = zmsg_recv(items[i].socket);
        if (!msg) break;
        zmsg_send(&msg, backend_sock_);
        request_counter_--;
        timeout_counter_ = kPollRetries;
      }
    }
  }
  // Stop when ^C
  zsock_destroy((zsock_t **)&backend_sock_);
  zsock_destroy((zsock_t **)&request_sock_);
  for (auto s : out_socks)
    zsock_destroy((zsock_t **)&s.second);

  // zpoller_destroy(&zpoller);
  for (size_t i=0; i < backend_threads_.size(); i++)
    backend_threads_[i].join();
}

void ClientZmqNet::ClientThread() {
  // create and connect to the frontend's dealer socket
  void *frontend = zsock_new(ZMQ_DEALER);
  CHECK_EQ(zsock_connect((zsock_t *)frontend,
           "%s", inproc_ep_.c_str()), 0);
  zmq_pollitem_t items[1];
  items[0] = {zsock_resolve(frontend), 0, ZMQ_POLLIN, 0};
  while (IsRunning()) {
    int rc = zmq_poll(items, 1, kWaitInterval);
    if (rc < 0)
      break;

    if (items[0].revents & ZMQ_POLLIN) {
      zmsg_t *msg = zmsg_recv(frontend);
      if (!msg)
        break;

      // message processing
      // first frame is connection ID
      // final frame is the message itself
      char *connection_id = zmsg_popstr(msg);

      zframe_t *content = zmsg_pop(msg);
      Dispatch(string(connection_id),
          zframe_data(content), zframe_size(content));
      zframe_destroy(&content);
      free(connection_id);
    }
  }
  zsock_destroy((zsock_t **)&frontend);
}

ServerZmqNet::ServerZmqNet(const node_id_t& id, int nthreads)
    : ZmqNet(id, nthreads) {
  int status;
  int ntries = kSocketTrials;

  // start router socket
  recv_sock_ = zsock_new(ZMQ_ROUTER);

  int split = id.find(':');
  string hostname = id.substr(0, split);
  string port = id.substr(split+1);
  hostent* record = gethostbyname(hostname.c_str());
  if (record == nullptr) {
    LOG(ERROR) << hostname <<  " is unavailable\n";
    exit(1);
  }
  in_addr* address = reinterpret_cast<in_addr*>(record->h_addr);
  string ip_address = inet_ntoa(*address);
  string host = "tcp://" + ip_address + ":" + port;
  ntries = kSocketTrials;
  while ((status = zsock_bind((zsock_t *)recv_sock_, "%s",
          host.c_str())) < 0 && ntries--)
    sleep(kSocketBindTimeout);
  CHECK_GT(status, 0);

  srand(time(NULL));
  int iport = rand() & 0xffffff;
  inproc_ep_ = "inproc://"  + std::to_string(iport);

  // backend socket
  backend_sock_ = zsock_new(ZMQ_DEALER);
  ntries = kSocketTrials;
  while ((status = zsock_bind((zsock_t *)backend_sock_, "%s",
          inproc_ep_.c_str())) && ntries--) {
    sleep(kSocketBindTimeout);
    iport = rand() & 0xffffff;
    inproc_ep_ = "inproc://"  + std::to_string(iport);
  }
  CHECK_EQ(status, 0);

  srand(time(NULL));
  iport = rand() & 0xffffff;
  result_ep_ = "inproc://" + std::to_string(iport);

  // result socket
  result_sock_ = zsock_new(ZMQ_DEALER);
  ntries = kSocketTrials;
  while ((status = zsock_bind((zsock_t *)result_sock_, "%s",
          result_ep_.c_str())) && ntries--) {
    sleep(kSocketBindTimeout);
    iport = rand() & 0xffffff;
    result_ep_ = "inproc://"  + std::to_string(iport);
  }
  CHECK_EQ(status, 0);

  is_running_ = true;
}

void ServerZmqNet::Start() {
  // start backend thread
  for (int i=0; i < nthreads_; i++) {
    CHECK_EQ(netmap_.count(std::to_string(i)), size_t(0));
    ServerZmqNetContext *nctx =
              new ServerZmqNetContext("", "", inproc_ep_, result_ep_,
                                    std::to_string(i));
    netmap_[std::to_string(i)] = nctx;
    backend_threads_.push_back(
                    std::thread(&ServerZmqNetContext::Start, nctx, this));
  }

  // zpoller_t *zpoller = zpoller_new(recv_sock_, result_sock_, NULL);
  // CHECK_NOTNULL(zpoller);
  zmq_pollitem_t items[2];
  items[0] = {zsock_resolve(recv_sock_), 0, ZMQ_POLLIN, 0};
  items[1] = {zsock_resolve(result_sock_), 0, ZMQ_POLLIN, 0};

  while (is_running_) {
    // void *sock = zpoller_wait(zpoller, kWaitInterval);
    int rc = zmq_poll(items, 2, kWaitInterval);
    if (rc < 0) break;
    // LOG(ERROR) << "Got client message ";
    if (items[0].revents & ZMQ_POLLIN) {
      zmsg_t *msg = zmsg_recv(items[0].socket);
      if (!msg) break;
      zmsg_send(&msg, zsock_resolve(backend_sock_));
    }

    if (items[1].revents & ZMQ_POLLIN) {
      zmsg_t *msg = zmsg_recv(items[1].socket);
      if (!msg) break;
      zmsg_send(&msg, zsock_resolve(recv_sock_));
    }
  }
  // Stop when ^C
  zsock_destroy((zsock_t **)&recv_sock_);
  zsock_destroy((zsock_t **)&backend_sock_);
  zsock_destroy((zsock_t **)&result_sock_);
  // zpoller_destroy(&zpoller);

  for (size_t i=0; i < backend_threads_.size(); i++)
    backend_threads_[i].join();
}

ServerZmqNetContext::ServerZmqNetContext(const node_id_t& src,
    const node_id_t& dest, const string& ipc_ep, const string& result_ep,
    const string& id) : NetContext(src, dest), id_(id) {
  recv_sock_ = zsock_new(ZMQ_DEALER);
  CHECK_EQ(zsock_connect((zsock_t *)recv_sock_, "%s", ipc_ep.c_str()), 0);
  send_sock_ = zsock_new(ZMQ_DEALER);
  CHECK_EQ(zsock_connect((zsock_t *)send_sock_, "%s", result_ep.c_str()), 0);
}

ssize_t ServerZmqNetContext::Send(const void *ptr, size_t len, CallBack* func) {
  // append id frame
  CHECK_NOTNULL(client_id_);

  zframe_t* frame = zframe_new(ptr, len);
  // free((char*) ptr);
  zframe_t* id = zframe_new(src_id_.c_str(), src_id_.length());
  zmsg_t* msg = zmsg_new();
  zmsg_append(msg, &id);
  zmsg_append(msg, &frame);

  zmsg_prepend(msg, reinterpret_cast<zframe_t**>(&client_id_));
  send_lock_.lock();
  int st = zmsg_send(&msg, (zsock_t *)send_sock_) == 0 ? len : -1;
  send_lock_.unlock();
  return st;
}

void ServerZmqNetContext::Start(ServerZmqNet *net) {
  // listen from  recv_sock_
  // zpoller_t *zpoller = zpoller_new(recv_sock_, NULL);
  zmq_pollitem_t items[1];
  items[0] = {zsock_resolve(recv_sock_), 0, ZMQ_POLLIN, 0};
  while (net->IsRunning()) {
    // void *sock = zpoller_wait(zpoller, kWaitInterval);
    int rc = zmq_poll(items, 1, kWaitInterval);
    if (rc < 0)
      break;

    if (items[0].revents && ZMQ_POLLIN) {
      zmsg_t *msg = zmsg_recv(items[0].socket);
      if (!msg)
        break;

      // message processing
      // first frame is the identity (from router)
      // next contain connection ID
      // final frame is the message itself
      client_id_ = zmsg_pop(msg);

      char *connection_id = zmsg_popstr(msg);

      zframe_t *content = zmsg_pop(msg);
      // if there's a response, it will be forwarded by this thread
      net->Dispatch(id_, zframe_data(content), zframe_size(content));
      zframe_destroy(&content);
      free(connection_id);
    }
  }
  zsock_destroy((zsock_t **)&send_sock_);
  zsock_destroy((zsock_t **)&recv_sock_);
}

}  // namespace ustore
