// Copyright (c) 2017 The Ustore Authors.

#ifndef INCLUDE_NET_RDMA_NET_H_
#define INCLUDE_NET_RDMA_NET_H_

#include <infiniband/verbs.h>
#include <atomic>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include "net/net.h"

namespace ustore {

// raddr means memory addr that is registered with RDMA device
typedef void* raddr;
class aeEventLoop;
class HashTable;
class RdmaNetResource;

class RdmaNet : public Net {
 public:
  explicit RdmaNet(const node_id_t& id, int nthreads);
  ~RdmaNet();

  NetContext* CreateNetContext(const node_id_t& id) override;
  void Start() override {}
  void Stop() override {}

 private:
  void StartService(const node_id_t& id, RdmaNetResource* res);

  RdmaNetResource* resource_ = nullptr;
  aeEventLoop* el_ = nullptr;
  int sockfd_ = 0;
  HashTable<uint32_t, RdmaNetContext*> qpCliMap_;
  HashTable<std::string, RdmaNetContext*> idCliMap_;
  std::thread* st_ = nullptr;
  std::mutex net_lock_;

  boost::asio::io_service ioService_;
  boost::thread_group threadpool_;
  boost::asio::io_service::work work_;

  int nthreads_;  // number of processing threads
  /*
  //below are for internal use
  RdmaNetContext* CreateRdmaNetContext(node_id_t id, bool& exist);
  inline void RemoveContext(RdmaNetContext* ctx) {
    qpCliMap.erase(ctx->GetQP());
    idCliMap.erase(ctx->GetID());
  }

  inline RdmaNetContext* FindContext(uint32_t qpn) {
    RdmaNetContext* ctx = nullptr;
    try {
      ctx = qpCliMap.at(qpn);
    } catch (const std::out_of_range& oor) {
      LOG(LOG_WARNING, "cannot find the client for qpn %d (%s)", qpn,
          oor.what());
    }
    return ctx;
  }

  inline RdmaNetContext* FindContextID(node_id_t id) {
    RdmaNetContext* ctx = nullptr;
    if (idCliMap.count(id)) {
      ctx = idCliMap.at(id);
    }
    return ctx;
  }

  void ProcessRdmaRequest();

  static void CbHandler(RdmaNetContext* ctx, void* msg, size_t size,
                        RdmaNetResource* resource, uint64_t wr_id);
                        */
};

// RDMA implementation of network context
class RdmaNetContext : public NetContext {
 public:
  RdmaNetContext(const node_id_t& id, RdmaNetResource* res);
  ~RdmaNetContext();

  // implementation of the methods inherited from NetContext
  ssize_t Send(void* ptr, size_t len, CallBackProc* func = nullptr,
               void* app_data = nullptr);
  void RegisterRecv(CallBackProc* func, void* data);

  /**
   * Memory API
   * @dest: dest addr at remote node
   * @src: src addr at local node
   */
  ssize_t Write(raddr dest, raddr src, size_t len, CallBackProc* func = nullptr,
                void* app_data = nullptr);
  ssize_t WriteWithImm(raddr dest, raddr src, size_t len, uint32_t imm,
                       CallBackProc* func = nullptr, void* app_data = nullptr);
  ssize_t Read(raddr dest, raddr src, size_t len, CallBackProc* func = nullptr,
               void* app_data = nullptr);
  ssize_t WriteBlocking(raddr dest, raddr src, size_t len);
  ssize_t WriteWithImmBlocking(raddr dest, raddr src, size_t len, uint32_t imm);
  ssize_t ReadBlocking(raddr dest, raddr src, size_t len);

 private:
  // below are for internal use
  inline const char* GetRdmaConnString() const;
  int SetRemoteConnParam(const char *remConn);
  int ExchConnParam(node_id_t cur_node, const char* ip, int port);

  inline uint32_t GetQP() { return qp->qp_num_; }
  inline const node_id_t& GetID() { return id_; }

  unsigned int SendComp(ibv_wc& wc);
  unsigned int WriteComp(ibv_wc& wc);
  char* RecvComp(ibv_wc& wc);
  char* GetFreeSlot();

  inline int PostRecv(int n) { return resource_->PostRecv(n);}
  inline void lock() { global_lock_.lock(); }
  inline void unlock() { global_lock_.unlock(); }

  char* GetFreeSlot_();
  void ProcessPendingRequests(int n);
  bool IsRegistered(const void* addr);

  ssize_t Rdma(ibv_wr_opcode op, const void* src, size_t len,
               unsigned int id = 0, bool signaled = false, void* dest = nullptr,
               uint32_t imm = 0, uint64_t oldval = 0, uint64_t newval = 0);
  ssize_t Rdma(RdmaRequest& r);
  ssize_t Send_(const void* ptr, size_t len, unsigned int id = 0,
                bool signaled = false);
  /*
   * @dest: dest addr at remote node
   * @src: src addr at local node
   */
  ssize_t Write_(raddr dest, raddr src, size_t len, unsigned int id = 0,
                 bool signaled = false);
  ssize_t WriteWithImm_(raddr dest, raddr src, size_t len, uint32_t imm,
                        unsigned int id = 0, bool signaled = false);
  /*
   * @dest: dest addr at local node
   * @src: src addr at remote node
   */
  ssize_t Read_(raddr dest, raddr src, size_t len, unsigned int id = 0,
                bool signaled = false);
  ssize_t Cas_(raddr src, uint64_t oldval, uint64_t newval, unsigned int id = 0,
               bool signaled = false);

  RdmaNetResource *resource_;
  ibv_qp *qp_;
  node_id_t id_;
  ibv_mr* send_buf_;  // send buf
  int slot_head_;
  int slot_tail_;
  // to differentiate between all free and all occupied slot_head == slot_tail
  bool full_;

  uint64_t vaddr_ = 0;  // for remote rdma read/write
  uint32_t rkey_ = 0;

  int max_pending_msg_;
  int max_unsignaled_msg_;
  // including both RDMA send and write/read that don't use the send buf
  std::atomic<int> pending_msg_;
  // including only send msg
  std::atomic<int> pending_send_msg_;
  // in order to proceed the slot_tail
  std::atomic<int> to_signaled_send_msg_;
  std::atomic<int> to_signaled_w_r_msg_;
  std::queue<RdmaRequest> pending_requests_;
  char *msg_ = nullptr;
  std::mutex global_lock_;
};

struct RdmaRequest {
  ibv_wr_opcode op;
  const void* src;
  size_t len;
  unsigned int id;
  bool signaled;
  void* dest;
  uint32_t imm;
  uint64_t oldval;
  uint64_t newval;
};

class RdmaNetResource {
  friend class RdmaNetContext;
 public:
  explicit RdmaNetResource(ibv_device *device);
  ~RdmaNetResource();

  inline const char* GetDevname() const { return this->devName; }
  inline ibv_cq* GetCompQueue() const { return cq; }
  inline int GetChannelFd() const { return channel->fd; }

  bool GetCompEvent() const;
  int RegLocalMemory(void *base, size_t sz);
  int RegCommSlot(int);
  char* GetSlot(int s);  // get the starting addr of the slot
  int PostRecv(int n);  // post n RR to the srq
  int PostRecvSlot(int slot);  // post slot to the srq
  // int ClearRecv(int low, int high);

  RdmaNetContext* NewRdmaNetContext(node_id_t id);
  void DeleteRdmaNetContext(RdmaNetContext* ctx);

  inline void ClearSlot(int s) { slots.at(s) = false; }
  inline int GetCounter() const { return rdma_context_counter; }

 private:
  ibv_device *device_;
  const char *devName_ = NULL;
  ibv_context *context_;
  ibv_comp_channel *channel_;
  ibv_pd *pd_;
  ibv_cq *cq_;  // global comp queue
  ibv_srq *srq_;  // share receive queue

  ibv_port_attr portAttribute_;
  int ibport_ = 1;  // TODO(zhanghao): dual-port support
  uint32_t psn_;

  // the follow three variables are only used for comm among workers
  void* base_ = nullptr;  // the base addr for the local memory
  size_t size_ = 0;
  struct ibv_mr *bmr_ = nullptr;

  // node-wide communication buf used for receive request
  std::vector<struct ibv_mr*> comm_buf_;
  // size_t buf_size; buf_size = slots.size() * MAX_REQUEST_SIZE
  int slot_head_;  // current slot head
  int slot_inuse_;  // number of slots in use
  // TODO(zhanghao): check whether head + tail is enough
  // the states of all the allocated slots (true: occupied, false: free)
  std::vector<bool> slots_;
  // current created RdmaNetContext
  int rdma_context_counter_;

  std::atomic<int> recv_posted_;
  int rx_depth_ = 0;
};

}  // namespace ustore

#endif  // INCLUDE_NET_RDMA_NET_H_
