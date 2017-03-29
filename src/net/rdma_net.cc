// Copyright (c) 2017 The Ustore Authors.

#ifdef USE_RDMA

#include <cstring>
#include <cassert>
#include <climits>
#include <vector>
#include "net/net.h"
#include "net/ae.h"
#include "net/anet.h"
#include "utils/logging.h"
#include "net/rdma_net.h"
#include "net/rdma_config.h"

namespace ustore {

int page_size = 4096;
int MAX_RDMA_INLINE_SIZE = 256;

template <>
vector<string>& Split<string>(stringstream& ss, vector<string>& elems,
                              char delim) {
  string item;
  while (getline(ss, item, delim)) {
    if (!item.empty()) elems.push_back(item);
  }
  return elems;
}

RdmaNetResource* RdmaNetResourceFactory::getRdmaNetResource(
    const char *devName) {
  if (!devName) {
    devName = defaultDevname;
  }

  if (devName) {
    for (std::vector<RdmaNetResource *>::iterator it = resources.begin();
        it != resources.end(); ++it) {
      if (!strcmp((*it)->GetDevname(), devName))
        return (*it);
    }
  }
  return newRdmaNetResource(devName);
}

RdmaNetResource* RdmaNetResourceFactory::newRdmaNetResource(
    const char* devName) {
  ibv_device **list = ibv_get_device_list(NULL);
  if (!devName && list[0])
    devName = defaultDevname = ibv_get_device_name(list[0]);

  for (int i = 0; list[i]; ++i) {
    if (!strcmp(devName, ibv_get_device_name(list[i]))) {
      try {
        RdmaNetResource *ret = new RdmaNetResource(list[i]);
        resources.push_back(ret);
        LOG(INFO)<< "new rdma resource_ at " << ret << std::endl;
        return ret;
      } catch (int err) {
        LOG(WARNING)<< "Unable to get rdam resource_" << std::endl;
        return NULL;
      }
    }
  }
  return NULL;
}

RdmaNetResource::RdmaNetResource(ibv_device *dev)
    : device_(dev),
      base_(nullptr),
      bmr_(nullptr),
      size_(0),
      rdma_context_counter_(0),
      slot_inuse_(0),
      slot_head_(0),
      recv_posted_() {
  // int rx_depth;

  if (!(context_ = ibv_open_device(dev))) {
    LOG(WARNING)<< "unable to get context for "
        << ibv_get_device_name(dev) << std::endl;
    return;
  }

  if (!(channel_ = ibv_create_comp_channel(this->context_))) {
    LOG(WARNING) << "Unable to create comp channel\n";
    goto clean_ctx;
  }

  if (!(pd_ = ibv_alloc_pd(this->context_))) {
    LOG(WARNING) << "Unable to allocate pd\n";
    goto clean_channel;
  }

  rx_depth_ = WORKER_RDMA_SRQ_RX_DEPTH > HW_MAX_PENDING ?
      HW_MAX_PENDING : WORKER_RDMA_SRQ_RX_DEPTH;
  if (!(cq_ = ibv_create_cq(this->context_,
                            (rx_depth_ << 1) + 1, NULL, this->channel_, 0))) {
    LOG(WARNING) << "Unable to create cq\n";
    goto clean_pd;
  }

  {
    ibv_srq_init_attr attr = {};
    attr.attr.max_wr = rx_depth_;
    attr.attr.max_sge = 1;

    if (!(srq_ = ibv_create_srq(this->pd_, &attr))) {
      LOG(WARNING) << "Unable to create srq\n";
      goto clean_cq;
    }
  }

  if (ibv_query_port(context_, ibport_, &portAttribute_)) {
    LOG(WARNING) << "Unable to query port %d\n", ibport_;
    goto clean_srq;
  }

  devName_ = ibv_get_device_name(this->device_);
  srand48(time(NULL));
  psn_ = lrand48() & 0xffffff;

  /* Request notification upon the next completion event */
  if (ibv_req_notify_cq(cq_, 0)) {
    LOG(WARNING) << "Couldn't request CQ notification\n";
    return;
  }

  return;

  clean_srq: ibv_destroy_srq(this->srq_);
  clean_cq: ibv_destroy_cq(this->cq_);
  clean_pd: ibv_dealloc_pd(this->pd_);
  clean_channel: ibv_destroy_comp_channel(this->channel_);
  clean_ctx: ibv_close_device(this->context_);

  throw RDMA_RESOURCE_EXCEPTION;
}

RdmaNetResource::~RdmaNetResource() {
  ibv_destroy_srq(this->srq_);
  ibv_destroy_cq(this->cq_);
  ibv_dealloc_pd(this->pd_);
  ibv_destroy_comp_channel(this->channel_);
  ibv_close_device(this->context_);
  for (ibv_mr* mr : comm_buf_) {
    ibv_dereg_mr(mr);
    free(mr->addr);
  }
}

int RdmaNetResource::RegCommSlot(int slot) {
  if (slots_.size() - slot_inuse_ >= slot) {
    slot_inuse_ += slot;
    return 0;
  } else {
    slot_inuse_ += slot;
    int i = slots_.size();
    for (; i < slot_inuse_; i += RECV_SLOT_STEP) {
      int sz = roundup(RECV_SLOT_STEP * MAX_REQUEST_SIZE, page_size);
      void* buf = malloc(sz);
      struct ibv_mr *mr = ibv_reg_mr(
          this->pd_,
          buf,
          sz,
          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
              | IBV_ACCESS_REMOTE_READ);
      if (!mr) {
        LOG(WARNING)<< "Unable to register mr for communication slots\n";
        return -1;
      }
      comm_buf_.push_back(mr);
      assert(mr->addr == buf && mr->length == sz);
    }
    slots_.reserve(i);
    for (int j = slots_.size(); j < i; j++) {
      slots_.push_back(false);
    }

    /*    LOG(LOG_DEBUG, "registered %d, enlarge to %d with inuse = %d\n", slot,
     slots_.size(), slot_inuse_);*/
    assert(slots_.size() % RECV_SLOT_STEP == 0);
    return 0;
  }
}

char* RdmaNetResource::GetSlot(int slot) const {
//  if(!(slots_.at(slot) == true && slot < slot_inuse_)) {
//    LOG(WARNING) << "slot = " << slot << "slots_.at(slot) = "
//      << slots_.at(slot) << " slot_inuse = " << slot_inuse_;
//    assert(false);
//  }
  // TODO(zhanghao): check slot == tail
  return static_cast<char*>(comm_buf_[BPOS(slot)]->addr) + BOFF(slot);
}

int RdmaNetResource::PostRecvSlot(int slot) {
  if (rx_depth_ == recv_posted_) {
    LOG(WARNING)<< "cannot post any recv, already full";
    return 0;
  }

  recv_posted_ += 1;
  ibv_recv_wr rr {};
  ibv_sge sge {};
  int bpos = BPOS(slot);
  int boff = BOFF(slot);

  sge.length = MAX_REQUEST_SIZE;
  sge.lkey = comm_buf_[bpos]->lkey;
  sge.addr = (uintptr_t) comm_buf_[bpos]->addr + boff;

  rr.wr_id = slot;
  rr.num_sge = 1;
  rr.sg_list = &sge;
  rr.next = nullptr;

  ibv_recv_wr* bad_rr;
  if (ibv_post_srq_recv(srq_, &rr, &bad_rr)) {
    LOG(WARNING) << "post recv request failed " << errno << strerror(errno);
    slots_.at(slot) = false;
    return 0;
  }
  return 1;
}

int RdmaNetResource::PostRecv(int n) {
  if (n > rx_depth_ - recv_posted_) {
    n = rx_depth_ - recv_posted_;
  }
  assert(n >= 0);
  if (n == 0)
    return 0;

  ibv_recv_wr rr[n];
  memset(rr, 0, sizeof(ibv_recv_wr) * n);
  ibv_sge sge[n];
  int i, ret;
  int head_init = slot_head_;
  for (i = 0; i < n;) {
    if (slots_.at(slot_head_) == true) {
      if (++slot_head_ == slot_inuse_)
        slot_head_ = 0;
      if (slot_head_ == head_init) {
        LOG(WARNING)<< "cannot find free recv slot " << n << std::endl;
        break;
      }
      continue;
    }
    int bpos = BPOS(slot_head_);
    int boff = BOFF(slot_head_);

    sge[i].length = MAX_REQUEST_SIZE;
    sge[i].lkey = comm_buf_[bpos]->lkey;
    sge[i].addr = (uintptr_t) comm_buf_[bpos]->addr + boff;

    rr[i].wr_id = slot_head_;
    rr[i].num_sge = 1;
    rr[i].sg_list = &sge[i];
    if (i + 1 < n)
      rr[i].next = &rr[i + 1];

    // advance the slot_head_ by 1
    slots_.at(slot_head_) = true;
    if (++slot_head_ == slot_inuse_)
      slot_head_ = 0;
    i++;
  }
  ret = i;

  if (i > 0) {
    rr[i - 1].next = nullptr;
    ibv_recv_wr* bad_rr;
    int ret = 0;
    if ((ret = ibv_post_srq_recv(srq_, rr, &bad_rr))) {
      LOG(WARNING)<< "post recv request failed: "
          << ret << " " << errno << " " << strerror(errno);
      assert(false);
      int s = bad_rr->wr_id;
      ret -= RMINUS(slot_head_, s, slot_inuse_);
      while (s != slot_head_) {
        slots_.at(s) = false;
        if (++s == slot_inuse_) s = 0;
      }
      slot_head_ = s;
    }
  }
  recv_posted_ += ret;
  return ret;
}

/*
 * TODO: check whether it is necessary if we already use the epoll mechanism
 */
bool RdmaNetResource::GetCompEvent() const {
  struct ibv_cq *ev_cq;
  void *ev_ctx;
  int ret;
  ret = ibv_get_cq_event(channel_, &ev_cq, &ev_ctx);
  if (ret) {
    LOG(WARNING)<< "Failed to get cq_event\n";
    return false;
  }
  /* Ack the event */
  ibv_ack_cq_events(ev_cq, 1);

  /* Request notification upon the next completion event */
  ret = ibv_req_notify_cq(ev_cq, 0);
  if (ret) {
    LOG(WARNING)<< "Couldn't request CQ notification\n";
    return false;
  }
  return true;
}

RdmaNetContext* RdmaNetResource::NewRdmaNetContext(node_id_t id) {
  rdma_context_counter_++;

  // int s = MAX_WORKER_PENDING_MSG;
  int s = MAX_WORKER_PENDING_MSG > HW_MAX_PENDING ?
      HW_MAX_PENDING : MAX_WORKER_PENDING_MSG;
  if (RegCommSlot(s)) {
    LOG(WARNING)<< "unable to register more communication slots\n";
    return nullptr;
  }
  LOG(INFO)<< "new RdmaNetContext: " << rdma_context_counter_ << std::endl;
  return new RdmaNetContext(id, this);
}

void RdmaNetResource::DeleteRdmaNetContext(RdmaNetContext* ctx) {
  rdma_context_counter_--;
  // TODO(zhanghao): de-regsiter the slots
  LOG(INFO)<< "delete RdmaNetContext: " << rdma_context_counter_ << std::endl;
  delete ctx;
}

RdmaNetContext::RdmaNetContext(const node_id_t& id, RdmaNetResource *res)
    : id_(id),
      resource_(res),
      msg_(),
      pending_msg_(0),
      pending_send_msg_(0) {

  max_pending_msg_ =
  MAX_WORKER_PENDING_MSG > HW_MAX_PENDING ?
  HW_MAX_PENDING :
                                            MAX_WORKER_PENDING_MSG;
  int max_buf_size = WORKER_BUFFER_SIZE;

  void* buf = malloc(roundup(max_buf_size, page_size));
  if (unlikely(!buf)) {
    LOG(WARNING)<< "Unable to allocate memeory\n";
    goto send_buf_err;
  }

  // init the send buf
  send_buf_ = ibv_reg_mr(res->pd_, buf, max_buf_size,
                         IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
  if (unlikely(!send_buf_)) {
    LOG(WARNING)<< "Unable to register mr\n";
    goto send_mr_err;
  }

  slot_head_ = slot_tail_ = 0;
  pending_msg_ = to_signaled_send_msg_ = to_signaled_w_r_msg_ = 0;
  max_unsignaled_msg_ =
  MAX_UNSIGNALED_MSG > max_pending_msg_ ? max_pending_msg_ : MAX_UNSIGNALED_MSG;
  // because we're using uint16_t to represent currently to_be_signalled msg
  assert(max_unsignaled_msg_ <= USHRT_MAX);
  full_ = false;

  {
    ibv_qp_init_attr attr = { };
    attr.srq = res->srq_;
    attr.send_cq = res->cq_;
    attr.recv_cq = res->cq_;
    attr.qp_type = IBV_QPT_RC;
    attr.cap.max_send_wr = max_pending_msg_;
    attr.cap.max_send_sge = 1;
    attr.sq_sig_all = 0;
//    attr.cap.max_recv_wr = 1;
//    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = MAX_RDMA_INLINE_SIZE;

    qp_ = ibv_create_qp(res->pd_, &attr);
  }
  if (unlikely(!qp_)) {
    LOG(WARNING)<< "Unable to create QP " << errno << strerror(errno);
    goto clean_mr;
  }

  {
    // set qp to init status
    ibv_qp_attr qattr = { };
    qattr.qp_state = IBV_QPS_INIT;
    qattr.pkey_index = 0;
    qattr.port_num = res->ibport_;
    qattr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
        | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ;

    if (ibv_modify_qp(
        qp_, &qattr,
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
      LOG(WARNING)<< "Unable to modify qp to init status\n";
      goto clean_qp;
    }
  }

  {
    ibv_qp_init_attr attr = { };
    ibv_qp_attr qattr = { };
    if (ibv_query_qp(qp_, &qattr, IBV_QP_CAP, &attr)) {
      LOG(WARNING)<< "Unable to query qp";
      goto clean_qp;
    }
    LOG(INFO)<< "qattr.cap.max_inline_data = " << attr.cap.max_inline_data
        << "attr.cap.max_inline_data = " << qattr.cap.max_inline_data;
    if (attr.cap.max_inline_data == 0) {
      LOG(INFO)<< "Do NOT support inline data";
      MAX_RDMA_INLINE_SIZE = 0;
    }
  }

  return;

  clean_qp: ibv_destroy_qp(qp_);
  clean_mr: ibv_dereg_mr(send_buf_);
  send_mr_err: free(buf);
  send_buf_err: throw RDMA_CONTEXT_EXCEPTION;
}

NetContext* RdmaNet::CreateNetContext(const node_id_t& id) {
  bool exist = false;
  RdmaNetContext* ctx = CreateRdmaNetContext(id, exist);

  if (!exist) {
    vector<std::string> ip_port;
    Split(id, ip_port);
    assert(ip_port.size() == 2);
    ctx->ExchConnParam(cur_node_, ip_port[0].c_str(), atoi(ip_port[1].c_str()));
  }
  return ctx;
}

RdmaNetContext* RdmaNet::CreateRdmaNetContext(const node_id_t& id,
                                              bool& exist) {
  net_lock_.lock();
  RdmaNetContext* ctx = nullptr;
  if (id == this->cur_node_) {
    LOG(INFO)<< "will not connect itself\n";
    exist = true;
    net_lock_.unlock();
    return ctx;
  }

  ctx = FindContextID(id);
  if (ctx) {
    LOG(INFO)<< "already exist\n";
    exist = true;
    net_lock_.unlock();
    return ctx;
  }
  ctx = resource_->NewRdmaNetContext(id);
  assert(qpCliMap_.count(ctx->GetQP()) == 0);
  assert(netmap_.count(id) == 0);
  qpCliMap_[ctx->GetQP()] = ctx;
  netmap_[id] = ctx;

  LOG(INFO)<< "create qp " << ctx->GetQP() << std::endl;

  net_lock_.unlock();
  return ctx;
}

int RdmaNetContext::SetRemoteConnParam(const char *conn) {
  int ret;
  uint32_t rlid, rpsn, rqpn, rrkey;
  uint64_t rvaddr;

  /* conn should be of the format "lid:qpn:psn:rkey:vaddr" */
  sscanf(conn, "%x:%x:%x:%x:%lx", &rlid, &rqpn, &rpsn, &rrkey, &rvaddr);
  this->rkey_ = rrkey;
  this->vaddr_ = rvaddr;

  /* modify qp to RTR state */
  {
    ibv_qp_attr attr = { };  // zero init the POD value (DON'T FORGET!!!!)
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_2048;
    attr.dest_qp_num = rqpn;
    attr.rq_psn = rpsn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = rlid;
    attr.ah_attr.src_path_bits = 0;
    // attr.ah_attr.sl = 1;
    attr.ah_attr.port_num = resource_->ibport_;

    ret = ibv_modify_qp(
        this->qp_,
        &attr,
        IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN
            | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
    if (unlikely(ret)) {
      LOG(WARNING)<< "Unable to modify qp to RTR " << errno <<
      strerror(errno) << std::endl;
      return 1;
    }
  }

  {
    ibv_qp_attr attr = { };
    /* modify qp to rts state */
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = this->resource_->psn_;
    attr.max_rd_atomic = 1;
    ret = ibv_modify_qp(
        this->qp_,
        &attr,
        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY
            | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);

    if (unlikely(ret)) {
      LOG(WARNING)<< "Unable to modify qp to RTS state\n";
      return 1;
    }
  }

  // resource_->PostRecv(max_pending_msg);
  resource_->PostRecv(MAX_WORKER_RECV_MSG);
  return 0;
}

const char* RdmaNetContext::GetRdmaConnString() {
  if (!msg_) {
    msg_ = static_cast<char*>(malloc(WORKER_RDMA_CONN_STRLEN + 1));
  } else {
    return msg_;
  }

  if (unlikely(!msg_)) {
    LOG(WARNING)<< "Unable to allocate memory\n";
    goto out;
  }

  /*
   * we use RDMA send/recv to do communication
   * for communication among workers, we also allow direct access to the whole memory space so that we expose the base addr and rkey
   */
  snprintf(msg_, WORKER_RDMA_CONN_STRLEN + 1, "%04x:%08x:%08x:%08x:%016lx",
          this->resource_->portAttribute_.lid, this->qp_->qp_num,
          this->resource_->psn_, 0, 0L);
  out:
  LOG(INFO)<< "msg = " << msg_ << std::endl;
  return msg_;
}

char* RdmaNetContext::GetFreeSlot_() {
  int avail = RMINUS(slot_tail_, slot_head_, max_pending_msg_);
  if (!avail && !full_)
    avail = max_pending_msg_;
  if (avail <= 0 || pending_msg_ >= max_pending_msg_) {
    LOG(INFO)<< "all the slots are busy\n";
    return nullptr;
  }

  char* s = static_cast<char*>(send_buf_->addr) + slot_head_ * MAX_REQUEST_SIZE;
  if (++slot_head_ == max_pending_msg_)
    slot_head_ = 0;
  if (slot_head_ == slot_tail_)
    full_ = true;
  return s;
}

char* RdmaNetContext::GetFreeSlot() {
  lock();
  char* s = GetFreeSlot_();
  unlock();  // we delay the unlock to after-send
  return s;
//  char* s = (char*)malloc(MAX_REQUEST_SIZE);
//  return s;
}

bool RdmaNetContext::IsRegistered(const void* addr) {
  return ((uintptr_t) addr >= (uintptr_t) send_buf_->addr)
      && ((uintptr_t) addr < (uintptr_t) send_buf_->addr + send_buf_->length);
}

ssize_t RdmaNetContext::Rdma(RdmaRequest& r) {
  ssize_t ret = Rdma(r.op, r.src, r.len, r.id, r.signaled, r.dest, r.imm,
                     r.oldval, r.newval);
  if (r.op == IBV_WR_SEND && r.src) {
    free(const_cast<void*>(r.src));
  }
  return ret;
}

ssize_t RdmaNetContext::Rdma(ibv_wr_opcode op, const void* src, size_t len,
                             unsigned int id, bool signaled, void* dest,
                             uint32_t imm, uint64_t oldval, uint64_t newval) {
  int ret = len;
  struct ibv_sge sge_list = { };
  struct ibv_send_wr wr = { };
  if (pending_msg_ >= max_pending_msg_) {
    // TODO(zhanghao): add the send request to the waiting queue
    LOG(INFO)<< "Rdma device is busy; will try later";
    if (op == IBV_WR_SEND && src) {
      char* copy = static_cast<char*>(malloc(len));
      memcpy(copy, src, len);
      src = copy;
    }
    pending_requests_.push(RdmaRequest {op, src, len, id, signaled, dest, imm,
          oldval, newval});
    assert(
        pending_requests_.back().op == op && pending_requests_.back().src == src
        && pending_requests_.back().len == len
        && pending_requests_.back().id == id
        && pending_requests_.back().signaled == signaled
        && pending_requests_.back().dest == dest
        && pending_requests_.back().imm == imm
        && pending_requests_.back().oldval == oldval
        && pending_requests_.back().newval == newval);

    // return -1;
    return len;
  }

  if (op == IBV_WR_SEND) {
    if (unlikely(!IsRegistered(src) && len > MAX_RDMA_INLINE_SIZE)) {
      if (len > MAX_REQUEST_SIZE) {
        LOG(WARNING)<< "len = " << len << " MAX_REQUEST_SIZE = "
            << MAX_REQUEST_SIZE << " src = " << static_cast<const char*>(src);
        assert(false);
      }
      char* sbuf = GetFreeSlot_();
      assert(sbuf);
      memcpy(sbuf, src, len);
      // free((void*)src);
      sge_list.addr = (uintptr_t) sbuf;
      pending_send_msg_++;
    } else {
      if (IsRegistered(src)) {
        pending_send_msg_++;
      }
      sge_list.addr = (uintptr_t) src;
    }
    sge_list.lkey = send_buf_->lkey;

  } else if (op == IBV_WR_RDMA_WRITE || op == IBV_WR_RDMA_WRITE_WITH_IMM) {
    sge_list.addr = (uintptr_t) src;
    sge_list.lkey = resource_->bmr_->lkey;
    wr.wr.rdma.remote_addr = (uintptr_t) dest;
    wr.wr.rdma.rkey = rkey_;
    if (op == IBV_WR_RDMA_WRITE_WITH_IMM) {
      wr.imm_data = htonl(imm);
    }
  } else if (op == IBV_WR_RDMA_READ) {
    sge_list.addr = (uintptr_t) dest;
    sge_list.lkey = resource_->bmr_->lkey;
    wr.wr.rdma.remote_addr = (uintptr_t) src;
    wr.wr.rdma.rkey = rkey_;
  } else {
    LOG(WARNING) << "unsupported RDMA OP";
    return -1;
  }

  sge_list.length = len;

  wr.opcode = op;
  wr.wr_id = -1;
  wr.sg_list = &sge_list;
  wr.num_sge = len == 0 ? 0 : 1;
  wr.next = nullptr;
  wr.send_flags = 0;
  if (len <= MAX_RDMA_INLINE_SIZE)
    wr.send_flags = IBV_SEND_INLINE;

  pending_msg_++;
  uint16_t curr_to_signaled_send_msg = pending_send_msg_
      - to_signaled_send_msg_;
  uint16_t curr_to_signaled_w_r_msg = pending_msg_ - pending_send_msg_
      - to_signaled_w_r_msg_;
  // we signal msg for every max_unsignaled_msg
  if (unlikely(
      curr_to_signaled_send_msg + curr_to_signaled_w_r_msg
          == max_unsignaled_msg_ || signaled)) {
    wr.send_flags |= IBV_SEND_SIGNALED;

    to_signaled_send_msg_ += curr_to_signaled_send_msg;
    assert(to_signaled_send_msg_ == pending_send_msg_);
    to_signaled_w_r_msg_ += curr_to_signaled_w_r_msg;
    assert(to_signaled_send_msg_ + to_signaled_w_r_msg_ == pending_msg_);
    assert(
        curr_to_signaled_send_msg + curr_to_signaled_w_r_msg
            <= max_unsignaled_msg_);

    /*
     * higher to lower: send_msg(16), w_r_msg(16), workid(32)
     */
    /*
     * FIXME: only such work requests have their wr_id set, but it seems
     * that the wr_id of each completed work request will be checked
     * against to see if there are any pending invalidate WRs.
     */
    wr.wr_id = (id & HALF_BITS)
        + ((uint64_t) (curr_to_signaled_send_msg & QUARTER_BITS) << 48)
        + ((uint64_t) (curr_to_signaled_w_r_msg & QUARTER_BITS) << 32);
  }

  struct ibv_send_wr *bad_wr;
  if (ibv_post_send(qp_, &wr, &bad_wr)) {
    LOG(WARNING)<< "ibv_post_send failed: " << errno << strerror(errno);
    return -2;
  }
  if (op == IBV_WR_SEND
      && !IsRegistered(reinterpret_cast<void*>(sge_list.addr))) {
    assert(wr.send_flags & IBV_SEND_INLINE);
    // free((void*)sge_list.addr);
  }
  return ret;
}

ssize_t RdmaNetContext::SendGeneric(const void* ptr, size_t len,
                                    unsigned int id, bool signaled) {
  lock();
  // lock(); // we already lock when getting the send buf
  ssize_t ret = Rdma(IBV_WR_SEND, ptr, len, id, signaled);
  unlock();
  return ret;
}

ssize_t RdmaNetContext::WriteGeneric(raddr dest, raddr src, size_t len,
                               unsigned int id, bool signaled) {
  lock();
  ssize_t ret = Rdma(IBV_WR_RDMA_WRITE, src, len, id, signaled, dest);
  unlock();
  return ret;
}

ssize_t RdmaNetContext::WriteWithImmGeneric(raddr dest, raddr src, size_t len,
                                      uint32_t imm, unsigned int id,
                                      bool signaled) {
  lock();
  ssize_t ret = Rdma(IBV_WR_RDMA_WRITE_WITH_IMM, src, len, id, signaled, dest,
                     imm);
  unlock();
  return ret;
}

ssize_t RdmaNetContext::ReadGeneric(raddr dest, raddr src, size_t len,
                              unsigned int id, bool signaled) {
  lock();
  ssize_t ret = Rdma(IBV_WR_RDMA_READ, src, len, id, signaled, dest);
  unlock();
  return ret;
}

ssize_t RdmaNetContext::CasGeneric(raddr src, uint64_t oldval, uint64_t newval,
                             unsigned int id, bool signaled) {
  lock();
  ssize_t ret = Rdma(IBV_WR_ATOMIC_CMP_AND_SWP, src, sizeof(uint64_t), id,
                     signaled, nullptr, 0, oldval, newval);
  unlock();
  return ret;
}

void RdmaNetContext::ProcessPendingRequests(int n) {
  // process pending rdma requests
  int size = pending_requests_.size();
  // we must iterate all the current pending requests
  // in order to ensure the original order
  int i = 0, j = -1;

  for (i = 0; i < n && i < size; i++) {
    RdmaRequest& r = pending_requests_.front();
    int ret = Rdma(r);
    assert(ret != -1);
    pending_requests_.pop();
  }

//  for(;i < size; i++) {
//    RdmaRequest& r = pending_requests_.front();
//    pending_requests_.push(r);
//    pending_requests_.pop();
//  }
}

unsigned int RdmaNetContext::SendComp(ibv_wc& wc) {
  unsigned int id = wc.wr_id & HALF_BITS;
  uint16_t curr_to_signaled_send_msg = wc.wr_id >> 48;
  uint16_t curr_to_signaled_w_r_msg = wc.wr_id >> 32 & QUARTER_BITS;

  lock();
  slot_tail_ += curr_to_signaled_send_msg;
  if (slot_tail_ >= max_pending_msg_)
    slot_tail_ -= max_pending_msg_;

  to_signaled_send_msg_ -= curr_to_signaled_send_msg;
  to_signaled_w_r_msg_ -= curr_to_signaled_w_r_msg;
  pending_msg_ -= (curr_to_signaled_send_msg + curr_to_signaled_w_r_msg);
  pending_send_msg_ -= curr_to_signaled_send_msg;

  assert(curr_to_signaled_send_msg + curr_to_signaled_w_r_msg
          <= max_unsignaled_msg_);
  assert(to_signaled_send_msg_ + to_signaled_w_r_msg_ <= pending_msg_);

  if (full_ && curr_to_signaled_send_msg)
    full_ = false;

  ProcessPendingRequests(curr_to_signaled_send_msg + curr_to_signaled_w_r_msg);

  unlock();
  return id;
}

unsigned int RdmaNetContext::WriteComp(ibv_wc& wc) {
  return SendComp(wc);
}

char* RdmaNetContext::RecvComp(ibv_wc& wc) {
  // FIXME: thread-safe
  // what if others grab this slot before the current thread finish its job
  char* ret = resource_->GetSlot(wc.wr_id);
  // resource_->ClearSlot(wc.wr_id);
  resource_->recv_posted_ -= 1;
  return ret;
}

int RdmaNetContext::ExchConnParam(node_id_t cur_node, const char* ip,
                                  int port) {
  // open the socket to exch rdma resouces
  char neterr[ANET_ERR_LEN];
  int sockfd = anetTcpConnect(neterr, const_cast<char *>(ip), port);
  if (sockfd < 0) {
    LOG(WARNING) << "Connecting to " << ip << ":" << port << ":" << neterr;
    exit(1);
    // return -1;
  }

  std::string conn;
  conn.append(cur_node);
  conn.append(";");
  conn.append(GetRdmaConnString());

  int conn_len = conn.length();
  if (write(sockfd, conn.c_str(), conn_len) != conn_len) {
    return -1;
  }

  char msg[conn_len + 1];
  /* waiting for server's response */
  int n = read(sockfd, msg, conn_len);
  if (n <= 0) {
    LOG(WARNING) << "Failed to read conn param from server "
        << strerror(errno) << " read " << n << "bytes)";
    return -1;
  }
  msg[n] = '\0';

  std::vector<std::string> cv;
  Split(msg, cv, ';');
  assert(cv.size() == 2);

  std::vector<std::string> ip_port;
  Split(cv[0], ip_port);
  assert(ip_port.size() == 2);
  assert(ip_port[0].compare(ip) == 0 && atoi(ip_port[1].c_str()) == port);

  SetRemoteConnParam(cv[1].c_str());

  close(sockfd);
  return 0;
}

RdmaNetContext::~RdmaNetContext() {
  ibv_destroy_qp(qp_);
  ibv_dereg_mr(send_buf_);
  free(send_buf_->addr);
  free(msg_);
}

RdmaNet::RdmaNet(const node_id_t& id, int nthreads)
    : Net(id),
      work_(ioService_), nthreads_(nthreads) {
  resource_ = RdmaNetResourceFactory::Instance()->newRdmaNetResource();
  // resource_ = RdmaNetResourceFactory::Instance()->getRdmaNetResource();
  for (int i = 0; i < nthreads_; i++) {
    threadpool_.create_thread(
        boost::bind(&boost::asio::io_service::run, &ioService_));
  }
  StartService(id, resource_);
}

RdmaNet::~RdmaNet() {
  // delete resource_;
  close(sockfd_);
  LOG(INFO) << "delete RdmaNet" << std::endl;
}

void RdmaNet::StartService(const node_id_t& id, RdmaNetResource* res) {
  // create the event loop
  el_ = aeCreateEventLoop(EVENTLOOP_FDSET_INCR);

  // open the socket for listening to the connections
  // from workers to exch rdma resouces
  char neterr[ANET_ERR_LEN];
  vector<std::string> ip_port;
  Split(id, ip_port);
  assert(ip_port.size() == 2);
  char* bind_addr = const_cast<char *>(ip_port[0].c_str());
  int port = atoi(ip_port[1].c_str());
  sockfd_ = anetTcpServer(neterr, port, bind_addr, TCP_BACKLOG);
  if (sockfd_ < 0) {
    LOG(WARNING) << "Opening port " << port
        << " (bind_addr " << bind_addr << "): " << neterr;
    assert(false);
  }

  // register tcp event for rdma parameter exchange
  if (sockfd_ > 0 &&
      aeCreateFileEvent(el_, sockfd_, AE_READABLE, TcpHandle, this) == AE_ERR) {
    LOG(WARNING) << "Unrecoverable error creating sockfd file event.";
  }

  // register rdma event
  if (resource_->GetChannelFd() > 0 &&
      aeCreateFileEvent(el_, resource_->GetChannelFd(),
                        AE_READABLE, RdmaHandle, this) == AE_ERR) {
    LOG(WARNING)<< "Unrecoverable error creating sockfd file event.";
  }
  this->st_ = new std::thread(startEventLoop, el_);
}

void RdmaNet::CbHandler(RdmaNet* net, const node_id_t& source,
                        const void* msg, size_t size,
                        RdmaNetResource* resource, uint64_t wr_id) {
  assert(net->cb_);
  (*(net->cb_))(msg, size, source);
  // resource->ClearSlot(wr_id);
  int n = resource->PostRecvSlot(wr_id);
  // Assert(n == 1);
}

void RdmaNet::ProcessRdmaRequest() {
  int ne;
  ibv_wc wc[MAX_CQ_EVENTS];
  ibv_cq *cq = resource_->GetCompQueue();
  RdmaNetContext *ctx;
  // uint32_t immdata, id;
  uint32_t immdata;
  int recv_c = 0;

  /*
   * to get notified in the event-loop,
   * we need ibv_req_notify_cq -> ibv_get_cq_event -> ibv_ack_cq_events seq -> ibv_req_notify_cq!!
   */
  if (likely(resource_->GetCompEvent())) {
    do {
      ne = ibv_poll_cq(cq, MAX_CQ_EVENTS, wc);
      if (unlikely(ne < 0)) {
        LOG(WARNING)<< "Unable to poll cq\n";
        goto out;
      }

      for (int i = 0; i < ne; ++i) {
        if (wc[i].status != IBV_WC_SUCCESS) {
          LOG(WARNING)<< "Completion with error, op = " << wc[i].opcode << " ("
              << wc[i].status << ":" << ibv_wc_status_str(wc[i].status) << ")";
          continue;
        }

        /*
         * FIXME
         * 1) check whether the wc is initiated from the local host (ibv_post_send)
         * 2) if caused by ibv_post_send, then clear some stat used for selective signal
         *    otherwise, find the client, check the op code, process, and response if needed.
         */
        ctx = FindContext(wc[i].qp_num);
        if (unlikely(!ctx)) {
          LOG(WARNING) << "cannot find the corresponding client for qp"
              << wc[i].qp_num;
          continue;
        }

        switch (wc[i].opcode) {
          case IBV_WC_SEND:
          ctx->SendComp(wc[i]);
          // send check initiated locally
          // CompletionCheck(id);
          // TODO(zhanghao): send out the waiting request
          break;
          case IBV_WC_RDMA_WRITE:
          // update pending_msg_
          LOG(WARNING) << "not supported for IBV_WC_RDMA_WRITE";
          ctx->WriteComp(wc[i]);
          // write check initiated locally
          // CompletionCheck(id);
          // TODO(zhanghao): send out the waiting request
          break;
          case IBV_WC_RECV: {
            char* msg = ctx->RecvComp(wc[i]);
            msg[wc[i].byte_len] = '\0';
            recv_c++;

            if (cb_) {
              ioService_.post(boost::bind(CbHandler, this, ctx->GetID(),
                         msg, wc[i].byte_len, resource_, wc[i].wr_id));
            } else {
              int n = resource_->PostRecvSlot(wc[i].wr_id);
              // assert(n == 1);
            }
            //  if(ctx->cb_) {
            //    ctx->cb_(msg, wc[i].byte_len, ctx->upstream_handle_);
            //  }
            //  // resource_->ClearSlot(wc[i].wr_id);
            //
            //    int n = resource_->PostRecvSlot(wc[i].wr_id);
            //    assert(n == 1);
            break;
          }
          case IBV_WC_RECV_RDMA_WITH_IMM: {
            LOG(WARNING) << "not supported for IBV_WC_RECV_RDMA_WITH_IMM";
            break;
          }
          default:
            LOG(WARNING) << "unknown opcode received " << wc[i].opcode;
            break;
        }
      }
    }while (ne == MAX_CQ_EVENTS);

    //  if(recv_c) {
    //    //epicAssert(recv_c == resource_->ClearRecv(low, high));
    //    int n = resource_->PostRecv(recv_c);
    //    assert(recv_c == n);
    //  }
  }

  out: return;
}

ssize_t RdmaNetContext::Send(const void* ptr, size_t len, CallBack* func) {
  static int counter = 0;
  counter++;
  int ret = SendGeneric(ptr, len);
  return ret;
}

void TcpHandle(aeEventLoop *el, int fd, void *data, int mask) {
  assert(data != nullptr);
  RdmaNet *net = static_cast<RdmaNet*>(data);
  char msg[MAX_CONN_STRLEN_G + 1];
  int n;
  const char *p;
  RdmaNetContext *ctx;
  char neterr[ANET_ERR_LEN];
  char cip[IP_STR_LEN];
  int cfd, cport;
  std::string conn;
  node_id_t id;
  vector<std::string> cv;
  bool exist = false;

  cfd = anetTcpAccept(neterr, fd, cip, sizeof(cip), &cport);
  if (cfd == ANET_ERR) {
    if (errno != EWOULDBLOCK)
    LOG(WARNING)<< "Accepting client connection: " << neterr << std::endl;
    return;
  }
  LOG(INFO) << "Accepted " << cip << ":" << cport << std::endl;

  if (mask & AE_READABLE) {
    n = read(cfd, msg, sizeof msg);
    if (unlikely(n <= 0)) {
      LOG(WARNING)<< "Unable to read conn string\n";
      goto out;
    }
    msg[n] = '\0';
    LOG(INFO) << "conn string " << msg;
}

  Split(msg, cv, ';');
  assert(cv.size() == 2);
  if (unlikely(!(ctx =
      static_cast<RdmaNetContext*>(net->CreateRdmaNetContext(cv[0], exist))))) {
    goto out;
  }
  if (!exist) {
    ctx->SetRemoteConnParam(cv[1].c_str());
  }

  conn.append(net->GetNodeID());
  conn.append(";");
  conn.append(ctx->GetRdmaConnString());

  n = write(cfd, conn.c_str(), conn.size());

  if (unlikely(n < conn.size())) {
    LOG(WARNING) << "Unable to send conn string\n";
    net->RemoveContext(ctx);
  }
  out: close(cfd);
}

void RdmaHandle(aeEventLoop *el, int fd, void *data, int mask) {
  (static_cast<RdmaNet*>(data))->ProcessRdmaRequest();
}

}  // namespace ustore

#endif
