// Copyright (c) 2017 The Ustore Authors.

#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <errno.h>
#include <sys/epoll.h>
#include "utils/utils.h"
#include "http/net.h"
#include "http/event.h"

namespace ustore {

int EventLoop::EpollAddEvent(int fd, int mask) {
  struct epoll_event ee;
  /* If the fd was already monitored for some event, we need a MOD
   * operation. Otherwise we need an ADD operation. */
  int op = events_[fd].mask == NONE ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;

  ee.events = 0;
  mask |= events_[fd].mask;  // Merge old events
  if (mask & READABLE)
    ee.events |= EPOLLIN;
  if (mask & WRITABLE)
    ee.events |= EPOLLOUT;
  if (mask & EDEGE)
    ee.events |= EPOLLET;
  ee.data.u64 = 0;  // avoid valgrind warning
  ee.data.fd = fd;
  if (epoll_ctl(estate_->epfd, op, fd, &ee) == -1)
    return -1;
  return 0;
}

void EventLoop::EpollDelEvent(int fd, int delmask) {
  struct epoll_event ee;
  int mask = events_[fd].mask & (~delmask);

  ee.events = 0;
  if (mask & READABLE)
    ee.events |= EPOLLIN;
  if (mask & WRITABLE)
    ee.events |= EPOLLOUT;
  ee.data.u64 = 0;  // avoid valgrind warning
  ee.data.fd = fd;
  if (mask != NONE) {
    epoll_ctl(estate_->epfd, EPOLL_CTL_MOD, fd, &ee);
  } else {
    /* Note, Kernel < 2.6.9 requires a non null event pointer even for
     * EPOLL_CTL_DEL. */
    epoll_ctl(estate_->epfd, EPOLL_CTL_DEL, fd, &ee);
  }
}

int EventLoop::EpollPoll(int timeout) {
  int retval, numevents = 0;
  retval = epoll_wait(estate_->epfd, estate_->events, setsize_, timeout);

  if (retval > 0) {
    int j;
    numevents = retval;
    for (j = 0; j < numevents; j++) {
      int mask = 0;
      struct epoll_event *e = estate_->events + j;
      if (likely(e->events & EPOLLIN))
        mask |= READABLE;
      if (e->events & EPOLLOUT)
        mask |= WRITABLE;
      if (e->events & EPOLLERR)
        mask |= WRITABLE;
      if (e->events & EPOLLHUP)
        mask |= WRITABLE;
      fired_[j].fd = e->data.fd;
      fired_[j].mask = mask;
    }
  }
  return numevents;
}

EventLoop::EventLoop(int setsize) {
  setsize += 10;  // in case some initial fd are in use
  this->events_.resize(setsize);
  this->fired_.resize(setsize);
  if (this->events_.size() != setsize || this->fired_.size() != setsize)
    return;
  this->setsize_ = setsize;
  this->stop_ = 0;
  this->maxfd_ = -1;
  for (int i = 0; i < setsize; i++)
    this->events_[i].mask = NONE;

  estate_ = new EpollState();
  if (!estate_) return;
  estate_->events = (struct epoll_event*)
      malloc(sizeof(struct epoll_event) * this->setsize_);
  if (!estate_->events) {
    free(estate_);
    return;
  }
  estate_->epfd = epoll_create(1024);  // 1024 is just a hint for the kernel
  if (estate_->epfd == -1) {
    free(estate_->events);
    free(estate_);
    return;
  }
}

// Resize the maximum set size of the event loop
int EventLoop::ResizeSetSize(int setsize) {
  if (setsize == this->setsize_)
    return ST_SUCCESS;
  if (this->maxfd_ >= setsize)
    return ST_ERROR;

  estate_->events = (struct epoll_event*)
      realloc(estate_->events, sizeof(struct epoll_event) * setsize);
  this->events_.resize(setsize);
  this->fired_.resize(setsize);

  this->setsize_ = setsize;

  // new slots are initialized with NONE mask
  for (int i = this->maxfd_ + 1; i < setsize; i++)
    this->events_[i].mask = NONE;
  return ST_SUCCESS;
}

EventLoop::~EventLoop() {
  close(estate_->epfd);
  free(estate_->events);
  free(estate_);
}

void EventLoop::Stop() {
  this->stop_ = 1;
}

int EventLoop::CreateFileEvent(int fd, int mask, FileProc *proc,
                                   void *client_data) {
//  if (fd >= this->setsize_) {
//    if(ResizeSetSize(this->setsize_ * 2) != ST_SUCCESS) {
//      LOG(LOG_WARNING, "cannot resize the event loop");
//      return ST_ERROR;
//    }
//  }

  if (fd >= this->setsize_) {
    errno = ERANGE;
    return ST_ERROR;
  }
  FileEvent *fe = &this->events_[fd];

  if (EpollAddEvent(fd, mask) == -1)
    return ST_ERROR;
  fe->mask |= mask;
  if (mask & READABLE)
    fe->rfile_proc = proc;
  if (mask & WRITABLE)
    fe->wfile_proc = proc;
  fe->client_data = client_data;
  if (fd > this->maxfd_)
    this->maxfd_ = fd;
  return ST_SUCCESS;
}

void EventLoop::DeleteFileEvent(int fd, int mask) {
  if (fd >= this->setsize_) return;

  FileEvent *fe = &this->events_[fd];
  if (fe->mask == NONE) return;

  EpollDelEvent(fd, mask);
  fe->mask = fe->mask & (~mask);
  if (fd == this->maxfd_ && fe->mask == NONE) {
    /* Update the max fd */
    int j;

    for (j = this->maxfd_ - 1; j >= 0; j--)
      if (this->events_[j].mask != NONE)
        break;
    this->maxfd_ = j;
  }
}

int EventLoop::GetFileEvents(int fd) {
  if (fd >= this->setsize_) return 0;

  FileEvent *fe = &this->events_[fd];
  return fe->mask;
}

int EventLoop::ProcessEvents(int timeout) {
  int processed = 0;

  if (likely(this->maxfd_ != -1)) {
    int numevents = EpollPoll(timeout);
    for (int j = 0; j < numevents; j++) {
      FileEvent *fe = &this->events_[this->fired_[j].fd];
      int mask = this->fired_[j].mask;
      int fd = this->fired_[j].fd;
      int rfired = 0;

      if (likely(fe->mask & mask & READABLE)) {
        rfired = 1;
        fe->rfile_proc(this, fd, fe->client_data, mask);
      }

      if (fe->mask & mask & WRITABLE) {
        if (!rfired || fe->wfile_proc != fe->rfile_proc)
          fe->wfile_proc(this, fd, fe->client_data, mask);
      }
      processed++;
    }
  } else {
    usleep(10000);
  }
  return processed;  // return the number of processed events
}

void EventLoop::Start() {
  // start epoll
  this->stop_ = 0;
  while (!this->stop_) {
    ProcessEvents();
  }
}
}  // namespace ustore
