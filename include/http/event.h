// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_USTORE_HTTP_EVENT_H_
#define USTORE_USTORE_HTTP_EVENT_H_

#include <time.h>
#include <vector>
using std::vector;

namespace ustore {
#define NONE 0
#define READABLE 1  // EPOLLIN
#define WRITABLE 2  // EPOLLOUT | EPOLLERR | EPOLLHUP
#define EDEGE 4

#define DEFAULT_TIMEOUT 0  // or 0: NON_BLOCKING, -1: BLOCKING
#define DEFAULT_MAX_FD 10000

struct EventLoop;

// Types and data structures
typedef void FileProc(struct EventLoop *event_loop, int fd, void *client_data,
                        int mask);

// File event structure
struct FileEvent {
  int mask;  // one of AE_(READABLE|WRITABLE)
  FileProc *rfile_proc;
  FileProc *wfile_proc;
  void *client_data;
};

// A fired event
struct FiredEvent {
  int fd;
  int mask;
};

struct EpollState {
  int epfd;
  struct epoll_event *events;
};

/*
 * event loop wrapper of epoll for easy use
 */
class EventLoop {
 public:
  explicit EventLoop(int setsize = DEFAULT_MAX_FD);
  ~EventLoop();

  void Start();
  void Stop();

  int CreateFileEvent(int fd, int mask, FileProc *proc, void *client_data);
  void DeleteFileEvent(int fd, int mask);
  int GetFileEvents(int fd);

 private:
  int ResizeSetSize(int setsize);

  // return number of processed events
  int ProcessEvents(int timeout = DEFAULT_TIMEOUT);
  int EpollAddEvent(int fd, int mask);
  void EpollDelEvent(int fd, int delmask);
  int EpollPoll(int timeout = DEFAULT_TIMEOUT);

  int maxfd_;  // highest file descriptor currently registered
  int setsize_;  // max number of file descriptors tracked
  vector<FileEvent> events_;  // Registered events
  vector<FiredEvent> fired_;  // Fired events
  volatile int stop_;
  EpollState* estate_;
};
}  // namespace ustore

#endif  // USTORE_USTORE_HTTP_EVENT_H_
