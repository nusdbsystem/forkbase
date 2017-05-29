// Copyright (c) 2017 The Ustore Authors.

#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netdb.h>
#include <errno.h>
#include <arpa/inet.h>
#include <string.h>
#include "utils/logging.h"
#include "http/net.h"

namespace ustore {

int ServerSocket::Listen() {
  int rv;
  char cport[6];  // max 65535
  addrinfo hints, *servinfo, *p;

  snprintf(cport, 6, "%d", port_);
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;  // No effect if bindaddr != nullptr

  if ((rv = getaddrinfo(ip_.size() == 0 ? nullptr : ip_.c_str(), cport, &hints,
                        &servinfo)) != 0) {
    LOG(WARNING)<< "getaddrinfo failed: %s", gai_strerror(rv);
    return ST_ERROR;
  }

  for (p = servinfo; p != nullptr; p = p->ai_next) {
    if ((fd_ = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
      continue;

    if (bind(fd_, p->ai_addr, p->ai_addrlen) == -1) {
      LOG(FATAL)<< "bind error: " << strerror(errno);
      close(fd_);
      fd_ = -1;
      freeaddrinfo(servinfo);
      return ST_ERROR;
    }

    if (listen(fd_, backlog_) == -1) {
      LOG(FATAL)<< "listen: " << strerror(errno);
      close(fd_);
      fd_ = -1;
      freeaddrinfo(servinfo);
      return ST_ERROR;
    }
    freeaddrinfo(servinfo);
    return ST_SUCCESS;
  }
  if (p == nullptr) {
    LOG(FATAL)<< "unable to bind socket";
  }

  fd_ = -1;
  freeaddrinfo(servinfo);
  return ST_ERROR;
}

ClientSocket* ServerSocket::Accept() {
  sockaddr_storage sa;
  socklen_t salen = sizeof(sa);

  int max_ip_len = 46;
  char cip[max_ip_len];
  int cfd, cport;

  while (true) {
    cfd = accept(fd_, reinterpret_cast<sockaddr*>(&sa), &salen);
    if (cfd == -1) {
      if (errno == EINTR) {
        continue;
      } else {
        LOG(WARNING)<< "accept: " << strerror(errno);
        return nullptr;
      }
    }
    break;
  }

  if (sa.ss_family == AF_INET) {
    sockaddr_in* s = reinterpret_cast<sockaddr_in*>(&sa);
    inet_ntop(AF_INET, &(s->sin_addr), cip, max_ip_len);
    cport = ntohs(s->sin_port);
  } else {
    LOG(WARNING)<< "not supported IPV6";
    return nullptr;
  }

  LOG(INFO)<< "accept " << cip << ":" << cport << " (socket = " << cfd << ")";
  return new ClientSocket(string(cip), cport, cfd);
}

int ClientSocket::Connect() {
  int rv;
  char portstr[6];  // strlen("65535") + 1;
  addrinfo hints, *servinfo, *bservinfo, *p, *b;

  snprintf(portstr, sizeof(portstr), "%d", port_);
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if ((rv = getaddrinfo(ip_.c_str(), portstr, &hints, &servinfo)) != 0) {
    LOG(FATAL)<< gai_strerror(rv) << ","
        << strerror(errno) << "(" << errno << ")";
    return ST_ERROR;
  }
  for (p = servinfo; p != NULL; p = p->ai_next) {
    /* Try to create the socket and to connect it.
     * If we fail in the socket() call, or on connect(), we retry with
     * the next entry in servinfo. */
    if ((fd_ = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
      continue;

    if (connect(fd_, p->ai_addr, p->ai_addrlen) == -1) {
      /* If the socket is non-blocking, it is ok for connect() to
       * return an ST_INPROCESS error here. */
      LOG(WARNING)<< "failed to connect";
      if (errno == EINPROGRESS) {
        freeaddrinfo(servinfo);
        return ST_INPROCESS;
      }
      close(fd_);
      fd_ = -1;
      continue;
    }

    /* If we ended an iteration of the for loop without errors, we
     * have a connected socket. Let's return to the caller. */
    freeaddrinfo(servinfo);
    return ST_SUCCESS;
  }
  if (p == nullptr)
    LOG(WARNING)<< "creating socket: " << strerror(errno);

  if (fd_ != -1) {
    close(fd_);
    fd_ = -1;
  }
  return ST_ERROR;
}

int ClientSocket::Send(const void* buf, int size) {
  int nwritten, totlen = 0;
  const char* p = static_cast<const char*>(buf);
  const char* copy = static_cast<const char*>(buf);
  while (totlen != size) {
    nwritten = write(fd_, p, size - totlen);
    if (nwritten == 0)
      return totlen;
    if (nwritten == -1)
      return -1;
    totlen += nwritten;
    p += nwritten;
  }
  return totlen;
}

int ClientSocket::Recv(void* buf, int size) {
  int nread = read(fd_, buf, size);
  if (nread == 0) {
    LOG(INFO)<< "socket " << fd_ << " has been closed";
  }
  if (nread == -1) {
    LOG(INFO)<< "read socket " << fd_
        << " failed: " << strerror(errno) << "(" << errno << ")";
  }
  return nread;
}

string ClientSocket::Recv(int size) {
  char buf[size];
  int nread = Recv(buf, size);
  return nread > 0 ? string(buf, nread) : string();
}

int ClientSocket::Recv(stringstream& ss, int size) {
  char buf[size];
  int nread = Recv(buf, size);
  ss.write(buf, nread);
  return nread;
}
}  // namespace ustore

