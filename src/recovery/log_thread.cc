// Copyright (c) 2017 The UStore Authors.

#include"recovery/log_thread.h"

namespace ustore {
namespace recovery {

LogThread::LogThread() {
  m_tid_ = 0;
  m_running_ = 0;
  m_detached_ = 0;
}

LogThread::~LogThread() {
  if (m_running_ == 1 && m_detached_ == 0) {
    pthread_detach(m_tid_);
  }
  if (m_running_ == 1) {
    pthread_cancel(m_tid_);
  }
}

int LogThread::Start() {
  int tmpret = pthread_create(&m_tid_, NULL, &runThread, this);
  if (tmpret == 0) {
    m_running_ = 1;
  }
  return tmpret;
}

int LogThread::Join() {
  int tmpret = -1;
  if (m_running_ == 1) {
    tmpret = pthread_join(m_tid_, NULL);
  }
  if (tmpret == 0) {
    m_detached_ = 1;
  }
  return tmpret;
}

int LogThread::Detach() {
  int tmpret = -1;
  if (m_running_ == 1 && m_detached_ == 0) {
    tmpret = pthread_detach(m_tid_);
    if (tmpret == 0) {
      m_detached_ = 1;
    }
  }
  return tmpret;
}

pthread_t LogThread::Self() {
  return m_tid_;
}

static void* runThread(void* args) {
  return (reinterpret_cast<LogThread*>(args))->Run();
  // return ((LogThread*)args)->Run();
}

}  // end of namespace recovery
}  // end of namespace ustore
