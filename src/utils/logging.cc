// Copyright (c) 2017 The Ustore Authors.

#include "utils/logging.h"

#include <stdlib.h>
#include <sys/types.h>
#ifdef _MSC_VER
#include <io.h>
#else
#include <unistd.h>
#endif

namespace ustore {

FILE* log_file[NUM_SEVERITIES] = {};
bool not_log_stderr[NUM_SEVERITIES] = {};

void InitLogging(const char *argv) {
  LogToStderr();
}

void LogToStderr() {
  for (int i = 0; i < NUM_SEVERITIES; ++i) {
    log_file[i] = nullptr;
    not_log_stderr[i] = false;
  }
}

void SetStderrLogging(int severity) {
  for (int i = 0; i < NUM_SEVERITIES; ++i) {
    not_log_stderr[i] = i >= severity ? false : true;
  }
}

void SetLogDestination(int severity, const char* path) {
  log_file[severity] = fopen(path, "a");
  if (severity < ERROR) not_log_stderr[severity] = true;
}

namespace logging {

LogMessage::LogMessage(const char* fname, int line, int severity)
    : fname_(fname), line_(line), severity_(severity) {}

inline pid_t GetPID() { return getpid(); }
inline pid_t GetTID() { return (pid_t)(uintptr_t)pthread_self(); }

void LogMessage::GenerateLogMessage() {
  time_t rw_time = time(nullptr);
  struct tm tm_time;
  localtime_r(&rw_time, &tm_time);
  // log to a file
  for (int i = severity_; i >= 0; --i)
    if (log_file[i] )
      DoLogging(log_file[i], tm_time);
  // log to stderr
  if (!not_log_stderr[severity_])
    DoLogging(stderr, tm_time);
}

void LogMessage::DoLogging(FILE* file, const struct tm& tm_time) {
  fprintf(file, "[%c d%02d%02d t%02d:%02d:%02d p%05d:%03d %s:%d] %s\n",
          "IWEF"[severity_],
          1 + tm_time.tm_mon,
          tm_time.tm_mday,
          tm_time.tm_hour,
          tm_time.tm_min,
          tm_time.tm_sec,
          GetPID(),
          static_cast<unsigned>(GetTID()%1000),
          fname_,
          line_,
          str().c_str());
}

LogMessage::~LogMessage() { GenerateLogMessage(); }

LogMessageFatal::LogMessageFatal(const char* file, int line)
  : LogMessage(file, line, FATAL) {}
LogMessageFatal::~LogMessageFatal() {
  // abort() ensures we don't return
  GenerateLogMessage();
  abort();
}

template <>
void MakeCheckOpValueString(std::ostream* os, const char& v) {
  if (v >= 32 && v <= 126) {
    (*os) << "'" << v << "'";
  } else {
    (*os) << "char value " << (short)v;
  }
}

template <>
void MakeCheckOpValueString(std::ostream* os, const signed char& v) {
  if (v >= 32 && v <= 126) {
    (*os) << "'" << v << "'";
  } else {
    (*os) << "signed char value " << (short)v;
  }
}

template <>
void MakeCheckOpValueString(std::ostream* os, const unsigned char& v) {
  if (v >= 32 && v <= 126) {
    (*os) << "'" << v << "'";
  } else {
    (*os) << "unsigned char value " << (unsigned short)v;
  }
}

template <>
void MakeCheckOpValueString(std::ostream* os, const std::nullptr_t& p) {
    (*os) << "nullptr";
}

CheckOpMessageBuilder::CheckOpMessageBuilder(const char* exprtext)
    : stream_(new std::ostringstream) {
  *stream_ << "Check failed: " << exprtext << " (";
}

CheckOpMessageBuilder::~CheckOpMessageBuilder() { delete stream_; }

std::ostream* CheckOpMessageBuilder::ForVar2() {
  *stream_ << " vs. ";
  return stream_;
}

string* CheckOpMessageBuilder::NewString() {
  *stream_ << ")";
  return new string(stream_->str());
}

}  // namespace logging
}  // namespace ustore
