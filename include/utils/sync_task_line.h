#ifndef USTORE_UTILS_SYNC_TASK_LINE_H_
#define USTORE_UTILS_SYNC_TASK_LINE_H_

#include <mutex>
#include "utils/blocking_queue.h"

namespace ustore {

template<class T1, class T2 = int>
class SyncTaskLine {
 public:
  virtual T2 Consume(const T1& data) = 0;
  virtual bool Terminate(const T1& data) = 0;

  explicit SyncTaskLine(size_t queue_size)
    : queue_(new BlockingQueue<T1>(queue_size)),
      mtx_(new std::mutex()),
      cv_(new std::condition_variable()),
      processed_(false) {}

  SyncTaskLine() : SyncTaskLine(2) {}

  ~SyncTaskLine() {
    delete queue_;
    delete mtx_;
    delete cv_;
  }

  void Run();

  void Produce(const T1& data);
  void Produce(const T1&& data);

  inline std::thread Launch() {
    return std::thread([this] { Run(); });
  }

  inline void Sync() {
    std::unique_lock<std::mutex> lock(*mtx_);
    cv_->wait(lock, [this] { return processed_; });
  }

  inline T2 Stat() {
    std::lock_guard<std::mutex> lock(*mtx_);
    return stat_;
  }

 private:
  ustore::BlockingQueue<T1>* queue_;
  std::mutex* mtx_;
  std::condition_variable* cv_;
  bool processed_;
  T2 stat_;
};

template<class T1, class T2>
void SyncTaskLine<T1, T2>::Run() {
  T1 data;
  do {
    data = queue_->Take();
    {
      std::lock_guard<std::mutex> lock(*mtx_);
      stat_ = Consume(data);
      processed_ = true;
    }
    cv_->notify_one();
  } while (!Terminate(data));
}

template<class T1, class T2>
void SyncTaskLine<T1, T2>::Produce(const T1& data) {
  {
    std::lock_guard<std::mutex> lock(*mtx_);
    processed_ = false;
  }
  queue_->Put(data);
}

template<class T1, class T2>
void SyncTaskLine<T1, T2>::Produce(const T1&& data) {
  {
    std::lock_guard<std::mutex> lock(*mtx_);
    processed_ = false;
  }
  queue_->Put(std::move(data));
}

}  // namespace ustore

#endif  // USTORE_UTILS_SYNC_TASK_LINE_H_
