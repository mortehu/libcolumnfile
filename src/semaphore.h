#ifndef SEMAPHORE_H_
#define SEMAPHORE_H_

#include <condition_variable>
#include <mutex>

class Semaphore {
 public:
  explicit Semaphore(size_t count) : count_{count} {}

  void put(size_t n = 1) {
    std::unique_lock<std::mutex> lk{mutex_};
    count_ += n;
    lk.unlock();
    while(n-- > 0) cv_.notify_one();
  }

  void get() {
    std::unique_lock<std::mutex> lk{mutex_};
    cv_.wait(lk, [this] { return count_ > 0; });
    --count_;
  }

  bool try_get() {
    std::unique_lock<std::mutex> lk{mutex_};
    if (count_ == 0) return false;
    --count_;
    return true;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  size_t count_;
};

#endif  // !SEMAPHORE_H_
