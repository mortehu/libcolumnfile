#ifndef BASE_COLUMNFILE_INTERNAL_H_
#define BASE_COLUMNFILE_INTERNAL_H_ 1

#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <experimental/string_view>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <kj/common.h>
#include <kj/debug.h>

#include "columnfile.h"

namespace cantera {
namespace columnfile_internal {

template <typename T>
class Delegate;

// Snatched from
//
//   https://codereview.stackexchange.com/questions/14730/impossibly-fast-delegate-in-c11
//
// Stack Exchange uses a CC-BY-SA license.
template <class R, class... A>
class Delegate<R(A...)> {
  using stub_ptr_type = R (*)(void*, A&&...);

  Delegate(void* const o, stub_ptr_type const m) noexcept : object_ptr_(o),
                                                            stub_ptr_(m) {}

 public:
  Delegate() = default;

  Delegate(Delegate const&) = default;

  Delegate(Delegate&&) = default;

  Delegate(std::nullptr_t const) noexcept : Delegate() {}

  template <class C,
            typename = typename std::enable_if<std::is_class<C>{}>::type>
  explicit Delegate(C const* const o) noexcept
      : object_ptr_(const_cast<C*>(o)) {}

  template <class C,
            typename = typename std::enable_if<std::is_class<C>{}>::type>
  explicit Delegate(C const& o) noexcept : object_ptr_(const_cast<C*>(&o)) {}

  template <class C>
  Delegate(C* const object_ptr, R (C::*const method_ptr)(A...)) {
    *this = from(object_ptr, method_ptr);
  }

  template <class C>
  Delegate(C* const object_ptr, R (C::*const method_ptr)(A...) const) {
    *this = from(object_ptr, method_ptr);
  }

  template <class C>
  Delegate(C& object, R (C::*const method_ptr)(A...)) {
    *this = from(object, method_ptr);
  }

  template <class C>
  Delegate(C const& object, R (C::*const method_ptr)(A...) const) {
    *this = from(object, method_ptr);
  }

  template <typename T, typename = typename std::enable_if<!std::is_same<
                            Delegate, typename std::decay<T>::type>{}>::type>
  Delegate(T&& f)
      : store_(operator new(sizeof(typename std::decay<T>::type)),
               functor_deleter<typename std::decay<T>::type>),
        store_size_(sizeof(typename std::decay<T>::type)) {
    using functor_type = typename std::decay<T>::type;

    new (store_.get()) functor_type(std::forward<T>(f));

    object_ptr_ = store_.get();

    stub_ptr_ = functor_stub<functor_type>;

    deleter_ = deleter_stub<functor_type>;
  }

  Delegate& operator=(Delegate const&) = default;

  Delegate& operator=(Delegate&&) = default;

  template <class C>
  Delegate& operator=(R (C::*const rhs)(A...)) {
    return *this = from(static_cast<C*>(object_ptr_), rhs);
  }

  template <class C>
  Delegate& operator=(R (C::*const rhs)(A...) const) {
    return *this = from(static_cast<C const*>(object_ptr_), rhs);
  }

  template <typename T, typename = typename std::enable_if<!std::is_same<
                            Delegate, typename std::decay<T>::type>{}>::type>
  Delegate& operator=(T&& f) {
    using functor_type = typename std::decay<T>::type;

    if ((sizeof(functor_type) > store_size_) || !store_.unique()) {
      store_.reset(operator new(sizeof(functor_type)),
                   functor_deleter<functor_type>);

      store_size_ = sizeof(functor_type);
    } else {
      deleter_(store_.get());
    }

    new (store_.get()) functor_type(std::forward<T>(f));

    object_ptr_ = store_.get();

    stub_ptr_ = functor_stub<functor_type>;

    deleter_ = deleter_stub<functor_type>;

    return *this;
  }

  template <R (*const function_ptr)(A...)>
  static Delegate from() noexcept {
    return {nullptr, function_stub<function_ptr>};
  }

  template <class C, R (C::*const method_ptr)(A...)>
  static Delegate from(C* const object_ptr) noexcept {
    return {object_ptr, method_stub<C, method_ptr>};
  }

  template <class C, R (C::*const method_ptr)(A...) const>
  static Delegate from(C const* const object_ptr) noexcept {
    return {const_cast<C*>(object_ptr), const_method_stub<C, method_ptr>};
  }

  template <class C, R (C::*const method_ptr)(A...)>
  static Delegate from(C& object) noexcept {
    return {&object, method_stub<C, method_ptr>};
  }

  template <class C, R (C::*const method_ptr)(A...) const>
  static Delegate from(C const& object) noexcept {
    return {const_cast<C*>(&object), const_method_stub<C, method_ptr>};
  }

  template <typename T>
  static Delegate from(T&& f) {
    return std::forward<T>(f);
  }

  static Delegate from(R (*const function_ptr)(A...)) { return function_ptr; }

  template <class C>
  using member_pair = std::pair<C* const, R (C::*const)(A...)>;

  template <class C>
  using const_member_pair =
      std::pair<C const* const, R (C::*const)(A...) const>;

  template <class C>
  static Delegate from(C* const object_ptr, R (C::*const method_ptr)(A...)) {
    return member_pair<C>(object_ptr, method_ptr);
  }

  template <class C>
  static Delegate from(C const* const object_ptr,
                       R (C::*const method_ptr)(A...) const) {
    return const_member_pair<C>(object_ptr, method_ptr);
  }

  template <class C>
  static Delegate from(C& object, R (C::*const method_ptr)(A...)) {
    return member_pair<C>(&object, method_ptr);
  }

  template <class C>
  static Delegate from(C const& object, R (C::*const method_ptr)(A...) const) {
    return const_member_pair<C>(&object, method_ptr);
  }

  void reset() {
    stub_ptr_ = nullptr;
    store_.reset();
  }

  void reset_stub() noexcept { stub_ptr_ = nullptr; }

  void swap(Delegate& other) noexcept { std::swap(*this, other); }

  bool operator==(Delegate const& rhs) const noexcept {
    return (object_ptr_ == rhs.object_ptr_) && (stub_ptr_ == rhs.stub_ptr_);
  }

  bool operator!=(Delegate const& rhs) const noexcept {
    return !operator==(rhs);
  }

  bool operator<(Delegate const& rhs) const noexcept {
    return (object_ptr_ < rhs.object_ptr_) ||
           ((object_ptr_ == rhs.object_ptr_) && (stub_ptr_ < rhs.stub_ptr_));
  }

  bool operator==(std::nullptr_t const) const noexcept { return !stub_ptr_; }

  bool operator!=(std::nullptr_t const) const noexcept { return stub_ptr_; }

  explicit operator bool() const noexcept { return stub_ptr_; }

  R operator()(A... args) const {
    //  assert(stub_ptr);
    return stub_ptr_(object_ptr_, std::forward<A>(args)...);
  }

 private:
  friend struct std::hash<Delegate>;

  using deleter_type = void (*)(void*);

  void* object_ptr_;
  stub_ptr_type stub_ptr_{};

  deleter_type deleter_;

  std::shared_ptr<void> store_;
  std::size_t store_size_;

  template <class T>
  static void functor_deleter(void* const p) {
    static_cast<T*>(p)->~T();

    operator delete(p);
  }

  template <class T>
  static void deleter_stub(void* const p) {
    static_cast<T*>(p)->~T();
  }

  template <R (*function_ptr)(A...)>
  static R function_stub(void* const, A&&... args) {
    return function_ptr(std::forward<A>(args)...);
  }

  template <class C, R (C::*method_ptr)(A...)>
  static R method_stub(void* const object_ptr, A&&... args) {
    return (static_cast<C*>(object_ptr)->*method_ptr)(std::forward<A>(args)...);
  }

  template <class C, R (C::*method_ptr)(A...) const>
  static R const_method_stub(void* const object_ptr, A&&... args) {
    return (static_cast<C const*>(object_ptr)->*method_ptr)(
        std::forward<A>(args)...);
  }

  template <typename>
  struct is_member_pair : std::false_type {};

  template <class C>
  struct is_member_pair<std::pair<C* const, R (C::*const)(A...)>>
      : std::true_type {};

  template <typename>
  struct is_const_member_pair : std::false_type {};

  template <class C>
  struct is_const_member_pair<
      std::pair<C const* const, R (C::*const)(A...) const>> : std::true_type {};

  template <typename T>
  static typename std::enable_if<
      !(is_member_pair<T>{} || is_const_member_pair<T>{}), R>::type
  functor_stub(void* const object_ptr, A&&... args) {
    return (*static_cast<T*>(object_ptr))(std::forward<A>(args)...);
  }

  template <typename T>
  static
      typename std::enable_if<is_member_pair<T>{} || is_const_member_pair<T>{},
                              R>::type
      functor_stub(void* const object_ptr, A&&... args) {
    return (
        static_cast<T*>(object_ptr)->first->*static_cast<T*>(object_ptr)->second)(
        std::forward<A>(args)...);
  }
};

// Creates a fixed number of threads, used for asynchronous task execution.
// Example use:
//
//   ThreadPool pool;
//
//   auto a = pool.Launch([] { return Job(0); });
//   auto b = pool.Launch([] { return Job(1); });
//
//   printf("Result: %d %d\n", a.get(), b.get());
//
// This is basically a replacement for std::async(std::launch::async, ...),
// which creates a thread on every invocation, which doesn't scale well.
class ThreadPool {
 public:
  // Constructs a thread pool with the given number of threads.
  ThreadPool(size_t n, size_t max_backlog = 256)
      : max_backlog_(max_backlog),
        caller_exec_count_(0),
        thread_exec_count_(n, 0) {
    while (n-- > 0)
      threads_.emplace_back(
          std::thread(std::bind(&ThreadPool::ThreadMain, this, n)));
  }

  // Constructs a thread pool with the same number of threads as supported by
  // the hardware.
  ThreadPool() : ThreadPool(std::thread::hardware_concurrency()) {}

  KJ_DISALLOW_COPY(ThreadPool);

  // Instructs all worker threads to stop, waits for them to stop, then
  // destroys the thread pool.  Call Wait() before destruction if you want to
  // ensure all tasks have completed.
  ~ThreadPool() {
    std::unique_lock<std::mutex> lock(mutex_);
    done_ = true;
    queue_not_empty_cv_.notify_all();
    lock.unlock();

    while (!threads_.empty()) {
      threads_.back().join();
      threads_.pop_back();
    }
  }

  // Schedules a void task for asynchronous execution.
  template <class Function,
            typename std::enable_if<
                std::is_void<typename std::result_of<Function()>::type>::value,
                void>::type* = nullptr>
  void Launch(Function&& f) {
    std::unique_lock<std::mutex> lock(mutex_);

    // If we've reached the backlog limit, we just execute in the context of
    // the calling thread.
    if (queued_calls_.size() >= max_backlog_) {
      caller_exec_count_++;
      lock.unlock();
      f();
      return;
    }

    queued_calls_.emplace_back(std::move(f));

    ++scheduled_tasks_;
    lock.unlock();

    queue_not_empty_cv_.notify_one();
  }

  // Schedules a task for asynchronous execution.
  template <class Function,
            typename std::enable_if<
                !std::is_void<typename std::result_of<Function()>::type>::value,
                void>::type* = nullptr>
  std::future<typename std::result_of<Function()>::type> Launch(Function&& f) {
    std::promise<typename std::result_of<Function()>::type> promise;
    auto result = promise.get_future();

    std::unique_lock<std::mutex> lock(mutex_);

    // If we've reached the backlog limit, we just execute in the context of
    // the calling thread.
    if (queued_calls_.size() >= max_backlog_) {
      caller_exec_count_++;
      lock.unlock();
      promise.set_value(f());
      return result;
    }

    queued_calls_.emplace_back(
        [ f = std::move(f), promise = std::move(promise) ]() mutable {
          try {
            promise.set_value(f());
          } catch (...) {
            try {
              promise.set_exception(std::current_exception());
            } catch (...) {
              std::terminate();
            }
          }
        });

    ++scheduled_tasks_;
    lock.unlock();

    queue_not_empty_cv_.notify_one();

    return result;
  }

  // Returns the number of threads in this thread pool.
  size_t Size() const { return threads_.size(); }

  // Waits for completion of all scheduled tasks.
  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    completion_cv_.wait(
        lock, [this] { return completed_tasks_ == scheduled_tasks_; });
  }

  void Stat() {
    for (size_t i = 0; i < thread_exec_count_.size(); i++)
      printf("thread %lu: %lu\n", (unsigned long)i,
             (unsigned long)thread_exec_count_[i]);
    printf("caller: %lu\n", (unsigned long)caller_exec_count_);
  }

 private:
  // Worker thread entry point.  Runs tasks added to the queue until `done_' is
  // set to true by the destructor.
  void ThreadMain(size_t index) {
    std::unique_lock<std::mutex> lock(mutex_);

    for (;;) {
      queue_not_empty_cv_.wait(
          lock, [this] { return done_ || !queued_calls_.empty(); });
      if (done_) break;

      Delegate<void()> call(std::move(queued_calls_.front()));
      queued_calls_.pop_front();

      lock.unlock();

      call();

      thread_exec_count_[index]++;

      lock.lock();

      if (++completed_tasks_ == scheduled_tasks_) completion_cv_.notify_all();
    }
  }

  // The number of tasks added with Launch().
  size_t scheduled_tasks_ = 0;

  // The number of completed tasks.
  size_t completed_tasks_ = 0;

  // The maximum number of queued tasks, before we start running tasks in the
  // calling thread.
  size_t max_backlog_;

  std::vector<std::thread> threads_;

  uint64_t caller_exec_count_;
  std::vector<uint64_t> thread_exec_count_;

  std::condition_variable queue_not_empty_cv_;
  std::condition_variable completion_cv_;
  std::mutex mutex_;

  bool done_ = false;

  std::deque<Delegate<void()>> queued_calls_;
};

// The magic code string is designed to cause a parse error if someone attempts
// to parse the file as a CSV.
static const char kMagic[4] = {'\n', '\t', '\"', 0};

enum Codes : uint8_t {
  kCodeNull = 0xff,
};

inline uint32_t GetUInt(std::experimental::string_view& input) {
  auto begin = input.data();
  auto i = begin;
  uint32_t b = *i++;
  uint32_t result = b & 127;
  if (b < 0x80) goto done;
  b = *i++;
  result |= (b & 127) << 6;
  if (b < 0x80) goto done;
  b = *i++;
  result |= (b & 127) << 13;
  if (b < 0x80) goto done;
  b = *i++;
  result |= (b & 127) << 20;
  if (b < 0x80) goto done;
  b = *i++;
  KJ_REQUIRE(b <= 0x1f, b);
  result |= b << 27;
done:
  input.remove_prefix(i - begin);
  return result;
}

inline int32_t GetInt(std::experimental::string_view& input) {
  const auto u = GetUInt(input);
  return (u >> 1) ^ -((int32_t)(u & 1));
}

inline void PutUInt(std::string& output, uint32_t value) {
  if (value < (1 << 7)) {
    output.push_back(value);
  } else if (value < (1 << 13)) {
    output.push_back((value & 0x3f) | 0x80);
    output.push_back(value >> 6);
  } else if (value < (1 << 20)) {
    output.push_back((value & 0x3f) | 0x80);
    output.push_back((value >> 6) | 0x80);
    output.push_back(value >> 13);
  } else if (value < (1 << 27)) {
    output.push_back((value & 0x3f) | 0x80);
    output.push_back((value >> 6) | 0x80);
    output.push_back((value >> 13) | 0x80);
    output.push_back(value >> 20);
  } else {
    output.push_back((value & 0x3f) | 0x80);
    output.push_back((value >> 6) | 0x80);
    output.push_back((value >> 13) | 0x80);
    output.push_back((value >> 20) | 0x80);
    output.push_back(value >> 27);
  }
}

inline void PutInt(std::string& output, int32_t value) {
  static const auto sign_shift = sizeof(value) * 8 - 1;

  PutUInt(output, (value << 1) ^ (value >> sign_shift));
}

void CompressZLIB(std::string& output, const string_view& input,
                  ThreadPool& thread_pool);

}  // namespace columnfile_internal
}  // namespace cantera

#endif  // !BASE_COLUMNFILE_INTERNAL_H_
