#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
static void *os_alloc_pages(std::size_t size) {
  void *p =
      VirtualAlloc(nullptr, size, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
  if (!p) throw std::bad_alloc();
  return p;
}
static void os_free_pages(void *p, std::size_t) {
  VirtualFree(p, 0, MEM_RELEASE);
}
#else
#include <sys/mman.h>
#include <unistd.h>
static void *os_alloc_pages(std::size_t size) {
  long page = sysconf(_SC_PAGESIZE);
  std::size_t s = ((size + page - 1) / page) * page;
  void *p = mmap(nullptr, s, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (p == MAP_FAILED) throw std::bad_alloc();
  return p;
}
static void os_free_pages(void *p, std::size_t size) {
  long page = sysconf(_SC_PAGESIZE);
  std::size_t s = ((size + page - 1) / page) * page;
  munmap(p, s);
}
#endif

namespace dagflow {

template <typename Sig, std::size_t StorageSize = 64>
class small_function;

template <std::size_t StorageSize>
class small_function<void(), StorageSize> {
 public:
  small_function() noexcept = default;
  small_function(std::nullptr_t) noexcept {}
  small_function(const small_function &) = delete;
  small_function &operator=(const small_function &) = delete;

  small_function(small_function &&other) noexcept {
    move_from(std::move(other));
  }

  small_function &operator=(small_function &&other) noexcept {
    if (this != &other) {
      reset();
      move_from(std::move(other));
    }
    return *this;
  }

  template <typename F, typename = std::enable_if_t<
                            !std::is_same_v<std::decay_t<F>, small_function>>>
  small_function(F &&f) {
    emplace(std::forward<F>(f));
  }

  ~small_function() { reset(); }

  explicit operator bool() const noexcept { return call_ != nullptr; }

  void operator()() {
    if (call_) call_(storage_);
  }

  void reset() noexcept {
    if (destroy_) {
      destroy_(storage_, nullptr);
      destroy_ = nullptr;
      call_ = nullptr;
      move_ = nullptr;
    }
  }

  template <typename F>
  void emplace(F &&f) {
    reset();
    using FnT = std::decay_t<F>;
    static_assert(std::is_invocable_r_v<void, FnT &>,
                  "Callable must be void()");
    static_assert(sizeof(FnT) <= StorageSize,
                  "Callable too large for small_function");

    new (storage_) FnT(std::forward<F>(f));

    call_ = [](void *s) { (*reinterpret_cast<FnT *>(s))(); };

    move_ = [](void *dst, void *src) {
      new (dst) FnT(std::move(*reinterpret_cast<FnT *>(src)));
      reinterpret_cast<FnT *>(src)->~FnT();
    };

    destroy_ = [](void *s, void *) { reinterpret_cast<FnT *>(s)->~FnT(); };
  }

 private:
  using CallFn = void (*)(void *);
  using OpFn = void (*)(void *, void *);

  void move_from(small_function &&other) noexcept {
    if (other.call_) {
      move_ = other.move_;
      destroy_ = other.destroy_;
      call_ = other.call_;
      move_(storage_, other.storage_);
      other.call_ = nullptr;
      other.move_ = nullptr;
      other.destroy_ = nullptr;
    }
  }

  alignas(std::max_align_t) unsigned char storage_[StorageSize];
  CallFn call_ = nullptr;
  OpFn move_ = nullptr;
  OpFn destroy_ = nullptr;
};

}

namespace shizalloc {

#ifdef __cpp_lib_hardware_interference_size
constexpr std::size_t CACHE_LINE_SIZE =
    std::hardware_destructive_interference_size;
#else
constexpr std::size_t CACHE_LINE_SIZE = 64;
#endif

struct Chunk;
struct Task {
  using Func = dagflow::small_function<void(), 64>;
  std::atomic<Task *> next;
  uint32_t owner_id;
  uint32_t flags;
  uint32_t priority;
  Func fn;
  Task()
      : next(nullptr),
        owner_id(UINT32_MAX),
        flags(0),
        priority(0),
        fn(nullptr) {}
  void execute() {
    if (fn) fn();
  }
};

struct Chunk {
  std::atomic<Chunk *> next;
  std::size_t capacity_bytes;
  std::atomic<std::size_t> used;
  std::atomic<std::size_t> active_count;
  std::atomic<uint32_t> owner_id;
  Chunk()
      : next(nullptr),
        capacity_bytes(0),
        used(0),
        active_count(0),
        owner_id(UINT32_MAX) {}
};

struct ObjectHeader {
  std::atomic<Task *> next;
  Chunk *chunk_ptr;
  std::size_t alloc_size;
  bool is_task;
};

class QuiescentStateReclaimer {
 public:
  explicit QuiescentStateReclaimer(std::size_t max_threads)
      : max_threads_(max_threads),
        observed_epoch_(nullptr),
        retired_lists_(max_threads) {
    observed_epoch_ = static_cast<std::atomic<std::size_t> *>(operator new[](
        sizeof(std::atomic<std::size_t>) * max_threads_));
    for (std::size_t i = 0; i < max_threads_; ++i) {
      new (&observed_epoch_[i]) std::atomic<std::size_t>(0);
    }
    global_epoch_.store(1, std::memory_order_relaxed);
  }

  ~QuiescentStateReclaimer() {
    try_reclaim();
    for (std::size_t i = 0; i < max_threads_; ++i) {
      observed_epoch_[i].~atomic<std::size_t>();
    }
    operator delete[](observed_epoch_);
  }

  std::size_t register_thread() {
    std::size_t id = next_id_.fetch_add(1, std::memory_order_relaxed);
    if (id >= max_threads_)
      throw std::runtime_error("QSBR: max threads exceeded");
    thread_local_id() = id;
    observed_epoch_[id].store(global_epoch_.load(std::memory_order_relaxed),
                              std::memory_order_release);
    return id;
  }

  void quiescent() {
    std::size_t id = current_thread_id();
    if (id == SIZE_MAX) return;
    std::size_t g = global_epoch_.load(std::memory_order_acquire);
    observed_epoch_[id].store(g, std::memory_order_release);
  }

  void retire_chunk(Chunk *c) {
    std::size_t id = current_thread_id();
    if (id == SIZE_MAX) return;
    Retired r{c, global_epoch_.load(std::memory_order_relaxed)};
    auto &vec = retired_lists_[id];
    if (vec.size() + 1 > vec.capacity()) vec.reserve((vec.size() + 1) * 2);
    vec.push_back(r);
  }

  void try_reclaim() {
    global_epoch_.fetch_add(1, std::memory_order_acq_rel);
    std::size_t registered = next_id_.load(std::memory_order_acquire);
    if (registered == 0) return;
    std::size_t min_obs = SIZE_MAX;
    for (std::size_t i = 0; i < registered; ++i) {
      std::size_t v = observed_epoch_[i].load(std::memory_order_acquire);
      if (v < min_obs) min_obs = v;
    }
    for (std::size_t i = 0; i < registered; ++i) {
      auto &vec = retired_lists_[i];
      std::size_t write = 0;
      for (std::size_t read = 0; read < vec.size(); ++read) {
        if (vec[read].epoch < min_obs) {
          reclaim_chunk(vec[read].chunk);
        } else {
          if (write != read) vec[write] = vec[read];
          ++write;
        }
      }
      vec.resize(write);
    }
  }

  std::size_t current_thread_id() const { return thread_local_id(); }

  static std::size_t &thread_local_id() {
    thread_local std::size_t t = SIZE_MAX;
    return t;
  }

 private:
  struct Retired {
    Chunk *chunk;
    std::size_t epoch;
  };

  void reclaim_chunk(Chunk *c) {
    std::size_t total = sizeof(Chunk) + c->capacity_bytes;
    os_free_pages(reinterpret_cast<void *>(c), total);
  }

  std::size_t max_threads_;
  std::atomic<std::size_t> next_id_{0};
  std::atomic<std::size_t> *observed_epoch_;
  std::vector<std::vector<Retired>> retired_lists_;
  std::atomic<std::size_t> global_epoch_{0};
};

class ChunkReservoir {
 public:
  explicit ChunkReservoir(std::size_t chunk_size)
      : head_(nullptr), chunk_size_(chunk_size) {}

  ~ChunkReservoir() {
    Chunk *c = head_.load(std::memory_order_acquire);
    while (c) {
      Chunk *next = c->next.load(std::memory_order_relaxed);
      std::size_t total = sizeof(Chunk) + c->capacity_bytes;
      os_free_pages(reinterpret_cast<void *>(c), total);
      c = next;
    }
  }

  Chunk *pop() {
    Chunk *h = head_.load(std::memory_order_acquire);
    while (h) {
      Chunk *nxt = h->next.load(std::memory_order_relaxed);
      if (head_.compare_exchange_weak(h, nxt, std::memory_order_acq_rel,
                                      std::memory_order_acquire))
        return h;
    }
    return nullptr;
  }

  void push(Chunk *c) {
    c->next.store(nullptr, std::memory_order_relaxed);
    c->owner_id.store(UINT32_MAX, std::memory_order_relaxed);
    Chunk *h = head_.load(std::memory_order_acquire);
    do {
      c->next.store(h, std::memory_order_relaxed);
    } while (!head_.compare_exchange_weak(h, c, std::memory_order_release,
                                          std::memory_order_acquire));
  }

  Chunk *obtain() {
    if (Chunk *c = pop()) {
      c->used.store(0, std::memory_order_relaxed);
      c->active_count.store(0, std::memory_order_relaxed);
      c->owner_id.store(UINT32_MAX, std::memory_order_relaxed);
      return c;
    }
    std::size_t total = sizeof(Chunk) + chunk_size_;
    void *mem = os_alloc_pages(total);
    Chunk *c = reinterpret_cast<Chunk *>(mem);
    c->next.store(nullptr, std::memory_order_relaxed);
    c->capacity_bytes = chunk_size_;
    c->used.store(0, std::memory_order_relaxed);
    c->active_count.store(0, std::memory_order_relaxed);
    c->owner_id.store(UINT32_MAX, std::memory_order_relaxed);
    return c;
  }

  void preallocate(std::size_t count) {
    for (std::size_t i = 0; i < count; ++i) {
      Chunk *c = obtain();
      push(c);
    }
  }

  std::size_t chunk_size() const noexcept { return chunk_size_; }

 private:
  std::atomic<Chunk *> head_;
  std::size_t chunk_size_;
};

class LockFreeTaskArena {
 public:
  LockFreeTaskArena(uint32_t owner_id, ChunkReservoir *reservoir,
                    QuiescentStateReclaimer *qsbr)
      : owner_id_(owner_id),
        reservoir_(reservoir),
        qsbr_(qsbr),
        cur_chunk_(nullptr),
        local_free_head_(nullptr),
        remote_head_(nullptr),
        local_bump_ptr_(0),
        local_bump_cache_(0) {}

  ~LockFreeTaskArena() {
    Chunk *c = cur_chunk_.load(std::memory_order_relaxed);
    if (c) reservoir_->push(c);
  }

  template <typename F>
  Task *allocate_task(uint32_t priority, F &&func) {
    if (Task *t = pop_local_free()) {
      construct_task(t, priority, std::forward<F>(func));
      t->owner_id = owner_id_;
      return t;
    }
    drain_remote_to_local();
    if (Task *t = pop_local_free()) {
      construct_task(t, priority, std::forward<F>(func));
      t->owner_id = owner_id_;
      return t;
    }
    Task *t = bump_alloc<Task>();
    if (!t) {
      refill_chunk();
      t = bump_alloc<Task>();
      if (!t) throw std::bad_alloc();
    }
    construct_task(t, priority, std::forward<F>(func));
    t->owner_id = owner_id_;
    return t;
  }

  void deallocate_task(Task *t) noexcept {
    if (!t) return;
    destroy_task(t);
    ObjectHeader *hdr = header_from_obj(t);
    Chunk *c = hdr->chunk_ptr;
    assert(c);
    if (is_current_thread_owner()) {
      push_local_free(t);
      if (c->active_count.fetch_sub(1, std::memory_order_acq_rel) == 1)
        retire_chunk_if_needed(c);
    } else {
      push_remote_from_any(static_cast<void *>(t));
    }
  }

  void *allocate_bytes(std::size_t bytes, std::size_t align) {
    if (bytes == 0) return nullptr;
    void *p = bump_alloc_bytes(bytes, align);
    if (!p) {
      refill_chunk();
      p = bump_alloc_bytes(bytes, align);
      if (!p) throw std::bad_alloc();
    }
    return p;
  }

  void deallocate_bytes(void *p, std::size_t) noexcept {
    if (!p) return;
    ObjectHeader *hdr = header_from_payload(p);
    Chunk *c = hdr->chunk_ptr;
    assert(c);
    if (is_current_thread_owner()) {
      if (c->active_count.fetch_sub(1, std::memory_order_acq_rel) == 1)
        retire_chunk_if_needed(c);
    } else {
      push_remote_from_any(p);
    }
  }

  void push_remote_from_any(void *payload) noexcept {
    ObjectHeader *hdr = header_from_payload(payload);
    Task *node = reinterpret_cast<Task *>(hdr);
    node->next.store(nullptr, std::memory_order_relaxed);
    Task *head = remote_head_.load(std::memory_order_acquire);
    do {
      node->next.store(head, std::memory_order_relaxed);
    } while (!remote_head_.compare_exchange_weak(
        head, node, std::memory_order_release, std::memory_order_acquire));
  }

  void poll() {
    drain_remote_to_local();
    qsbr_->quiescent();
    qsbr_->try_reclaim();
    try_return_empty_current_chunk();
  }

  uint32_t owner_id() const noexcept { return owner_id_; }

  static ObjectHeader *header_from_payload(void *p) noexcept {
    return reinterpret_cast<ObjectHeader *>(reinterpret_cast<std::byte *>(p) -
                                            sizeof(ObjectHeader));
  }

  static ObjectHeader *header_from_obj(Task *t) noexcept {
    return reinterpret_cast<ObjectHeader *>(reinterpret_cast<std::byte *>(t) -
                                            sizeof(ObjectHeader));
  }

  static std::byte *chunk_data_ptr(Chunk *c) noexcept {
    return reinterpret_cast<std::byte *>(c + 1);
  }

 private:
  void push_local_free(Task *task) noexcept {
    Task *expected = local_free_head_.load(std::memory_order_relaxed);
    do {
      task->next.store(expected, std::memory_order_relaxed);
    } while (!local_free_head_.compare_exchange_weak(
        expected, task, std::memory_order_release, std::memory_order_relaxed));
  }

  Task *pop_local_free() noexcept {
    Task *head = local_free_head_.load(std::memory_order_acquire);
    while (head) {
      Task *next = head->next.load(std::memory_order_relaxed);
      if (local_free_head_.compare_exchange_weak(head, next,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
        ObjectHeader *hdr = header_from_obj(head);
        Chunk *c = hdr->chunk_ptr;
        c->active_count.fetch_add(1, std::memory_order_acq_rel);
        return head;
      }
    }
    return nullptr;
  }

  void drain_remote_to_local() noexcept {
    Task *head = remote_head_.exchange(nullptr, std::memory_order_acq_rel);
    if (!head) return;
    Task *cur = head;
    Task *rev = nullptr;
    while (cur) {
      Task *next = cur->next.load(std::memory_order_relaxed);
      cur->next.store(rev, std::memory_order_relaxed);
      rev = cur;
      cur = next;
    }
    cur = rev;
    while (cur) {
      Task *next = cur->next.load(std::memory_order_relaxed);
      ObjectHeader *hdr = reinterpret_cast<ObjectHeader *>(cur);
      Chunk *c = hdr->chunk_ptr;
      if (hdr->alloc_size == sizeof(Task) && hdr->is_task) {
        Task *actual_task = reinterpret_cast<Task *>(
            reinterpret_cast<std::byte *>(hdr) + sizeof(ObjectHeader));
        push_local_free(actual_task);
      }
      if (c->active_count.fetch_sub(1, std::memory_order_acq_rel) == 1)
        retire_chunk_if_needed(c);
      cur = next;
    }
  }

  template <typename U>
  U *bump_alloc() {
    Chunk *c = cur_chunk_.load(std::memory_order_acquire);
    if (!c) return nullptr;

    constexpr std::size_t bytes = sizeof(U);
    constexpr std::size_t align = alignof(U);
    constexpr std::size_t total_needed =
        sizeof(ObjectHeader) + bytes + align - 1;

    std::size_t cur = c->used.load(std::memory_order_relaxed);
    std::size_t new_used = cur + total_needed;

    if (new_used > c->capacity_bytes) return nullptr;

    if (c->used.compare_exchange_weak(cur, new_used, std::memory_order_acq_rel,
                                      std::memory_order_relaxed)) {
      std::byte *base = chunk_data_ptr(c);
      std::byte *obj_start = base + cur + sizeof(ObjectHeader);

      std::size_t misalignment = reinterpret_cast<uintptr_t>(obj_start) % align;
      if (misalignment > 0) {
        obj_start += align - misalignment;
      }

      ObjectHeader *hdr =
          reinterpret_cast<ObjectHeader *>(obj_start - sizeof(ObjectHeader));
      hdr->chunk_ptr = c;
      hdr->alloc_size = bytes;
      hdr->is_task = true;

      U *obj = reinterpret_cast<U *>(obj_start);
      c->active_count.fetch_add(1, std::memory_order_acq_rel);
      c->owner_id.store(owner_id_, std::memory_order_release);
      return obj;
    }
    return nullptr;
  }

  void *bump_alloc_bytes(std::size_t bytes, std::size_t align) {
    Chunk *c = cur_chunk_.load(std::memory_order_acquire);
    if (!c) return nullptr;

    const std::size_t total_needed = sizeof(ObjectHeader) + bytes + align - 1;
    std::size_t cur = c->used.load(std::memory_order_relaxed);
    std::size_t new_used = cur + total_needed;

    if (new_used > c->capacity_bytes) return nullptr;

    if (c->used.compare_exchange_weak(cur, new_used, std::memory_order_acq_rel,
                                      std::memory_order_relaxed)) {
      std::byte *base = chunk_data_ptr(c);
      std::byte *obj_start = base + cur + sizeof(ObjectHeader);

      std::size_t misalignment = reinterpret_cast<uintptr_t>(obj_start) % align;
      if (misalignment > 0) {
        obj_start += align - misalignment;
      }

      ObjectHeader *hdr =
          reinterpret_cast<ObjectHeader *>(obj_start - sizeof(ObjectHeader));
      hdr->chunk_ptr = c;
      hdr->alloc_size = bytes;
      hdr->is_task = false;

      void *payload = static_cast<void *>(obj_start);
      c->active_count.fetch_add(1, std::memory_order_acq_rel);
      c->owner_id.store(owner_id_, std::memory_order_release);
      return payload;
    }
    return nullptr;
  }

  void refill_chunk() {
    Chunk *c = reservoir_->obtain();
    c->owner_id.store(owner_id_, std::memory_order_release);
    cur_chunk_.store(c, std::memory_order_release);
  }

  void retire_chunk_if_needed(Chunk *c) noexcept {
    if (c->active_count.load(std::memory_order_acquire) == 0) {
      Chunk *current = cur_chunk_.load(std::memory_order_acquire);
      if (current == c)
        cur_chunk_.compare_exchange_strong(current, nullptr,
                                           std::memory_order_release,
                                           std::memory_order_relaxed);
      qsbr_->retire_chunk(c);
    }
  }

  void try_return_empty_current_chunk() noexcept {
    Chunk *c = cur_chunk_.load(std::memory_order_acquire);
    if (!c) return;
    if (c->active_count.load(std::memory_order_acquire) == 0 &&
        c->used.load(std::memory_order_acquire) == 0) {
      if (cur_chunk_.compare_exchange_strong(c, nullptr,
                                             std::memory_order_release,
                                             std::memory_order_relaxed)) {
        reservoir_->push(c);
      }
    }
  }

  template <typename F>
  static void construct_task(Task *p, uint32_t priority, F &&func) {
    std::construct_at(p);
    p->priority = priority;
    p->owner_id = UINT32_MAX;
    p->flags = 1;
    p->fn.emplace(std::forward<F>(func));
  }

  static void destroy_task(Task *p) noexcept { std::destroy_at(p); }

  bool is_current_thread_owner() const noexcept {
    return qsbr_->current_thread_id() == owner_id_;
  }

  void refill_local_bump_cache() {
    Chunk *c = cur_chunk_.load(std::memory_order_acquire);
    if (c) {
      local_bump_cache_ = c->used.load(std::memory_order_relaxed);
      local_bump_ptr_.store(local_bump_cache_, std::memory_order_relaxed);
    }
  }

 private:
  uint32_t owner_id_;
  ChunkReservoir *reservoir_;
  QuiescentStateReclaimer *qsbr_;

  std::atomic<Chunk *> cur_chunk_;
  std::atomic<Task *> local_free_head_;
  std::atomic<Task *> remote_head_;

  alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> local_bump_ptr_;
  std::size_t local_bump_cache_;
};

class LockFreeTaskAllocatorManager {
 public:
  LockFreeTaskAllocatorManager(std::size_t max_threads,
                               std::size_t chunk_size_bytes = 4 * 1024 * 1024)
      : qsbr_(max_threads),
        reservoir_(chunk_size_bytes),
        max_threads_(max_threads),
        arenas_(nullptr) {
    arenas_ = static_cast<std::atomic<LockFreeTaskArena *> *>(operator new[](
        sizeof(std::atomic<LockFreeTaskArena *>) * max_threads_));
    for (std::size_t i = 0; i < max_threads_; ++i) {
      new (&arenas_[i]) std::atomic<LockFreeTaskArena *>(nullptr);
    }

    reservoir_.preallocate(max_threads * 2);
  }

  ~LockFreeTaskAllocatorManager() {
    for (std::size_t i = 0; i < max_threads_; ++i) {
      LockFreeTaskArena *a = arenas_[i].load(std::memory_order_relaxed);
      if (a) delete a;
      arenas_[i].~atomic<LockFreeTaskArena *>();
    }
    operator delete[](arenas_);
  }

  std::size_t register_thread() { return qsbr_.register_thread(); }

  LockFreeTaskArena *create_thread_local_arena() {
    std::size_t id = qsbr_.current_thread_id();
    if (id == SIZE_MAX) id = register_thread();
    if (id >= max_threads_) throw std::runtime_error("thread id overflow");
    LockFreeTaskArena *expected = arenas_[id].load(std::memory_order_acquire);
    if (!expected) {
      auto up = std::make_unique<LockFreeTaskArena>(static_cast<uint32_t>(id),
                                                    &reservoir_, &qsbr_);
      LockFreeTaskArena *desired = up.get();
      if (arenas_[id].compare_exchange_strong(expected, desired,
                                              std::memory_order_release,
                                              std::memory_order_acquire)) {
        up.release();
        return desired;
      } else {
        return expected;
      }
    }
    return expected;
  }

  LockFreeTaskArena *get_arena(std::size_t id) {
    if (id >= max_threads_) return nullptr;
    return arenas_[id].load(std::memory_order_acquire);
  }

  QuiescentStateReclaimer &qsbr() { return qsbr_; }
  ChunkReservoir &reservoir() { return reservoir_; }

  void route_deallocate_by_payload(void *payload, std::size_t) {
    if (!payload) return;
    ObjectHeader *hdr = LockFreeTaskArena::header_from_payload(payload);
    Chunk *c = hdr->chunk_ptr;
    if (!c) return;
    uint32_t owner = c->owner_id.load(std::memory_order_acquire);
    if (owner == UINT32_MAX) {
      LockFreeTaskArena *a = create_thread_local_arena();
      a->deallocate_bytes(payload, hdr->alloc_size);
      return;
    }
    LockFreeTaskArena *owner_arena = get_arena(owner);
    if (!owner_arena) {
      LockFreeTaskArena *a = create_thread_local_arena();
      a->deallocate_bytes(payload, hdr->alloc_size);
      return;
    }

    owner_arena->push_remote_from_any(payload);
  }

 private:
  QuiescentStateReclaimer qsbr_;
  ChunkReservoir reservoir_;
  std::size_t max_threads_;
  std::atomic<LockFreeTaskArena *> *arenas_;
};

class QSBRMemory {
 public:
  static QSBRMemory &instance() {
    static QSBRMemory s;
    return s;
  }

  void init(std::size_t max_threads,
            std::size_t chunk_size_bytes = 4 * 1024 * 1024) {
    std::lock_guard<std::mutex> lk(init_mtx_);
    if (!mgr_)
      mgr_ = std::make_unique<LockFreeTaskAllocatorManager>(max_threads,
                                                            chunk_size_bytes);
  }

  LockFreeTaskArena *ensure_thread_arena() {
    if (!mgr_)
      init(std::thread::hardware_concurrency()
               ? std::thread::hardware_concurrency()
               : 4);
    std::size_t id = mgr_->qsbr().current_thread_id();
    if (id == SIZE_MAX) id = mgr_->register_thread();
    LockFreeTaskArena *a = mgr_->create_thread_local_arena();
    return a;
  }

  template <typename T, typename... Args>
  T *allocate_object(Args &&...args) {
    LockFreeTaskArena *arena = ensure_thread_arena();
    void *mem = arena->allocate_bytes(sizeof(T), alignof(T));
    T *obj = ::new (mem) T(std::forward<Args>(args)...);
    return obj;
  }

  template <typename T>
  void deallocate_object(T *p) {
    if (!p) return;
    p->~T();

    if (!mgr_) {
      return;
    }
    mgr_->route_deallocate_by_payload(static_cast<void *>(p), sizeof(T));
  }

  void *allocate_bytes(std::size_t bytes, std::size_t align,
                       std::size_t n = 1) {
    if (!mgr_)
      init(std::thread::hardware_concurrency()
               ? std::thread::hardware_concurrency()
               : 4);
    LockFreeTaskArena *arena = ensure_thread_arena();

    return arena->allocate_bytes(bytes * n, align);
  }

  void deallocate_bytes(void *p, std::size_t bytes, std::size_t n = 1) {
    if (!p) return;
    if (!mgr_) return;

    mgr_->route_deallocate_by_payload(p, bytes * n);
  }

  LockFreeTaskAllocatorManager *manager() { return mgr_.get(); }

  void shutdown() {
    std::lock_guard<std::mutex> lk(init_mtx_);
    if (mgr_) {
      mgr_->qsbr().try_reclaim();
      mgr_.reset();
    }
  }

 private:
  QSBRMemory() = default;
  std::unique_ptr<LockFreeTaskAllocatorManager> mgr_;
  std::mutex init_mtx_;
};

template <typename T>
class QSBRAllocator {
 public:
  using value_type = T;

  QSBRAllocator() noexcept {}
  template <class U>
  QSBRAllocator(const QSBRAllocator<U> &) noexcept {}

  T *allocate(std::size_t n) {
    if (n == 0) return nullptr;
    void *p = QSBRMemory::instance().allocate_bytes(sizeof(T), alignof(T), n);
    if (!p) throw std::bad_alloc();
    return reinterpret_cast<T *>(p);
  }

  void deallocate(T *p, std::size_t n) noexcept {
    if (!p) return;
    QSBRMemory::instance().deallocate_bytes(p, sizeof(T), n);
  }

  template <class U>
  bool operator==(const QSBRAllocator<U> &) const noexcept {
    return true;
  }
  template <class U>
  bool operator!=(const QSBRAllocator<U> &) const noexcept {
    return false;
  }
}; // namespace shizalloc
