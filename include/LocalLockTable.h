#if !defined(_LOCAL_LOCK_TABLE_H_)
#define _LOCAL_LOCK_TABLE_H_

#include "Common.h"
#include "Key.h"
#include "Node.h"
#include "GlobalAddress.h"
#include "Hash.h"

#include <queue>
#include <set>
#include <atomic>
#include <mutex>


#define MAX_HANDOVER_TYPE_NUM 2
#define MAX_HOCL_HANDOVER 8
enum HandoverType {
  READ_HANDOVER,
  WRITE_HANDOVER,
};


// 个人认为，acquire/release_lock中对r/w_lock和对current/ticket faa的使用还有一定的商榷空间
struct LocalLockNode {
  // window_start代表当前是否有正在进行的read_window或write_window
  
  // 每次read/write操作，使用cas来判断当前unique_read_key/unique_write_key是否为空
  // 为空则写入当前要read/write的key
  // 如果cas失败且当前unique_read_key/unique_write_key与要read/write的key不相同
  // 则返回代理失败和存在冲突key(上锁失败)，否则对cas成功的返回代理失败和不存在冲突key(成功上锁)

  // 对操作同一个key的不同线程，对read/write ticket进行faa，仅当current增长到ticket时
  // 该线程进入unque_key的检查。这样使得对同一个key的操作顺序进行。
  // 第一个faa成功的线程成为winner，其ticket和current相同，其他所有线程成为follower，
  // 在acquire_local_read/write_lock处被ticket=current的判断阻塞
  
  // 初始时read_window为0，winner将read_handover标志为false，
  // 这样在winner执行完search操作进入release_local_read_lock中时，通过read_handover其可以知道自己时winner，设置node中read_handover的result
  // 同时将window_start和read_window/write_window进行设置。（这里可以阻塞并发write的出现吗？等我看完write_lock相关的）
  // 然后根据是否有follower,winner设置node的read_handover
  // 如果不存在follwer则清空unique_read_key
  // 最后winner将read_window--，同时对read_current faa
  // 然后其他follower依次进入release_local_read_lock（这里牺牲了很多并发性，follower的read都是可以并发的）
  // follower只需要读取read_handover的result，然后更新read_handover（是否是最后一个follower，如果是则设置read_handover为false），并faa current。


  // write lock与read lock完全一致
  // 现在不理解的地方在于，这不就search/write同时在进行吗
  // 理解了。
  // read和write并发竞争window start
  // 竞争成功者设置read window和write window
  // 分别设置为此刻的write follower队列长度和read follower队列长度
  // 这里有一个问题，就是存在线程，在等待ticket对应的current
  // 然后设置window_start的线程并没有同步到其ticket，也就是其不在read_window对应的长度中
  // 然而起仍然执行read_handover，并且仍然能够在release local read lock中获取handover的result
  // 那read_window的意义是什么呢？
  
  // SMART需要的是写代理的线程返回后，不会进入先于当前写代理的读代理
  // 而一个线程成为读写代理的条件是
  // node.read_handover/write_handover = false;
  // 两者当read_window/write_window等于0时，或最后一个follower在release lock中进行了更新
  // 

  // get了
  // write/read window设置为更新window start时读到的read/write ticket - read/write current
  // 标识了当前follower队列的长度。而错过window start节点进入follower队列的线程，虽然更新了ticket
  // 但是在acquire lock中，其按顺序进入对write/read window的检查区域后
  // 会发现write/read window已经减少到0，导致其无法被读写代理
  // 因此在一个线程提交了写代理之后，其读访问不会访问到早于写代理的读代理。
  // 证明：如果遇见了读代理早于写代理，这两者的window start是同一时间，XXX。大概是这个意思。
  // 反正就是解决了本地读写时，本地读不会读到本地写之前的数据。
  // 然而对于不同的client上的线程，一个线程之内的因果一致性能够保证吗？
  // 初始(x=0,y=0) 
  // client 0的线程a，write x->1 , write y->1.
  // client 0的x,y分别被两个写代理，且y在前，x在后
  // client 1则会读到x=0,y=1
  

  // read waiting queue
  std::atomic<uint8_t> read_current;
  std::atomic<uint8_t> read_ticket;
  volatile bool read_handover;

  // write waiting queue
  std::atomic<uint8_t> write_current;
  std::atomic<uint8_t> write_ticket;
  volatile bool write_handover;

  /* ----- auxiliary variables for simplicity  TODO: dynamic allocate ----- */
  // identical time window start time
  std::atomic<bool> window_start;
  // read_window/write_window代表每个window中允许进入的client次数
  // 每个
  std::atomic<uint8_t> read_window; 
  std::atomic<uint8_t> write_window;
  std::mutex r_lock; // 保护对read_window read_current的修改
  std::mutex w_lock; // 保护对write_window write_current的修改

  // hash conflict
  std::atomic<Key*> unique_read_key;
  std::atomic<Key*> unique_write_key;
  GlobalAddress unique_addr;

  // read delegation
  bool res;
  union {
    Value ret_value;
    InternalEntry ret_p;
  };

  // write combining
  std::mutex wc_lock;
  Value wc_buffer;

  // lock handover
  int handover_cnt;

  LocalLockNode() : read_current(0), read_ticket(0), read_handover(0), write_current(0), write_ticket(0), write_handover(0),
                    window_start(0), read_window(0), write_window(0),
                    unique_read_key(0), unique_write_key(0), unique_addr(0), handover_cnt(0) {}
};


class LocalLockTable {
public:
  LocalLockTable() {}

  // read-delegation
  // 返回值：<是否被代理，是否成功上锁/存在冲突key>
  std::pair<bool, bool> acquire_local_read_lock(const Key& k, CoroQueue *waiting_queue = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);
  void release_local_read_lock(const Key& k, std::pair<bool, bool> acquire_ret, bool& res, Value& ret_value);

  // write-combining
  std::pair<bool, bool> acquire_local_write_lock(const Key& k, const Value& v, CoroQueue *waiting_queue = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);
  bool get_combining_value(const Key& k, Value& v);
  void release_local_write_lock(const Key& k, std::pair<bool, bool> acquire_ret);

  /* ---- baseline ---- */
  // lock-handover
  // in_place_update_leaf调用
  bool acquire_local_lock(const GlobalAddress& addr, CoroQueue *waiting_queue = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);
  using RemoteFunc = std::function<void (const GlobalAddress &)>;
  void release_local_lock(const GlobalAddress& addr, RemoteFunc unlock_func);
  void release_local_lock(const GlobalAddress& addr, RemoteFunc unlock_func, RemoteFunc write_without_unlock, RemoteFunc write_and_unlock);

  // cas-handover
  // out_of_place_update_leaf调用
  bool acquire_local_lock(const Key& k, CoroQueue *waiting_queue = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);
  void release_local_lock(const Key& k, bool& res, InternalEntry& ret_p);

  // write_testing
  bool acquire_local_write_lock(const GlobalAddress& addr, const Value& v, CoroQueue *waiting_queue = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);
  using RemoteWriteBackFunc = std::function<void (const Value&)>;
  void release_local_write_lock(const GlobalAddress& addr, RemoteFunc unlock_func, const Value& v, RemoteWriteBackFunc write_func);

  // read-testing
  bool acquire_local_read_lock(const GlobalAddress& addr, CoroQueue *waiting_queue = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);
  void release_local_read_lock(const GlobalAddress& addr, bool& res, Value& ret_value);

private:
  Hash hasher;
  // sizeof(LocalLockNode) = 192
  // define::kLocalLockNum = 4194304
  // 这个锁表一共消耗 ： 192 * 4194304 = 768 MB, 这个开销巨大
  LocalLockNode local_locks[define::kLocalLockNum];
};


// read-delegation
inline std::pair<bool, bool> LocalLockTable::acquire_local_read_lock(const Key& k, CoroQueue *waiting_queue, CoroContext *cxt, int coro_id) {
  auto &node = local_locks[hasher.get_hashed_lock_index(k)];

  Key* unique_key = nullptr;
  Key* new_key = new Key(k);
  bool res = node.unique_read_key.compare_exchange_strong(unique_key, new_key);
  if (!res) {
    delete new_key;
    if (*unique_key != k) {  // conflict keys
      return std::make_pair(false, true);
    }
  }

  uint8_t ticket = node.read_ticket.fetch_add(1);  // acquire local lock
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  // 理解了一下，这里采用ticket + current的加锁方式
  // 尝试上锁时对read_ticket进行faa，保留获取faa之前的值
  // 然后load read_current的值，如果read_current的值和保存的read_ticket旧值相同
  // 说明在ticket faa和current load之间没有并发的acquire local read ticket
  while (ticket != current) { // lock failed
    if (cxt != nullptr) {
      waiting_queue->push(std::make_pair(coro_id, [=, &node](){
        return ticket == node.read_current.load(std::memory_order_relaxed);
      }));
      (*cxt->yield)(*cxt->master);
    }
    current = node.read_current.load(std::memory_order_relaxed);
  }

  // 如果unique_read_key已经被其他线程修改
  unique_key = node.unique_read_key.load();
  if (!unique_key || *unique_key != k) {  // conflict keys
    if (node.read_window) {
      -- node.read_window;
      if (!node.read_window && !node.write_window) {
        // node的read window为零，同时不存在write window
        node.window_start = false;
      }
    }
    // node.read_handover = false;
    node.read_current.fetch_add(1);
    return std::make_pair(false, true);
  }
  if (!node.read_window) {
    node.read_handover = false;
  }
  return std::make_pair(node.read_handover, false);
}

// read-delegation
inline void LocalLockTable::release_local_read_lock(const Key& k, std::pair<bool, bool> acquire_ret, bool& res, Value& ret_value) {
  if (acquire_ret.second) return;

  auto &node = local_locks[hasher.get_hashed_lock_index(k)];

  if (!node.read_handover) {  // winner
    node.res = res;
    node.ret_value = ret_value;
  }
  else {  // losers accept the ret val from winner
    res = node.res;
    ret_value = node.ret_value;
  }

  uint8_t ticket = node.read_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  bool start_window = false;
  if (!node.read_handover && node.window_start.compare_exchange_strong(start_window, true)) {
    // read time window start
    node.read_window = ((1UL << 8) + ticket - current) % (1UL << 8);

    node.w_lock.lock();
    auto w_current = node.write_current.load(std::memory_order_relaxed);
    node.write_window = ((1UL << 8) + node.write_ticket.load(std::memory_order_relaxed) - w_current) % (1UL << 8);
    node.w_lock.unlock();
  }

  node.read_handover = ticket != (uint8_t)(current + 1);

  if (!node.read_handover) {  // next epoch
    node.unique_read_key = nullptr;
  }

  node.r_lock.lock();
  if (node.read_window) {
    -- node.read_window;
    if (!node.read_window && !node.write_window) {
      node.window_start = false;
    }
  }
  node.read_current.fetch_add(1);
  node.r_lock.unlock();

  return;
}

// write-combining
inline std::pair<bool, bool> LocalLockTable::acquire_local_write_lock(const Key& k, const Value& v, CoroQueue *waiting_queue, CoroContext *cxt, int coro_id) {
  auto &node = local_locks[hasher.get_hashed_lock_index(k)];

  Key* unique_key = nullptr;
  Key* new_key = new Key(k);
  bool res = node.unique_write_key.compare_exchange_strong(unique_key, new_key);
  if (!res) {
    delete new_key;
    if (*unique_key != k) {  // conflict keys
      return std::make_pair(false, true);
    } 
  }

  node.wc_lock.lock();
  node.wc_buffer = v;     // local overwrite (combining)
  node.wc_lock.unlock();

  uint8_t ticket = node.write_ticket.fetch_add(1);  // acquire local lock
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  // 如果还有之前对ticket进行了faa的client，没有设置完write_window等信息，则current会落后
  // 问题：不会出现current比ticket超前而导致死循环吗？
  // 不会出现这种问题：进入到这里，保证了是cas一个空的unique_write_key.
  // 保证不存在并发的lock操作。
  // 所有follower被阻塞在此，知道winner完成release
  // follower按照ticket顺序进入
  while (ticket != current) { // lock failed
    if (cxt != nullptr) {
      waiting_queue->push(std::make_pair(coro_id, [=, &node](){
        return ticket == node.write_current.load(std::memory_order_relaxed);
      }));
      (*cxt->yield)(*cxt->master);
    }
    current = node.write_current.load(std::memory_order_relaxed);
  }

  // 通过ticket和current，保证了在一个key上的操作是顺序的一个一个进入下面的check的
  // 有其他线程重新修改了unique_write_key，why？
  // 是因为前面的cas失败，但是发现是同一个key，在尝试进行handover的过程中发现unique write_key发生了改变。
  unique_key = node.unique_write_key.load();
  if (!unique_key || *unique_key != k) {  // conflict keys
    if (node.write_window) {
      -- node.write_window;
      if (!node.read_window && !node.write_window) {
        node.window_start = false;
      }
    }
    // node.write_handover = false;
    node.write_current.fetch_add(1);
    return std::make_pair(false, true);
  }
  // follower队列为空
  if (!node.write_window) {
    node.write_handover = false;
  }
  return std::make_pair(node.write_handover, false);
}

// write-combining
inline bool LocalLockTable::get_combining_value(const Key& k, Value& v) {
  auto &node = local_locks[hasher.get_hashed_lock_index(k)];
  bool res = false;
  Key* unique_key = node.unique_write_key.load();
  if (unique_key && *unique_key == k) {  // wc
    node.wc_lock.lock();
    res = node.wc_buffer != v;
    v = node.wc_buffer;
    node.wc_lock.unlock();
  }
  return res;
}

// write-combining
inline void LocalLockTable::release_local_write_lock(const Key& k, std::pair<bool, bool> acquire_ret) {
  if (acquire_ret.second) return;

  auto &node = local_locks[hasher.get_hashed_lock_index(k)];

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  bool start_window = false;
  if (!node.write_handover && node.window_start.compare_exchange_strong(start_window, true)) {
    // write time window start
    node.r_lock.lock();
    auto r_current = node.read_current.load(std::memory_order_relaxed);
    node.read_window = ((1UL << 8) + node.read_ticket.load(std::memory_order_relaxed) - r_current) % (1UL << 8);
    node.r_lock.unlock();

    node.write_window = ((1UL << 8) + ticket - current) % (1UL << 8);
  }

  node.write_handover = ticket != (uint8_t)(current + 1);

  if (!node.write_handover) {  // next epoch
    node.unique_write_key = nullptr;
  }

  node.w_lock.lock();
  if (node.write_window) {
    -- node.write_window;
    if (!node.read_window && !node.write_window) {
      node.window_start = false;
    }
  }
  node.write_current.fetch_add(1);
  node.w_lock.unlock();

  return;
}

// lock-handover
// in_place_update_leaf调用
// 获取write_ticket，并等待current
// 代理负责设置unique_addr
// 返回是否能被代理以及unique addr是否与addr相等
inline bool LocalLockTable::acquire_local_lock(const GlobalAddress& addr, CoroQueue *waiting_queue, CoroContext *cxt, int coro_id) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  uint8_t ticket = node.write_ticket.fetch_add(1);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (cxt != nullptr) {
      waiting_queue->push(std::make_pair(coro_id, [=, &node](){
        return ticket == node.write_current.load(std::memory_order_relaxed);
      }));
      (*cxt->yield)(*cxt->master);
    }
    current = node.write_current.load(std::memory_order_relaxed);
  }

  if (!node.write_handover) {  // winner
    node.unique_addr = addr;
  }
  // if (node.unique_addr == addr) {
  //   node.handover_cnt ++;
  // }
  return node.write_handover && node.unique_addr == addr;  // only if updating at the same k can this update handover
}

// lock-handover
// in_place_update_leaf调用
// 读取ticket和current设置write_handover
// 对handover_cnt进行++并判断是否超出上限
// winner重置handover_cnt
// 如果unique_addr和传入的addr不相同，则对addr进行unlock
// 如果不存在被写代理的follower，对unique_addr进行unlock
inline void LocalLockTable::release_local_lock(const GlobalAddress& addr, RemoteFunc unlock_func) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  node.write_handover = ticket != (uint8_t)(current + 1);
  if (node.handover_cnt ++ > MAX_HOCL_HANDOVER) {
    node.write_handover = false;
  }
  if (!node.write_handover) {
    node.handover_cnt = 0;
  }

  if (node.unique_addr != addr) {
    unlock_func(addr);
  }
  if (!node.write_handover) {
    unlock_func(node.unique_addr);
  }

  node.write_current.fetch_add(1);
  return;
}

// lock-handover + embedding lock
inline void LocalLockTable::release_local_lock(const GlobalAddress& addr, RemoteFunc unlock_func, RemoteFunc write_without_unlock, RemoteFunc write_and_unlock) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  node.write_handover = ticket != (uint8_t)(current + 1);
  if (node.handover_cnt ++ > MAX_HOCL_HANDOVER) {
    node.write_handover = false;
  }
  if (!node.write_handover) {
    node.handover_cnt = 0;
  }

  if (!node.write_handover) {
    if (node.unique_addr != addr) {
      unlock_func(node.unique_addr);
      write_and_unlock(addr);
    }
    else {
      write_and_unlock(addr);
    }
  }
  else {
    if (node.unique_addr != addr) {
      write_and_unlock(addr);
    }
    else {
      write_without_unlock(addr);
    }
  }

  node.write_current.fetch_add(1);
  return;
}

// cas-handover
// out_of_place_update_leaf调用
inline bool LocalLockTable::acquire_local_lock(const Key& k, CoroQueue *waiting_queue, CoroContext *cxt, int coro_id) {
  auto &node = local_locks[hasher.get_hashed_lock_index(k)];

  uint8_t ticket = node.write_ticket.fetch_add(1);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (cxt != nullptr) {
      waiting_queue->push(std::make_pair(coro_id, [=, &node](){
        return ticket == node.write_current.load(std::memory_order_relaxed);
      }));
      (*cxt->yield)(*cxt->master);
    }
    current = node.write_current.load(std::memory_order_relaxed);
  }

  if (!node.write_handover) {  // winner
    auto old_key = node.unique_write_key.load(std::memory_order_relaxed);
    node.unique_write_key = new Key(k);
    if(old_key) delete old_key;
  }
  // if (*node.unique_write_key == k) {
  //   node.handover_cnt ++;
  // }
  auto unique_key = node.unique_write_key.load(std::memory_order_relaxed);
  return node.write_handover && (unique_key && *unique_key == k);  // only if updating at the same k can this update handover
}

// cas-handover
// out_of_place_update_leaf调用
inline void LocalLockTable::release_local_lock(const Key& k, bool& res, InternalEntry& ret_p) {
  auto &node = local_locks[hasher.get_hashed_lock_index(k)];

  auto unique_key = node.unique_write_key.load(std::memory_order_relaxed);
  if (unique_key && *unique_key == k) {
    if (!node.write_handover) {  // winner
      node.res = res;
      node.ret_p = ret_p;
    }
    else {
      res = node.res;
      ret_p = node.ret_p;
    }
  }

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  node.write_handover = ticket != (uint8_t)(current + 1);
  if (node.handover_cnt ++ > MAX_HOCL_HANDOVER) {
    node.write_handover = false;
  }
  if (!node.write_handover) {
    node.handover_cnt = 0;
  }

  node.write_current.fetch_add(1);
  return;
}

// write-testing
inline bool LocalLockTable::acquire_local_write_lock(const GlobalAddress& addr, const Value& v, CoroQueue *waiting_queue, CoroContext *cxt, int coro_id) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  node.wc_lock.lock();
  node.wc_buffer = v;     // local overwrite (combining)
  node.wc_lock.unlock();

  uint8_t ticket = node.write_ticket.fetch_add(1);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (cxt != nullptr) {
      waiting_queue->push(std::make_pair(coro_id, [=, &node](){
        return ticket == node.write_current.load(std::memory_order_relaxed);
      }));
      (*cxt->yield)(*cxt->master);
    }
    current = node.write_current.load(std::memory_order_relaxed);
  }

  if (!node.write_handover) {  // winner
    node.unique_addr = addr;
  }
  return node.write_handover && node.unique_addr == addr;  // only if updating at the same k can this update handover
}

// write-testing
inline void LocalLockTable::release_local_write_lock(const GlobalAddress& addr, RemoteFunc unlock_func, const Value& v, RemoteWriteBackFunc write_func) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  if (!node.write_handover) {
    // unlock lock_node.unique_key
    node.wc_lock.lock();
    Value wc_v = node.wc_buffer;
    node.wc_lock.unlock();
    write_func(wc_v);
    unlock_func(node.unique_addr);
  }
  if (node.unique_addr != addr) {
    write_func(v);
    unlock_func(addr);
  }

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  node.write_handover = ticket != (uint8_t)(current + 1);

  node.write_current.fetch_add(1);
  return;
}

// read-testing
inline bool LocalLockTable::acquire_local_read_lock(const GlobalAddress& addr, CoroQueue *waiting_queue, CoroContext *cxt, int coro_id) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  uint8_t ticket = node.read_ticket.fetch_add(1);
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (cxt != nullptr) {
      waiting_queue->push(std::make_pair(coro_id, [=, &node](){
        return ticket == node.read_current.load(std::memory_order_relaxed);
      }));
      (*cxt->yield)(*cxt->master);
    }
    current = node.read_current.load(std::memory_order_relaxed);
  }

  if (!node.read_handover) {  // winner
    node.unique_addr = addr;
  }
  return node.read_handover && node.unique_addr == addr;  // only if updating at the same k can this update handover
}

// read-testing
inline void LocalLockTable::release_local_read_lock(const GlobalAddress& addr, bool& res, Value& ret_value) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  uint8_t ticket = node.read_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  if (node.unique_addr == addr) {  // hash conflict clients is not involved in ret value handover
    if (!node.read_handover) {  // winner
      node.res = res;
      node.ret_value = ret_value;
    }
    else {  // losers accept the ret val from winner
      res = node.res;
      ret_value = node.ret_value;
    }
  }
  node.read_handover = ticket != (uint8_t)(current + 1);
  node.read_current.fetch_add(1);
  return;
}

#endif // _LOCAL_LOCK_TABLE_H_
