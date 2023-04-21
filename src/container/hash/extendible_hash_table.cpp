
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  std::shared_ptr<Bucket> bptr = std::make_shared<Bucket>(bucket_size, global_depth_);
  dir_.push_back(bptr);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);

  int idx = IndexOf(key);
  auto tmp = dir_[idx]->Find(key, value);
  return tmp;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);

  int idx = IndexOf(key);
  auto tmp = dir_[idx]->Remove(key);
  return tmp;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  return InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  int idx = IndexOf(key);
  bool insert_result = dir_[idx]->Insert(key, value);

  if (insert_result) {
    return;
  }

  auto local_depth = ExtendibleHashTable<K, V>::GetLocalDepthInternal(idx);
  if (ExtendibleHashTable<K, V>::GetGlobalDepthInternal() == local_depth) {
    ExtendibleHashTable<K, V>::DoubleDir(idx);
    dir_[idx]->IncrementDepth();
    RedistributeBucket(dir_[idx]);
    InsertInternal(key, value);
  } else {
    std::shared_ptr<Bucket> bptr = std::make_shared<Bucket>(bucket_size_, local_depth);
    int mask = (1 << local_depth);

    for (int i = 0; i < 1 << global_depth_; i++) {
      if (dir_[idx] == dir_[i]) {
        if (!((i & mask) == (idx & mask))) {
          dir_[i] = bptr;
        }
      }
    }
    RedistributeBucket(dir_[idx]);
    dir_[idx]->IncrementDepth();
    bptr->IncrementDepth();
    num_buckets_++;
    InsertInternal(key, value);
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::DoubleDir(const int &idx) {
  dir_.resize(1 << (global_depth_ + 1));
  for (int i = 0; i < (1 << global_depth_); i++) {
    dir_[i + (1 << global_depth_)] = dir_[i];
  }

  std::shared_ptr<Bucket> bptr = std::make_shared<Bucket>(bucket_size_, global_depth_ + 1);
  num_buckets_++;

  dir_[idx + (1 << global_depth_)] = bptr;
  ++global_depth_;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) {
  std::list<std::pair<K, V>> list = bucket->GetItems();
  for (const auto &it : list) {
    int idx = IndexOf(it.first);
    if (dir_[idx] != bucket) {
      dir_[idx]->Insert(it.first, it.second);
      bucket->Remove(it.first);
    }
  }
}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  if (list_.size() == size_) {
    return false;
  }
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }

  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
