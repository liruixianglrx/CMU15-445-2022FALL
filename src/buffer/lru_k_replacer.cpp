//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto it = history_.rbegin(); it != history_.rend(); it++) {
    if (is_evictable_[*it]) {
      *frame_id = *it;
      records_.erase(*frame_id);
      history_.erase(history_map_[*frame_id]);
      history_map_.erase(*frame_id);
      curr_size_--;
      is_evictable_.erase(*frame_id);
      return true;
    }
  }

  for (auto it = cache_.rbegin(); it != cache_.rend(); it++) {
    if (is_evictable_[*it]) {
      *frame_id = *it;
      records_.erase(*frame_id);
      cache_.erase(cache_map_[*frame_id]);
      cache_map_.erase(*frame_id);
      curr_size_--;
      is_evictable_.erase(*frame_id);
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "frame id invalid");

  if (records_[frame_id] == 0) {
    is_evictable_[frame_id] = true;
    records_[frame_id]++;
    curr_size_++;
    history_.push_front(frame_id);
    history_map_[frame_id] = history_.begin();
  } else if (records_[frame_id] < k_ - 1) {
    records_[frame_id]++;
  } else if (records_[frame_id] == k_ - 1) {
    records_[frame_id]++;
    auto it = history_map_[frame_id];
    history_.erase(it);
    history_map_.erase(frame_id);

    cache_.push_front(frame_id);
    cache_map_[frame_id] = cache_.begin();
  } else {
    auto it = cache_map_[frame_id];
    cache_.erase(it);
    cache_map_.erase(frame_id);

    cache_.push_front(frame_id);
    cache_map_[frame_id] = cache_.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "frame id invalid");

  if (records_[frame_id] == 0) {
    return;
  }

  if (!set_evictable && is_evictable_[frame_id]) {
    curr_size_--;
  }

  if (set_evictable && !is_evictable_[frame_id]) {
    curr_size_++;
  }

  is_evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (history_map_.find(frame_id) != history_map_.end()) {
    auto it = history_map_[frame_id];
    records_.erase(frame_id);
    if (is_evictable_[frame_id]) {
      curr_size_--;
      is_evictable_.erase(frame_id);
    }
    history_map_.erase(frame_id);
    history_.erase(it);
  }

  if (cache_map_.find(frame_id) != cache_map_.end()) {
    auto it = cache_map_[frame_id];
    records_.erase(frame_id);
    if (is_evictable_[frame_id]) {
      curr_size_--;
      is_evictable_.erase(frame_id);
    }
    cache_map_.erase(frame_id);
    cache_.erase(it);
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
