//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  ValueType value{array_[index].second};
  return value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *target, BufferPoolManager *bpm) {
  int split_size = GetMinSize();
  std::copy(array_ + split_size, array_ + GetSize(), target->array_);
  target->SetSize(GetSize() - split_size);
  SetSize(split_size);

  target->ReSiring(0, target->GetSize(), bpm);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ReSiring(int index, int size, BufferPoolManager *bpm) {
  for (int idx = 0; idx < size; idx++) {
    page_id_t value = array_[index + idx].second;
    auto *node = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(value)->GetData());
    node->SetParentPageId(GetPageId());
    bpm->UnpinPage(value, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int idx = IndexOfKey(key, comparator) + 1;
  if (idx != 0 && idx != GetSize() && comparator(array_[idx].first, key) == 0) {
    return false;
  }

  std::move_backward(array_ + idx, array_ + GetSize(), array_ + GetSize() + 1);
  array_[idx] = std::make_pair(key, value);

  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(int index) {
  std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IndexOfKey(const KeyType &key, const KeyComparator &comparator) -> int {
  int left;
  int right;
  left = 1;
  right = GetSize() - 1;

  while (left <= right) {
    int mid = (right - left) / 2 + left;
    int compare_result1 = comparator(key, array_[mid].first);
    int compare_result2;

    if (mid + 1 == GetSize()) {
      compare_result2 = -1;
    } else {
      compare_result2 = comparator(key, array_[mid + 1].first);
    }

    if (compare_result1 == 0 || (compare_result1 == 1 && compare_result2 == -1)) {
      return mid;
    }

    if (compare_result1 == -1) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  if (right == 0) {
    return 0;
  }

  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Resiring(int index, int size, BufferPoolManager *bpm) {
  for (int idx = 0; idx < size; idx++) {
    ValueType value = array_[idx + index].second;
    auto *node = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(value)->GetData());
    node->SetParentPageId(GetPageId());
    bpm->UnpinPage(value, true);
  }
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
