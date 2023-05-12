#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
INDEX_TEMPLATE_ARGUMENTS
using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  LOG_INFO("B_plus_tree initing: leaf_size %d , internal_size %d pool size %zu", leaf_max_size, internal_max_size,
           buffer_pool_manager_->GetPoolSize());
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  printf("GetVALUE %ld\n", key.ToString());
  auto *leaf_page = FindLeafPage(key, Operation::SEARCH, transaction);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int idx = leaf_node->IndexOfKey(key, comparator_);
  if (idx == -1 || idx == leaf_node->GetSize()) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    leaf_page->RUnlatch();
    return false;
  }

  if (comparator_(key, leaf_node->KeyAt(idx)) != 0) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    leaf_page->RUnlatch();
    return false;
  }
  result->push_back(leaf_node->ValueAt(idx));
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  leaf_page->RUnlatch();
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Operation operation, Transaction *transaction) -> Page * {
  if (operation == Operation::SEARCH) {
    root_page_id_lock_.RLock();
    auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
    root_page_id_lock_.RUnlock();
    BUSTUB_ASSERT(page != nullptr, "can't fetch page");
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    page->RLatch();
    // LOG_INFO("Root page %d unlock", root_page_id_);
    // root_page_id_lock_.RUnlock();

    Page *child_page;
    int idx;
    while (!node->IsLeafPage()) {
      auto *internal_node = reinterpret_cast<InternalPage *>(node);
      idx = internal_node->IndexOfKey(key, comparator_);
      child_page = buffer_pool_manager_->FetchPage(internal_node->ValueAt(idx));
      BUSTUB_ASSERT(child_page != nullptr, "can't fetch page");
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(node->GetPageId(), false);

      child_page->RLatch();
      page = child_page;
      node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    }
    return page;
  }
  if (operation == Operation::DELETE) {
    // can fuyong daima
    root_page_id_lock_.WLock();
    transaction->AddIntoPageSet(nullptr);

    auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
    BUSTUB_ASSERT(page != nullptr, "can't fetch page");
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    page->WLatch();

    int idx;
    while (!node->IsLeafPage()) {
      auto *internal_node = reinterpret_cast<InternalPage *>(node);
      if (node->GetSize() > node->GetMinSize()) {
        ReleaseLatchFromQueue(transaction);
      }
      transaction->AddIntoPageSet(page);

      idx = internal_node->IndexOfKey(key, comparator_);
      page = buffer_pool_manager_->FetchPage(internal_node->ValueAt(idx));
      BUSTUB_ASSERT(page != nullptr, "can't fetch page");
      page->WLatch();
      node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    }

    if (node->GetSize() > node->GetMinSize()) {
      ReleaseLatchFromQueue(transaction);
    }
    return page;
  }
  if (operation == Operation::INSERT) {
    root_page_id_lock_.WLock();
    transaction->AddIntoPageSet(nullptr);

    auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
    BUSTUB_ASSERT(page != nullptr, "can't fetch page");
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    page->WLatch();

    int idx;
    while (!node->IsLeafPage()) {
      auto *internal_node = reinterpret_cast<InternalPage *>(node);
      if (node->GetSize() < node->GetMaxSize()) {
        ReleaseLatchFromQueue(transaction);
      }
      transaction->AddIntoPageSet(page);

      idx = internal_node->IndexOfKey(key, comparator_);
      page = buffer_pool_manager_->FetchPage(internal_node->ValueAt(idx));
      BUSTUB_ASSERT(page != nullptr, "can't fetch page");
      page->WLatch();
      node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    }

    if (node->GetSize() < node->GetMaxSize() - 1) {
      ReleaseLatchFromQueue(transaction);
    }
    return page;
  }

  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatchFromQueue(Transaction *transaction) {
  while (!transaction->GetPageSet()->empty()) {
    auto *page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    if (page == nullptr) {
      root_page_id_lock_.WUnlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
}

// TODO(lrx): DEBUG,ADD LATCH_
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftMostLeafPage() -> Page * {
  root_page_id_lock_.RLock();
  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  root_page_id_lock_.RUnlock();
  BUSTUB_ASSERT(page != nullptr, "can't fetch page");
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  page->RLatch();

  while (!node->IsLeafPage()) {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *child_page = buffer_pool_manager_->FetchPage(internal_node->ValueAt(0));
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    BUSTUB_ASSERT(child_page != nullptr, "can't fetch page");

    child_page->RLatch();
    page = child_page;
    node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  }

  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindRightMostLeafPage() -> Page * {
  root_page_id_lock_.RLock();
  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  root_page_id_lock_.RUnlock();
  BUSTUB_ASSERT(page != nullptr, "can't fetch page");
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  page->RLatch();

  int idx;
  while (!node->IsLeafPage()) {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    idx = internal_node->GetSize() - 1;
    auto *child_page = buffer_pool_manager_->FetchPage(internal_node->ValueAt(idx));
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    BUSTUB_ASSERT(child_page != nullptr, "can't fetch page");
    child_page->RLatch();
    page = child_page;
    node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  }

  return page;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // LOG_INFO("Insert %ld", key.ToString());
  printf("Insert %ld\n", key.ToString());
  root_page_id_lock_.WLock();

  if (IsEmpty()) {
    auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    BUSTUB_ASSERT(root_page != nullptr, "buffer pool out of page");

    // LOG_INFO("root page latch");
    // root_page->WLatch();
    auto *root_node = reinterpret_cast<LeafPage *>(root_page->GetData());
    root_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    root_node->Insert(key, value, comparator_);
    // LOG_INFO("root page unlatch");
    // root_page->WUnlatch();

    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId(1);

    // LOG_INFO("Root page %d unlock", root_page_id_);
    // root_page_id_lock_.WUnlock();
    root_page_id_lock_.WUnlock();
    return true;
  }
  root_page_id_lock_.WUnlock();
  // Find leaf node

  auto leaf_page = FindLeafPage(key, Operation::INSERT, transaction);  // have changed auto * ->auto
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  if (!leaf_node->Insert(key, value, comparator_)) {
    ReleaseLatchFromQueue(transaction);

    // if (leaf_page->GetPageId() == root_page_id_) {
    //   root_page_id_lock_.WUnlock();
    // }
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return false;
  }

  // spliting condition
  if (leaf_node->GetSize() == leaf_max_size_) {
    page_id_t new_leaf_page_id;
    auto *new_leaf_page = buffer_pool_manager_->NewPage(&new_leaf_page_id);

    BUSTUB_ASSERT(new_leaf_page != nullptr, "buffer pool out of page");
    auto *new_leaf_node = reinterpret_cast<LeafPage *>(new_leaf_page->GetData());

    new_leaf_node->Init(new_leaf_page_id, leaf_node->GetParentPageId(), leaf_max_size_);
    new_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->MoveHalfTo(new_leaf_node);
    leaf_node->SetNextPageId(new_leaf_page_id);
    auto insert_into_parent = new_leaf_node->KeyAt(0);
    InsertInParent(leaf_node, insert_into_parent, new_leaf_page_id, transaction);

    buffer_pool_manager_->UnpinPage(new_leaf_page_id, true);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  } else {
    ReleaseLatchFromQueue(transaction);
    // if (leaf_page->GetPageId() == root_page_id_) {
    //   root_page_id_lock_.WUnlock();
    // }
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *child_node, const KeyType &key, const page_id_t &value,
                                    Transaction *transaction) {
  if (child_node->GetParentPageId() == INVALID_PAGE_ID) {
    page_id_t new_root_page_id;

    auto *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);

    BUSTUB_ASSERT(new_root_page != nullptr, "buffer pool out of page");
    auto *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());

    new_root_node->SetValueAt(0, child_node->GetPageId());
    new_root_node->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_node->Insert(key, value, comparator_);

    // can be optimized
    // child_node->SetParentPageId(new_root_page_id);
    new_root_node->ReSiring(0, new_root_node->GetSize() /*2*/, buffer_pool_manager_);

    //  LOG_INFO("Root page %d lock", root_page_id_);
    root_page_id_ = new_root_page_id;

    // if (child_node -> IsLeafPage()) {
    //   root_page_id_lock_.WUnlock();
    // }

    UpdateRootPageId(0);

    ReleaseLatchFromQueue(transaction);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);  // this should matter where it is
    return;
  }

  auto *parent_page = FindParentPage(child_node);
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page);

  // spliting condition
  if (parent_node->GetSize() == parent_node->GetMaxSize()) {
    std::pair<KeyType, page_id_t> large_array[parent_node->GetMaxSize() + 1];  // use smart poiters
    std::pair<KeyType, page_id_t> *parent_array = parent_node->GetItems();
    std::copy(parent_array, parent_array + parent_node->GetSize(), large_array);

    int index = parent_node->IndexOfKey(key, comparator_) + 1;

    std::move_backward(large_array + index, large_array + parent_node->GetSize(),
                       large_array + parent_node->GetSize() + 1);
    large_array[index] = std::make_pair(key, value);

    page_id_t new_page_id;
    auto *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    BUSTUB_ASSERT(new_page != nullptr, "buffer pool out of page");
    auto *new_page_node = reinterpret_cast<InternalPage *>(new_page->GetData());

    new_page_node->Init(new_page_id, parent_node->GetParentPageId(), internal_max_size_);

    int min_size = parent_node->GetMinSize();
    int max_size = parent_node->GetMaxSize();
    std::copy(large_array, large_array + min_size, parent_node->GetItems());
    parent_node->SetSize(min_size);
    // can be optimized
    parent_node->ReSiring(0, min_size, buffer_pool_manager_);

    std::copy(large_array + min_size, large_array + max_size + 1, new_page_node->GetItems());
    new_page_node->SetSize(min_size);
    new_page_node->ReSiring(0, min_size, buffer_pool_manager_);
    KeyType new_key = large_array[min_size].first;
    InsertInParent(reinterpret_cast<BPlusTreePage *>(parent_node), new_key, new_page_id, transaction);
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_page_node->GetPageId(), true);
  } else {
    parent_node->Insert(key, value, comparator_);
    ReleaseLatchFromQueue(transaction);
    // parent_page -> WUnlatch(); //maybe no need???
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

// checked until here
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // LOG_INFO("Remove %ld", key.ToString());
  printf("Remove %ld\n", key.ToString());
  if (IsEmpty()) {
    return;
  }
  // can be optimized
  auto *leaf_page = FindLeafPage(key, Operation::DELETE, transaction);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  auto key_in_leaf_node = leaf_node->IndexOfKey(key, comparator_);
  if (comparator_(leaf_node->KeyAt(key_in_leaf_node), key) != 0) {
    ReleaseLatchFromQueue(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return;
  }
  leaf_node->Delete(key_in_leaf_node);
  // if the leaf node is root node
  if (leaf_node->IsRootPage()) {
    ReleaseLatchFromQueue(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    return;
  }
  auto *parent_page = FindParentPage(leaf_node);
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  auto leaf_node_index = parent_node->IndexOfKey(key, comparator_);

  if (leaf_node->GetSize() < leaf_node->GetMinSize()) {
    LeafPage *left_sibling_node = nullptr;
    LeafPage *right_sibling_node = nullptr;
    // debug [[maybe]] add locks?
    if (leaf_node_index != 0) {
      auto *left_sibing_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(leaf_node_index - 1));
      BUSTUB_ASSERT(left_sibing_page != nullptr, "can't fetch page");
      left_sibling_node = reinterpret_cast<LeafPage *>(left_sibing_page->GetData());
    }

    if (leaf_node_index != parent_node->GetSize() - 1) {
      auto *right_sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(leaf_node_index + 1));
      BUSTUB_ASSERT(right_sibling_page != nullptr, "can't fetch page");
      right_sibling_node = reinterpret_cast<LeafPage *>(right_sibling_page->GetData());
    }

    if (left_sibling_node && left_sibling_node->GetSize() > left_sibling_node->GetMinSize()) {
      StoleFrom<LeafPage>(leaf_node, left_sibling_node, parent_node, leaf_node_index, 1, transaction);
      ReleaseLatchFromQueue(transaction);
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
      if (right_sibling_node) {
        buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
      }
      leaf_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
      return;
    }

    if (right_sibling_node && right_sibling_node->GetSize() > right_sibling_node->GetMinSize()) {
      StoleFrom<LeafPage>(leaf_node, right_sibling_node, parent_node, leaf_node_index, 0, transaction);
      ReleaseLatchFromQueue(transaction);
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
      if (left_sibling_node) {
        buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
      }
      leaf_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
      return;
    }

    if (left_sibling_node) {
      auto next_page = leaf_node->GetNextPageId();
      left_sibling_node->SetNextPageId(next_page);
      MergeFrom<LeafPage>(leaf_node, left_sibling_node, leaf_node_index, 1);
      RemoveInternalPageKey(parent_node, leaf_node_index, transaction);
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
      if (right_sibling_node) {
        buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
      }
      leaf_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    } else if (right_sibling_node) {
      auto next_page = right_sibling_node->GetNextPageId();
      leaf_node->SetNextPageId(next_page);
      MergeFrom<LeafPage>(leaf_node, right_sibling_node, leaf_node_index, 0);
      RemoveInternalPageKey(parent_node, leaf_node_index + 1, transaction);
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
      if (left_sibling_node) {
        buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);  // should be false
      }
      leaf_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    }
  } else {
    ReleaseLatchFromQueue(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename PageType>
void BPLUSTREE_TYPE::StoleFrom(PageType *target_node, PageType *source_node, InternalPage *parent_node, int index,
                               bool is_left, Transaction *transaction) {
  auto *target_array = target_node->GetItems();
  auto *source_array = source_node->GetItems();
  if (is_left) {
    std::move_backward(target_array, target_array + target_node->GetSize(), target_array + target_node->GetSize() + 1);
    target_array[0] = source_array[source_node->GetSize() - 1];
    if constexpr (std::is_same<PageType, InternalPage>()) {
      target_node->ReSiring(0, 1, buffer_pool_manager_);
    }
    target_node->IncreaseSize(1);
    source_node->IncreaseSize(-1);
    parent_node->SetKeyAt(index, target_array[0].first);
  } else {
    target_array[target_node->GetSize()] = source_array[0];
    std::move(source_array + 1, source_array + source_node->GetSize(), source_array);
    if constexpr (std::is_same<PageType, InternalPage>()) {
      target_node->ReSiring(target_node->GetSize(), 1, buffer_pool_manager_);
    }
    target_node->IncreaseSize(1);
    source_node->IncreaseSize(-1);
    parent_node->SetKeyAt(index + 1, source_array[0].first);
  }
  // ReleaseLatchFromQueue(transaction);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename PageType>
void BPLUSTREE_TYPE::MergeFrom(PageType *target_node, PageType *source_node, int index, bool is_left) {
  assert(source_node != nullptr);
  auto *target_array = target_node->GetItems();
  auto *source_array = source_node->GetItems();
  if (is_left) {
    std::copy(target_array, target_array + target_node->GetSize(), source_array + source_node->GetSize());
    if constexpr (std::is_same<PageType, InternalPage>()) {
      source_node->ReSiring(source_node->GetSize(), target_node->GetSize(), buffer_pool_manager_);
    }
    source_node->IncreaseSize(target_node->GetSize());
    target_node->SetSize(0);
  } else {
    std::copy(source_array, source_array + source_node->GetSize(), target_array + target_node->GetSize());
    if constexpr (std::is_same<PageType, InternalPage>()) {
      target_node->ReSiring(target_node->GetSize(), source_node->GetSize(), buffer_pool_manager_);
    }
    target_node->IncreaseSize(source_node->GetSize());
    source_node->SetSize(0);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveInternalPageKey(InternalPage *target_node, int index, Transaction *transaction) {
  target_node->Delete(index);

  if (target_node->IsRootPage()) {
    if (target_node->GetSize() == 1) {
      root_page_id_ = target_node->ValueAt(0);
      UpdateRootPageId(0);
      auto *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
      BUSTUB_ASSERT(root_page != nullptr, "can't fetch page");
      auto *root_node = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
      root_node->SetParentPageId(INVALID_PAGE_ID);
      ReleaseLatchFromQueue(transaction);
      // buffer_pool_manager_->UnpinPage(target_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    } else {
      ReleaseLatchFromQueue(transaction);
      // buffer_pool_manager_->UnpinPage(target_node->GetPageId(), true);
    }
    return;  // debug ,if is root and is less than min size merge
  }
  auto *parent_page = FindParentPage(target_node);
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  auto target_node_index = parent_node->IndexOfKey(target_node->KeyAt(1), comparator_);
  if (target_node->GetSize() < target_node->GetMinSize()) {
    InternalPage *left_sibling_node = nullptr;
    InternalPage *right_sibling_node = nullptr;
    if (target_node_index != 0) {
      left_sibling_node = reinterpret_cast<InternalPage *>(
          buffer_pool_manager_->FetchPage(parent_node->ValueAt(target_node_index - 1))->GetData());
    }

    if (target_node_index != parent_node->GetSize() - 1) {
      right_sibling_node = reinterpret_cast<InternalPage *>(
          buffer_pool_manager_->FetchPage(parent_node->ValueAt(target_node_index + 1))->GetData());
    }

    if (left_sibling_node && left_sibling_node->GetSize() > left_sibling_node->GetMinSize()) {
      StoleFrom(target_node, left_sibling_node, parent_node, target_node_index, 1, transaction);
      ReleaseLatchFromQueue(transaction);
      // buffer_pool_manager_->UnpinPage(target_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
      if (right_sibling_node) {
        buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
      }
      return;
    }

    if (right_sibling_node && right_sibling_node->GetSize() > right_sibling_node->GetMinSize()) {
      StoleFrom(target_node, right_sibling_node, parent_node, target_node_index, 0, transaction);
      ReleaseLatchFromQueue(transaction);
      // buffer_pool_manager_->UnpinPage(target_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
      if (left_sibling_node) {
        buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
      }
      return;
    }

    if (left_sibling_node) {
      MergeFrom(target_node, left_sibling_node, target_node_index, 1);
      RemoveInternalPageKey(parent_node, target_node_index, transaction);
      // buffer_pool_manager_->UnpinPage(target_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
      if (right_sibling_node) {
        buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
      }
    } else if (right_sibling_node) {
      MergeFrom(target_node, right_sibling_node, target_node_index, 0);
      RemoveInternalPageKey(parent_node, target_node_index + 1, transaction);
      // buffer_pool_manager_->UnpinPage(target_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
      if (left_sibling_node) {
        buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
      }
    }
  } else {
    ReleaseLatchFromQueue(transaction);
    // buffer_pool_manager_->UnpinPage(target_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // LOG_INFO("Get iterator at beginning");
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(-1, nullptr, nullptr);
  }
  auto *leaf_page = FindLeftMostLeafPage();
  return INDEXITERATOR_TYPE(0, leaf_page, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // LOG_INFO("Get iterator at key %ld", key.ToString());
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(-1, nullptr, nullptr);
  }
  auto *leaf_page = FindLeafPage(key, Operation::SEARCH);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  auto index = leaf_node->IndexOfKey(key, comparator_);
  return INDEXITERATOR_TYPE(index, leaf_page, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(-1, nullptr, nullptr);
  }
  auto *leaf_page = FindRightMostLeafPage();
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  return INDEXITERATOR_TYPE(leaf_node->GetSize(), leaf_page, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindParentPage(const BPlusTreePage *child_node) const -> Page * {
  auto *parent_page = buffer_pool_manager_->FetchPage(child_node->GetParentPageId());
  BUSTUB_ASSERT(parent_page != nullptr, "can't fetch page");
  return parent_page;
}
/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
