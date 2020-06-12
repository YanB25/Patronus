#include "Tree.h"

#include <iostream>

#include "RdmaBuffer.h"
#include <city.h>

thread_local GlobalAddress path_stack[define::kMaxLevelOfTree];

Tree::Tree(DSM *dsm, uint16_t tree_id) : dsm(dsm), tree_id(tree_id) {

  assert(dsm->is_register());
  print_verbose();

  root_ptr_ptr = get_root_ptr_ptr();

  // try to init tree and install root pointer.
  auto page_buffer = (dsm->get_rbuf()).get_page_buffer();
  auto root_addr = dsm->alloc(kLeafPageSize);
  auto root_page = new (page_buffer) LeafPage;

  root_page->set_consistent();
  dsm->write_sync(page_buffer, root_addr, kLeafPageSize);

  auto cas_buffer = (dsm->get_rbuf()).get_cas_buffer();
  bool res = dsm->cas_sync(root_ptr_ptr, 0, root_addr.val, cas_buffer);
  if (res) {
    std::cout << "Tree root pointer value " << root_addr << std::endl;
  } else {
    // std::cout << "fail\n";
  }
}

void Tree::print_verbose() {

  constexpr int kLeafHdrOffset = STRUCT_OFFSET(LeafPage, hdr);
  constexpr int kInternalHdrOffset = STRUCT_OFFSET(InternalPage, hdr);
  static_assert(kLeafHdrOffset == kInternalHdrOffset, "XXX");

  if (dsm->getMyNodeID() == 0) {
    std::cout << "Header size: " << sizeof(Header) << std::endl;
    std::cout << "Internal Page size: " << sizeof(InternalPage) << " ["
              << kInternalPageSize << "]" << std::endl;
    std::cout << "Internal per Page: " << kInternalCardinality << std::endl;
    std::cout << "Leaf Page size: " << sizeof(LeafPage) << " [" << kLeafPageSize
              << "]" << std::endl;
    std::cout << "Leaf per Page: " << kLeafCardinality << std::endl;
  }
}

GlobalAddress Tree::get_root_ptr_ptr() {
  GlobalAddress addr;
  addr.nodeID = 0;
  addr.offset =
      define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id;

  return addr;
}

GlobalAddress Tree::get_root_ptr() {
  auto page_buffer = (dsm->get_rbuf()).get_page_buffer();
  dsm->read_sync(page_buffer, root_ptr_ptr, sizeof(GlobalAddress));
  GlobalAddress root_ptr = *(GlobalAddress *)page_buffer;

  // std::cout << "root ptr " << root_ptr << std::endl;

  return root_ptr;
}

void Tree::insert(const Key &k, const Value &v) {
  assert(dsm->is_register());

  auto root = get_root_ptr();
  SearchResult result;

  GlobalAddress p = root;

next:

  if (!page_search(p, k, result)) {
    std::cout << "SEARCH WARNING" << std::endl;
    p = get_root_ptr();
    sleep(1);
    goto next;
  }

  if (!result.is_leaf) {
    assert(result.level != 0);
    if (result.slibing != GlobalAddress::Null()) {
      p = result.slibing;
      goto next;
    }

    p = result.next_level;
    if (result.level != 1) {

      goto next;
    }
  }

  leaf_page_store(p, k, v, root, 0);
}

bool Tree::search(const Key &k, Value &v) {
  assert(dsm->is_register());

  auto root = get_root_ptr();
  SearchResult result;

  GlobalAddress p = root;

next:
  if (!page_search(p, k, result)) {
    std::cout << "SEARCH WARNING" << std::endl;
    p = path_stack[result.level + 1];
    goto next;
  }
  if (result.is_leaf) {
    if (result.val != kValueNull) { // find
      v = result.val;
      return true;
    }
    if (result.slibing != GlobalAddress::Null()) { // turn right
      p = result.slibing;
      goto next;
    }
    return false; // not found
  } else {        // internal
    p = result.slibing != GlobalAddress::Null() ? result.slibing
                                                : result.next_level;
    goto next;
  }
}

void Tree::del(const Key &k) {
  assert(dsm->is_register());

  auto root = get_root_ptr();
  SearchResult result;

  GlobalAddress p = root;

next:
  if (!page_search(p, k, result)) {
    std::cout << "SEARCH WARNING" << std::endl;
    p = path_stack[result.level + 1];
    goto next;
  }

  if (!result.is_leaf) {
    assert(result.level != 0);
    if (result.slibing != GlobalAddress::Null()) {
      p = result.slibing;
      goto next;
    }

    p = result.next_level;
    if (result.level != 1) {

      goto next;
    }
  }

  leaf_page_del(p, k, 0);
}

bool Tree::page_search(GlobalAddress page_addr, const Key &k,
                       SearchResult &result) {
  auto page_buffer = (dsm->get_rbuf()).get_page_buffer();

  int counter = 0;
re_read:
  if (++counter > 100) {
    printf("re read too many times\n");
    sleep(1);
  }
  dsm->read_sync(page_buffer, page_addr, kLeafPageSize);

  auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));

  memset(&result, 0, sizeof(result));
  result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
  result.level = header->level;
  path_stack[result.level] = page_addr;
  // std::cout << "level " << (int)result.level << " " << page_addr <<
  // std::endl;

  if (result.is_leaf) {
    auto page = (LeafPage *)page_buffer;
    if (!page->check_consistent()) {
      goto re_read;
    }

    assert(result.level == 0);
    if (k >= page->hdr.highest) { // should turn right
      result.slibing = page->hdr.sibling_ptr;
      return true;
    }
    if (k < page->hdr.lowest) {
      return false;
    }
    leaf_page_search(page, k, result);
  } else {
    auto page = (InternalPage *)page_buffer;
    if (!page->check_consistent()) {
      goto re_read;
    }
    if (k >= page->hdr.highest) { // should turn right
      result.slibing = page->hdr.sibling_ptr;
      return true;
    }
    if (k < page->hdr.lowest) {
      assert(false);
      print_and_check_tree();
      return false;
    }
    internal_page_search(page, k, result);
  }

  return true;
}

void Tree::internal_page_search(InternalPage *page, const Key &k,
                                SearchResult &result) {

  assert(k >= page->hdr.lowest);
  assert(k < page->hdr.highest);

  auto cnt = page->hdr.last_index + 1;
  // page->debug();
  if (k < page->records[0].key) {
    result.next_level = page->hdr.leftmost_ptr;
    return;
  }

  for (int i = 1; i < cnt; ++i) {
    if (k < page->records[i].key) {
      result.next_level = page->records[i - 1].ptr;
      return;
    }
  }
  result.next_level = page->records[cnt - 1].ptr;
}

void Tree::leaf_page_search(LeafPage *page, const Key &k,
                            SearchResult &result) {
  auto cnt = page->hdr.last_index + 1;
  for (int i = 0; i < cnt; ++i) {
    if (page->records[i].key == k) {
      result.val = page->records[i].value;
    }
  }
}

void Tree::internal_page_store(GlobalAddress page_addr, const Key &k,
                               GlobalAddress v, GlobalAddress root, int level) {
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  uint64_t *cas_buffer = dsm->get_rbuf().get_cas_buffer();

retry:
  bool res = dsm->cas_sync(lock_addr, 0, 1, cas_buffer);
  if (!res) {
    // wait
    goto retry;
  }

  auto page_buffer = dsm->get_rbuf().get_page_buffer();
  dsm->read_sync(page_buffer, page_addr, kInternalPageSize);
  auto page = (InternalPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  if (k >= page->hdr.highest) {
    *cas_buffer = 0;
    dsm->write((char *)cas_buffer, lock_addr, sizeof(uint64_t),
               false); // unlock

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->internal_page_store(page->hdr.sibling_ptr, k, v, root, level);
  }

  auto cnt = page->hdr.last_index + 1;

  bool is_update = false;
  uint16_t insert_index = 0;
  for (int i = cnt - 1; i >= 0; --i) {
    if (page->records[i].key == k) { // find and update
      page->records[i].ptr = v;

      is_update = true;
      break;
    }
    if (page->records[i].key < k) {
      insert_index = i + 1;
      break;
    }
  }

  assert(cnt != kInternalCardinality);

  if (!is_update) { // insert and shift
    for (int i = cnt; i > insert_index; --i) {
      page->records[i].key = page->records[i - 1].key;
      page->records[i].ptr = page->records[i - 1].ptr;
    }
    page->records[insert_index].key = k;
    page->records[insert_index].ptr = v;

    page->hdr.last_index++;
  }

  cnt = page->hdr.last_index + 1;
  bool need_split = cnt == kInternalCardinality;
  Key split_key;
  GlobalAddress sibling_addr;
  if (need_split) { // need split
    sibling_addr = dsm->alloc(kInternalPageSize);
    auto sibling_buf = dsm->get_rbuf().get_sibling_buffer();

    auto sibling = new (sibling_buf) InternalPage(page->hdr.level);

    //    std::cout << "addr " <<  sibling_addr << " | level " <<
    //    (int)(page->hdr.level) << std::endl;

    int m = cnt / 2;
    split_key = page->records[m].key;
    for (int i = m + 1; i < cnt; ++i) { // move
      sibling->records[i - m].key = page->records[i].key;
      sibling->records[i - m].ptr = page->records[i].ptr;
    }
    page->hdr.last_index -= (cnt - m);
    sibling->hdr.last_index += (cnt - m - 1);

    sibling->hdr.leftmost_ptr = page->records[m].ptr;
    sibling->hdr.lowest = page->records[m].key;
    sibling->hdr.highest = page->hdr.highest;
    page->hdr.highest = page->records[m].key;

    // link
    sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
    page->hdr.sibling_ptr = sibling_addr;

    sibling->set_consistent();
    dsm->write_sync(sibling_buf, sibling_addr, kInternalPageSize);
  }

  page->set_consistent();
  dsm->write_sync(page_buffer, page_addr, kInternalPageSize);
  *cas_buffer = 0;
  dsm->write((char *)cas_buffer, lock_addr, sizeof(uint64_t),
             false); // unlock, async

  if (!need_split)
    return;

  if (root == page_addr) { // update root
    auto new_root = new (page_buffer)
        InternalPage(page_addr, split_key, sibling_addr, level + 1);

    auto new_root_addr = dsm->alloc(kInternalPageSize);

    new_root->set_consistent();
    dsm->write_sync(page_buffer, new_root_addr, kInternalPageSize);
    if (dsm->cas_sync(root_ptr_ptr, root, new_root_addr, cas_buffer)) {

      std::cout << "new root level " << level + 1 << std::endl;
      return;
    }
  }

  internal_page_store(path_stack[level + 1], split_key, sibling_addr, root,
                      level + 1);
}

void Tree::leaf_page_store(GlobalAddress page_addr, const Key &k,
                           const Value &v, GlobalAddress root, int level) {
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  uint64_t *cas_buffer = dsm->get_rbuf().get_cas_buffer();

retry:
  bool res = dsm->cas_sync(lock_addr, 0, 1, cas_buffer);
  if (!res) {
    // wait
    goto retry;
  }

  auto page_buffer = dsm->get_rbuf().get_page_buffer();
  dsm->read_sync(page_buffer, page_addr, kLeafPageSize);
  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  if (k >= page->hdr.highest) {
    *cas_buffer = 0;
    dsm->write((char *)cas_buffer, lock_addr, sizeof(uint64_t),
               false); // unlock

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->leaf_page_store(page->hdr.sibling_ptr, k, v, root, level);
  }

  auto cnt = page->hdr.last_index + 1;

  bool is_update = false;
  uint16_t insert_index = 0;
  for (int i = cnt - 1; i >= 0; --i) {
    if (page->records[i].key == k) { // find and update
      page->records[i].value = v;

      is_update = true;
      break;
    }
    if (page->records[i].key < k) {
      insert_index = i + 1;
      break;
    }
  }

  assert(cnt != kLeafCardinality);

  if (!is_update) { // insert and shift
    for (int i = cnt; i > insert_index; --i) {
      page->records[i].key = page->records[i - 1].key;
      page->records[i].value = page->records[i - 1].value;
    }
    page->records[insert_index].key = k;
    page->records[insert_index].value = v;

    page->hdr.last_index++;
  }

  cnt = page->hdr.last_index + 1;
  bool need_split = cnt == kLeafCardinality;
  Key split_key;
  GlobalAddress sibling_addr;
  if (need_split) { // need split
    sibling_addr = dsm->alloc(kLeafPageSize);
    auto sibling_buf = dsm->get_rbuf().get_sibling_buffer();

    auto sibling = new (sibling_buf) LeafPage(page->hdr.level);

    // std::cout << "addr " <<  sibling_addr << " | level " <<
    // (int)(page->hdr.level) << std::endl;

    int m = cnt / 2;
    split_key = page->records[m].key;
    for (int i = m; i < cnt; ++i) { // move
      sibling->records[i - m].key = page->records[i].key;
      sibling->records[i - m].value = page->records[i].value;
    }
    page->hdr.last_index -= (cnt - m);
    sibling->hdr.last_index += (cnt - m);

    sibling->hdr.lowest = page->records[m].key;
    sibling->hdr.highest = page->hdr.highest;
    page->hdr.highest = page->records[m].key;

    // link
    sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
    page->hdr.sibling_ptr = sibling_addr;

    sibling->set_consistent();
    dsm->write_sync(sibling_buf, sibling_addr, kLeafPageSize);
  }

  page->set_consistent();
  dsm->write_sync(page_buffer, page_addr, kLeafPageSize);
  *cas_buffer = 0;
  dsm->write((char *)cas_buffer, lock_addr, sizeof(uint64_t),
             false); // unlock, async

  if (!need_split)
    return;

  if (root == page_addr) { // update root
    auto new_root = new (page_buffer)
        InternalPage(page_addr, split_key, sibling_addr, level + 1);
    auto new_root_addr = dsm->alloc(kInternalPageSize);

    new_root->set_consistent();
    dsm->write_sync(page_buffer, new_root_addr, kInternalPageSize);
    if (dsm->cas_sync(root_ptr_ptr, root, new_root_addr, cas_buffer)) {
      //
      std::cout << "new root level " << level + 1 << std::endl;
      return;
    }
  }

  internal_page_store(path_stack[level + 1], split_key, sibling_addr, root,
                      level + 1);
}

void Tree::leaf_page_del(GlobalAddress page_addr, const Key &k, int level) {
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  uint64_t *cas_buffer = dsm->get_rbuf().get_cas_buffer();

retry:
  bool res = dsm->cas_sync(lock_addr, 0, 1, cas_buffer);
  if (!res) {
    // wait
    goto retry;
  }

  auto page_buffer = dsm->get_rbuf().get_page_buffer();
  dsm->read_sync(page_buffer, page_addr, kLeafPageSize);
  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  if (k >= page->hdr.highest) {
    *cas_buffer = 0;
    dsm->write((char *)cas_buffer, lock_addr, sizeof(uint64_t),
               false); // unlock

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->leaf_page_del(page->hdr.sibling_ptr, k, level);
  }

  auto cnt = page->hdr.last_index + 1;

  int del_index = -1;
  for (int i = 0; i < cnt; ++i) {
    if (page->records[i].key == k) { // find and update
      del_index = i;
      break;
    }
  }

  if (del_index != -1) { // remove and shift
    for (int i = del_index + 1; i < cnt; ++i) {
      page->records[i - 1].key = page->records[i].key;
      page->records[i - 1].value = page->records[i].value;
    }

    page->hdr.last_index--;

    page->set_consistent();
    dsm->write_sync(page_buffer, page_addr, kLeafPageSize);
  }

  *cas_buffer = 0;
  dsm->write((char *)cas_buffer, lock_addr, sizeof(uint64_t),
             false); // unlock, async
}