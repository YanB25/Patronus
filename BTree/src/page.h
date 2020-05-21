#ifndef _BTREE_PAGE_H_
#define _BTREE_PAGE_H_

class page {
private:
  header hdr;                 // header , 64 bytes
  entry records[cardinality]; // slots, 16 bytes * n

public:
  friend class btree;

  page(uint32_t level = 0) {
    hdr.level = level;
    records[0].ptr = NULL;
  }

  // this is called when tree grows
  page(page *left, entry_key_t key, page *right, uint32_t level = 0) {
    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = (char *)right;
    records[1].ptr = NULL;

    hdr.last_index = 0;
  }

  void *operator new(size_t size) {
    void *ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }

  inline int count() { return hdr.last_index + 1; }

  inline bool remove_key(entry_key_t key) {
    // Set the switch_counter
    if (IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    bool shift = false;
    int i;
    for (i = 0; records[i].ptr != NULL; ++i) {
      if (!shift && records[i].key == key) {
        records[i].ptr =
            (i == 0) ? (char *)hdr.leftmost_ptr : records[i - 1].ptr;
        shift = true;
      }

      if (shift) {
        records[i].key = records[i + 1].key;
        records[i].ptr = records[i + 1].ptr;

        uint64_t records_ptr = (uint64_t)(&records[i]);
        // int remainder = records_ptr % CACHE_LINE_SIZE;
      }
    }

    if (shift) {
      --hdr.last_index;
    }
    return shift;
  }

  bool remove(btree *bt, entry_key_t key, bool only_rebalance = false,
              bool with_lock = true) {
    hdr.mtx->lock();

    bool ret = remove_key(key);

    hdr.mtx->unlock();

    return ret;
  }

  inline void insert_key(entry_key_t key, char *ptr, int *num_entries,
                         bool flush = true, bool update_last_index = true) {

    ++hdr.switch_counter;

    // FAST
    if (*num_entries == 0) { // this page is empty
      entry *new_entry = (entry *)&records[0];
      entry *array_end = (entry *)&records[1];
      new_entry->key = (entry_key_t)key;
      new_entry->ptr = (char *)ptr;
      array_end->ptr = (char *)NULL;
    } else {
      int i = *num_entries - 1, inserted = 0;
      records[*num_entries + 1].ptr = records[*num_entries].ptr;

      // FAST
      for (i = *num_entries - 1; i >= 0; i--) {
        if (key < records[i].key) {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = records[i].key;
        } else {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = key;
          records[i + 1].ptr = ptr;
          inserted = 1;
          break;
        }
      }
      if (inserted == 0) {
        records[0].ptr = (char *)hdr.leftmost_ptr;
        records[0].key = key;
        records[0].ptr = ptr;
      }
    }

    if (update_last_index) {
      hdr.last_index = *num_entries;
    }
    ++(*num_entries);
  }

  // Insert a new key - FAST and FAIR
  page *store(btree *bt, char *left, entry_key_t key, char *right, bool flush,
              page *invalid_sibling = NULL) {

    hdr.mtx->lock(); // Lock the write lock

    if (hdr.is_deleted) {
      hdr.mtx->unlock();

      return NULL;
    }

    // If this node has a sibling node,
    if (hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
      // Compare this key with the first key of the sibling
      if (key >= hdr.sibling_ptr->hdr.highest) { // internal node

        hdr.mtx->unlock(); // Unlock the write lock
        return hdr.sibling_ptr->store(bt, NULL, key, right, true,
                                      invalid_sibling);
      }
    }

    register int num_entries = count();

    // FAST
    if (num_entries < cardinality - 1) {
      insert_key(key, right, &num_entries, flush);

      hdr.mtx->unlock(); // Unlock the write lock

      return this;
    } else { // FAIR
      // overflow
      // create a new node
      page *sibling = new page(hdr.level);
      register int m = (int)ceil(num_entries / 2);
      entry_key_t split_key = records[m].key;

      // migrate half of keys into the sibling
      int sibling_cnt = 0;
      if (hdr.leftmost_ptr == NULL) { // leaf node
        for (int i = m; i < num_entries; ++i) {
          sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt,
                              false);
        }
        sibling->hdr.highest = records[m].key;
      } else { // internal node
        for (int i = m + 1; i < num_entries; ++i) {
          sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt,
                              false);
        }
        sibling->hdr.leftmost_ptr = (page *)records[m].ptr;
        sibling->hdr.highest = records[m].key;
      }

      sibling->hdr.sibling_ptr = hdr.sibling_ptr;

      hdr.sibling_ptr = sibling;

      // set to NULL
      if (IS_FORWARD(hdr.switch_counter))
        hdr.switch_counter += 2;
      else
        ++hdr.switch_counter;
      records[m].ptr = NULL;

      hdr.last_index = m - 1;

      num_entries = hdr.last_index + 1;

      page *ret;

      // insert the key
      if (key < split_key) {
        insert_key(key, right, &num_entries);
        ret = this;
      } else {
        sibling->insert_key(key, right, &sibling_cnt);
        ret = sibling;
      }

      // Set a new root or insert the split key to the parent
      if (bt->root == (char *)this) { // only one node can update the root ptr
        page *new_root =
            new page((page *)this, split_key, sibling, hdr.level + 1);
        bt->setNewRoot((char *)new_root);

        hdr.mtx->unlock(); // Unlock the write lock
      } else {
        hdr.mtx->unlock(); // Unlock the write lock
        bt->btree_insert_internal(NULL, split_key, (char *)sibling,
                                  hdr.level + 1);
      }

      return ret;
    }
  }

  // Search keys with linear search
  void linear_search_range(entry_key_t min, entry_key_t max,
                           unsigned long *buf) {
    int i, off = 0;
    uint8_t previous_switch_counter;
    page *current = this;

    while (current) {
      int old_off = off;
      do {
        previous_switch_counter = current->hdr.switch_counter;
        off = old_off;

        entry_key_t tmp_key;
        char *tmp_ptr;

        if (IS_FORWARD(previous_switch_counter)) {
          if ((tmp_key = current->records[0].key) > min) {
            if (tmp_key < max) {
              if ((tmp_ptr = current->records[0].ptr) != NULL) {
                if (tmp_key == current->records[0].key) {
                  if (tmp_ptr) {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            } else
              return;
          }

          for (i = 1; current->records[i].ptr != NULL; ++i) {
            if ((tmp_key = current->records[i].key) > min) {
              if (tmp_key < max) {
                if ((tmp_ptr = current->records[i].ptr) !=
                    current->records[i - 1].ptr) {
                  if (tmp_key == current->records[i].key) {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              } else
                return;
            }
          }
        } else {
          for (i = count() - 1; i > 0; --i) {
            if ((tmp_key = current->records[i].key) > min) {
              if (tmp_key < max) {
                if ((tmp_ptr = current->records[i].ptr) !=
                    current->records[i - 1].ptr) {
                  if (tmp_key == current->records[i].key) {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              } else
                return;
            }
          }

          if ((tmp_key = current->records[0].key) > min) {
            if (tmp_key < max) {
              if ((tmp_ptr = current->records[0].ptr) != NULL) {
                if (tmp_key == current->records[0].key) {
                  if (tmp_ptr) {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            } else
              return;
          }
        }
      } while (previous_switch_counter != current->hdr.switch_counter);

      current = current->hdr.sibling_ptr;
    }
  }

  char *linear_search(entry_key_t key) {
    int i = 1;
    uint8_t previous_switch_counter;
    char *ret = NULL;
    char *t;
    entry_key_t k;

    if (hdr.leftmost_ptr == NULL) { // Search a leaf node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        // search from left ro right
        if (IS_FORWARD(previous_switch_counter)) {
          if ((k = records[0].key) == key) {
            if ((t = records[0].ptr) != NULL) {
              if (k == records[0].key) {
                ret = t;
                continue;
              }
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i) {
            if ((k = records[i].key) == key) {
              if (records[i - 1].ptr != (t = records[i].ptr)) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }
        } else { // search from right to left
          for (i = count() - 1; i > 0; --i) {
            if ((k = records[i].key) == key) {
              if (records[i - 1].ptr != (t = records[i].ptr) && t) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }

          if (!ret) {
            if ((k = records[0].key) == key) {
              if (NULL != (t = records[0].ptr) && t) {
                if (k == records[0].key) {
                  ret = t;
                  continue;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if (ret) {
        return ret;
      }

      if ((t = (char *)hdr.sibling_ptr) && key >= ((page *)t)->hdr.highest)
        return t;

      return NULL;
    } else { // internal node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        if (IS_FORWARD(previous_switch_counter)) {
          if (key < (k = records[0].key)) {
            if ((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
              ret = t;
              continue;
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i) {
            if (key < (k = records[i].key)) {
              if ((t = records[i - 1].ptr) != records[i].ptr) {
                ret = t;
                break;
              }
            }
          }

          if (!ret) {
            ret = records[i - 1].ptr;
            continue;
          }
        } else { // search from right to left
          for (i = count() - 1; i >= 0; --i) {
            if (key >= (k = records[i].key)) {
              if (i == 0) {
                if ((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              } else {
                if (records[i - 1].ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if ((t = (char *)hdr.sibling_ptr) != NULL) {
        if (key >= ((page *)t)->hdr.highest)
          return t;
      }

      if (ret) {
        return ret;
      } else
        return (char *)hdr.leftmost_ptr;
    }

    return NULL;
  }

  // print a node
  void print() {
    if (hdr.leftmost_ptr == NULL)
      printf("[%d] leaf %x \n", this->hdr.level, this);
    else
      printf("[%d] internal %x \n", this->hdr.level, this);
    printf("last_index: %d\n", hdr.last_index);
    printf("switch_counter: %d\n", hdr.switch_counter);
    printf("search direction: ");
    if (IS_FORWARD(hdr.switch_counter))
      printf("->\n");
    else
      printf("<-\n");

    if (hdr.leftmost_ptr != NULL)
      printf("%x ", hdr.leftmost_ptr);

    for (int i = 0; records[i].ptr != NULL; ++i)
      printf("%ld,%x ", records[i].key, records[i].ptr);

    printf("%x ", hdr.sibling_ptr);

    printf("\n");
  }

  void printAll() {
    if (hdr.leftmost_ptr == NULL) {
      printf("printing leaf node: ");
      print();
    } else {
      printf("printing internal node: ");
      print();
      ((page *)hdr.leftmost_ptr)->printAll();
      for (int i = 0; records[i].ptr != NULL; ++i) {
        ((page *)records[i].ptr)->printAll();
      }
    }
  }

  /*
   * Although we implemented the rebalancing of B+-Tree, it is currently blocked
   * for the performance. Please refer to the follow. Chi, P., Lee, W. C., &
   * Xie, Y. (2014, August). Making B+-tree efficient in PCM-based main memory.
   * In Proceedings of the 2014 international symposium on Low power electronics
   * and design (pp. 69-74). ACM.
   */
  bool remove_rebalancing(btree *bt, entry_key_t key,
                          bool only_rebalance = false, bool with_lock = true) {
    if (with_lock) {
      hdr.mtx->lock();
    }
    if (hdr.is_deleted) {
      if (with_lock) {
        hdr.mtx->unlock();
      }
      return false;
    }

    if (!only_rebalance) {
      register int num_entries_before = count();

      // This node is root
      if (this == (page *)bt->root) {
        if (hdr.level > 0) {
          if (num_entries_before == 1 && !hdr.sibling_ptr) {
            bt->root = (char *)hdr.leftmost_ptr;

            hdr.is_deleted = 1;
          }
        }

        // Remove the key from this node
        bool ret = remove_key(key);

        if (with_lock) {
          hdr.mtx->unlock();
        }
        return true;
      }

      bool should_rebalance = true;
      // check the node utilization
      if (num_entries_before - 1 >= (int)((cardinality - 1) * 0.5)) {
        should_rebalance = false;
      }

      // Remove the key from this node
      bool ret = remove_key(key);

      if (!should_rebalance) {
        if (with_lock) {
          hdr.mtx->unlock();
        }
        return (hdr.leftmost_ptr == NULL) ? ret : true;
      }
    }

    // Remove a key from the parent node
    entry_key_t deleted_key_from_parent = 0;
    bool is_leftmost_node = false;
    page *left_sibling;
    bt->btree_delete_internal(key, (char *)this, hdr.level + 1,
                              &deleted_key_from_parent, &is_leftmost_node,
                              &left_sibling);

    if (is_leftmost_node) {
      if (with_lock) {
        hdr.mtx->unlock();
      }

      if (!with_lock) {
        hdr.sibling_ptr->hdr.mtx->lock();
      }
      hdr.sibling_ptr->remove(bt, hdr.sibling_ptr->records[0].key, true,
                              with_lock);
      if (!with_lock) {
        hdr.sibling_ptr->hdr.mtx->unlock();
      }
      return true;
    }

    if (with_lock) {
      left_sibling->hdr.mtx->lock();
    }

    while (left_sibling->hdr.sibling_ptr != this) {
      if (with_lock) {
        page *t = left_sibling->hdr.sibling_ptr;
        left_sibling->hdr.mtx->unlock();
        left_sibling = t;
        left_sibling->hdr.mtx->lock();
      } else
        left_sibling = left_sibling->hdr.sibling_ptr;
    }

    register int num_entries = count();
    register int left_num_entries = left_sibling->count();

    // Merge or Redistribution
    int total_num_entries = num_entries + left_num_entries;
    if (hdr.leftmost_ptr)
      ++total_num_entries;

    entry_key_t parent_key;

    if (total_num_entries > cardinality - 1) { // Redistribution
      register int m = (int)ceil(total_num_entries / 2);

      if (num_entries < left_num_entries) { // left -> right
        if (hdr.leftmost_ptr == nullptr) {
          for (int i = left_num_entries - 1; i >= m; i--) {
            insert_key(left_sibling->records[i].key,
                       left_sibling->records[i].ptr, &num_entries);
          }

          left_sibling->records[m].ptr = nullptr;

          left_sibling->hdr.last_index = m - 1;

          parent_key = records[0].key;
        } else {
          insert_key(deleted_key_from_parent, (char *)hdr.leftmost_ptr,
                     &num_entries);

          for (int i = left_num_entries - 1; i > m; i--) {
            insert_key(left_sibling->records[i].key,
                       left_sibling->records[i].ptr, &num_entries);
          }

          parent_key = left_sibling->records[m].key;

          hdr.leftmost_ptr = (page *)left_sibling->records[m].ptr;

          left_sibling->records[m].ptr = nullptr;

          left_sibling->hdr.last_index = m - 1;
        }

        if (left_sibling == ((page *)bt->root)) {
          page *new_root =
              new page(left_sibling, parent_key, this, hdr.level + 1);
          bt->setNewRoot((char *)new_root);
        } else {
          bt->btree_insert_internal((char *)left_sibling, parent_key,
                                    (char *)this, hdr.level + 1);
        }
      } else { // from leftmost case
        hdr.is_deleted = 1;

        page *new_sibling = new page(hdr.level);
        new_sibling->hdr.mtx->lock();
        new_sibling->hdr.sibling_ptr = hdr.sibling_ptr;

        int num_dist_entries = num_entries - m;
        int new_sibling_cnt = 0;

        if (hdr.leftmost_ptr == nullptr) {
          for (int i = 0; i < num_dist_entries; i++) {
            left_sibling->insert_key(records[i].key, records[i].ptr,
                                     &left_num_entries);
          }

          for (int i = num_dist_entries; records[i].ptr != NULL; i++) {
            new_sibling->insert_key(records[i].key, records[i].ptr,
                                    &new_sibling_cnt, false);
          }

          left_sibling->hdr.sibling_ptr = new_sibling;

          parent_key = new_sibling->records[0].key;
        } else {
          left_sibling->insert_key(deleted_key_from_parent,
                                   (char *)hdr.leftmost_ptr, &left_num_entries);

          for (int i = 0; i < num_dist_entries - 1; i++) {
            left_sibling->insert_key(records[i].key, records[i].ptr,
                                     &left_num_entries);
          }

          parent_key = records[num_dist_entries - 1].key;

          new_sibling->hdr.leftmost_ptr =
              (page *)records[num_dist_entries - 1].ptr;
          for (int i = num_dist_entries; records[i].ptr != NULL; i++) {
            new_sibling->insert_key(records[i].key, records[i].ptr,
                                    &new_sibling_cnt, false);
          }

          left_sibling->hdr.sibling_ptr = new_sibling;
        }

        if (left_sibling == ((page *)bt->root)) {
          page *new_root =
              new page(left_sibling, parent_key, new_sibling, hdr.level + 1);
          bt->setNewRoot((char *)new_root);
        } else {
          bt->btree_insert_internal((char *)left_sibling, parent_key,
                                    (char *)new_sibling, hdr.level + 1);
        }

        new_sibling->hdr.mtx->unlock();
      }
    } else {
      hdr.is_deleted = 1;

      if (hdr.leftmost_ptr)
        left_sibling->insert_key(deleted_key_from_parent,
                                 (char *)hdr.leftmost_ptr, &left_num_entries);

      for (int i = 0; records[i].ptr != NULL; ++i) {
        left_sibling->insert_key(records[i].key, records[i].ptr,
                                 &left_num_entries);
      }

      left_sibling->hdr.sibling_ptr = hdr.sibling_ptr;
    }

    if (with_lock) {
      left_sibling->hdr.mtx->unlock();
      hdr.mtx->unlock();
    }

    return true;
  }
};

#endif