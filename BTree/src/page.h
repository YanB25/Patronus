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

    hdr.lowest = kKeyMin;
    hdr.highest = kKeyMax;
  }

  // this is called when tree grows
  page(page *left, entry_key_t key, page *right, uint32_t level = 0) {
    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = (char *)right;
    records[1].ptr = NULL;

    hdr.last_index = 0;

    hdr.lowest = kKeyMin;
    hdr.highest = kKeyMax;
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
  page *store(btree *bt, char *left, entry_key_t key, char *right,
              page *invalid_sibling = NULL) {

    hdr.mtx->lock(); // Lock the write lock

    if (hdr.is_deleted) {
      hdr.mtx->unlock();
      return NULL;
    }

    // If this node has a sibling node,
    // Compare this key with the first key of the sibling
    if (key >= hdr.highest) { // internal node

      hdr.mtx->unlock(); // Unlock the write lock
      assert(hdr.sibling_ptr != nullptr);
      return hdr.sibling_ptr->store(bt, NULL, key, right, invalid_sibling);
    }

    register int num_entries = count();

    hdr.switch_counter++;
    compiler_barrier();

    // FAST
    if (num_entries < cardinality - 1) {
      insert_key(key, right, &num_entries);

      hdr.mtx->unlock(); // Unlock the write lock

      compiler_barrier();
      hdr.switch_counter++;
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
        
        sibling->hdr.lowest = records[m].key;
        sibling->hdr.highest = hdr.highest;
        hdr.highest = records[m].key;

        // printf("[%ld %ld)\n", hdr.lowest, hdr.highest);
        //  printf("[%ld %ld)\n", sibling->hdr.lowest, hdr.highest);
      } else { // internal node
        for (int i = m + 1; i < num_entries; ++i) {
          sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt,
                              false);
        }
        sibling->hdr.leftmost_ptr = (page *)records[m].ptr;
        sibling->hdr.lowest = records[m].key;
        sibling->hdr.highest = hdr.highest;
        hdr.highest = records[m].key;
      }

      sibling->hdr.sibling_ptr = hdr.sibling_ptr;

      hdr.sibling_ptr = sibling;

      // set to NULL
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
        // printf("%p -- %p\n", this, sibling);
        bt->setNewRoot((char *)new_root);

        hdr.mtx->unlock(); // Unlock the write lock

      } else {
        hdr.mtx->unlock(); // Unlock the write lock
        bt->btree_insert_internal(NULL, split_key, (char *)sibling,
                                  hdr.level + 1);
      }

      compiler_barrier();
      hdr.switch_counter++;
      return ret;
    }
  }

  // Search keys with linear search
  void linear_search_range(entry_key_t min, entry_key_t max,
                           unsigned long *buf) {}

  char *linear_search(entry_key_t key) {
    uint8_t previous_switch_counter;
    char *ret = NULL;
    char *t;

    if (hdr.leftmost_ptr == NULL) { // Search a leaf node

    retry_leaf_version:
      previous_switch_counter = hdr.switch_counter;
      if (previous_switch_counter % 2 != 0) {
        goto retry_leaf_version;
      }

      compiler_barrier();

      auto cnt = count();

      if (key >= hdr.highest) {
        ret = (char *)hdr.sibling_ptr;
      } else {
        for (int i = 0; i < cnt; ++i) {
          if (records[i].key == key) {
            ret = records[i].ptr;
            break;
          }
        }
      }

      compiler_barrier();
      if (hdr.switch_counter != previous_switch_counter) {
        goto retry_leaf_version;
      }

      return ret;
    } else { // internal node

    retry_internal_version:
      previous_switch_counter = hdr.switch_counter;
      if (previous_switch_counter % 2 != 0) {
        goto retry_internal_version;
      }

      ret = NULL;

      auto cnt = count();

      if (key >= hdr.highest) {
        // printf("1  %ld\n", hdr.highest);
        ret = (char *)hdr.sibling_ptr;
      } else {
        if (key < records[0].key) {
          ret = (char *)hdr.leftmost_ptr;
          // printf("2\n");
        } else {
          for (int i = 1; i < cnt; ++i) {
            if (key < records[i].key) {
              ret = records[i - 1].ptr;
              // printf("3\n");
              break;
            }
          }
        }
        if (!ret) {
          // printf("4 %x\n", records[cnt - 1].ptr);
          ret = records[cnt - 1].ptr;
        }
      }

      compiler_barrier();
      if (hdr.switch_counter != previous_switch_counter) {
        goto retry_internal_version;
      }
      return ret;
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

};

#endif