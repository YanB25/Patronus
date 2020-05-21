/*
   Copyright (c) 2018, UNIST. All rights reserved.  The license is a free
   non-exclusive, non-transferable license to reproduce, use, modify and display
   the source code version of the Software, with or without modifications solely
   for non-commercial research, educational or evaluation purposes. The license
   does not entitle Licensee to technical support, telephone assistance,
   enhancements or updates to the Software. All rights, title to and ownership
   interest in the Software, including all intellectual property rights therein
   shall remain in UNIST. 
*/

#ifndef _B_TREE_H_
#define _B_TREE_H_

#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string.h>
#include <cassert>
#include <climits>
#include <future>
#include <mutex>

#define PAGESIZE 512
#define IS_FORWARD(c) (c % 2 == 0)
using entry_key_t = int64_t;
extern pthread_mutex_t print_mtx;

class page;
class btree{
  private:
    int height;
    char* root;

  public:

    btree();
    void setNewRoot(char *);
    void getNumberOfNodes();
    void btree_insert(entry_key_t, char*);
    void btree_insert_internal(char *, entry_key_t, char *, uint32_t);
    void btree_delete(entry_key_t);
    void btree_delete_internal
      (entry_key_t, char *, uint32_t, entry_key_t *, bool *, page **);
    char *btree_search(entry_key_t);
    void btree_search_range(entry_key_t, entry_key_t, unsigned long *); 
    void printAll();

    friend class page;
};

class header {
  private:
    page* leftmost_ptr;         // 8 bytes
    page* sibling_ptr;          // 8 bytes
    uint32_t level;             // 4 bytes
    uint8_t switch_counter;     // 1 bytes
    uint8_t is_deleted;         // 1 bytes
    int16_t last_index;         // 2 bytes
    std::mutex *mtx;            // 8 bytes
    entry_key_t highest;        // 8 bytes
    uint64_t dummy[1];          // 8 bytes

    friend class page;
    friend class btree;

  public:
    header() {
      mtx = new std::mutex();

      leftmost_ptr = NULL;  
      sibling_ptr = NULL;
      switch_counter = 0;
      last_index = -1;
      is_deleted = false;
    }

    ~header() {
      delete mtx;
    }
};

class entry{ 
  private:
    entry_key_t key; // 8 bytes
    char* ptr; // 8 bytes

  public :
    entry(){
      key = LONG_MAX;
      ptr = NULL;
    }

    friend class page;
    friend class btree;
};

const int cardinality = (PAGESIZE-sizeof(header))/sizeof(entry);

#include "page.h"

#endif