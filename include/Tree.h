#if !defined(_TREE_H_)
#define _TREE_H_

#include "DSM.h"


struct SearchResult {
    bool is_leaf;
    uint8_t level;
    GlobalAddress slibing;
    GlobalAddress next_level;
    Value val;
};

class InternalPage;
class LeafPage;
class Tree {

    public:

    Tree(DSM *dsm, uint16_t tree_id = 0);

    void insert(const Key &k, const Value &v);
    bool search(const Key &k, Value &v);
    void del(const Key &k);
    
    private:

    DSM *dsm;
    uint64_t tree_id;
    GlobalAddress root_ptr_ptr; // the address which stores root pointer;

    GlobalAddress get_root_ptr_ptr();
    GlobalAddress get_root_ptr();
    

    void page_search(GlobalAddress page_addr, const Key &k, 
    SearchResult &result);
    void internal_page_search(InternalPage *page, const Key &k, SearchResult &result);
    void leaf_page_search(LeafPage *page, const Key &k, SearchResult &result);

    
};

class Header {
private:
  GlobalAddress leftmost_ptr;    
  GlobalAddress sibling_ptr;    
  uint8_t level;       
  int8_t last_index;   
  Key lowest;
  Key highest; 

  friend class InternalPage;
  friend class LeafPage;
  friend class Tree;

public:
  Header() {
    leftmost_ptr = GlobalAddress::Null();
    sibling_ptr = GlobalAddress::Null();
    last_index = -1;
    lowest = kKeyMin;
    highest = kKeyMax;
  }
} __attribute__ ((packed));;

class InternalEntry {
public:
    Key key; 
    GlobalAddress ptr;

  InternalEntry() {
   ptr = GlobalAddress::Null();
  }
} __attribute__ ((packed));;

class LeafEntry {
public:
  
    uint8_t f_version: 4;
    Key key; 
    Value value;
    uint8_t r_version: 4;

  LeafEntry() {
   f_version = 0;
   r_version = 0;
   value = kValueNull;
  }
} __attribute__ ((packed));

constexpr int kInternalCardinality = (kInternalPageSize - sizeof(Header) - sizeof(uint8_t) * 2) / sizeof(InternalEntry);

constexpr int kLeafCardinality = (kLeafPageSize - sizeof(Header) - sizeof(uint8_t) * 2) / sizeof(LeafEntry);

class InternalPage {
private:
  uint8_t front_version;
  Header hdr;                
  InternalEntry records[kInternalCardinality];

  uint8_t padding[12];
  uint8_t rear_version;
  
  friend class Tree;

public:
  // this is called when tree grows
  InternalPage(GlobalAddress left, const Key &key, GlobalAddress right, uint32_t level = 0) {
    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = right;
    records[1].ptr = GlobalAddress::Null();

    hdr.last_index = 0;
  }

  
}  __attribute__ ((packed));

class LeafPage {
private:
  uint8_t front_version;
  Header hdr;                
  LeafEntry records[kLeafCardinality];

  uint8_t padding[8];
  uint8_t rear_version;

  friend class Tree;

public:

  LeafPage(uint32_t level = 0) {
    hdr.level = level;
    records[0].value = kValueNull;

    front_version = 0;
    rear_version = 0;
  }


}  __attribute__ ((packed));


#endif // _TREE_H_
