#include "Tree.h"

#include <iostream>

#include "RdmaBuffer.h"
#include <city.h>




Tree::Tree(DSM *dsm, uint16_t tree_id): dsm(dsm), 
tree_id(tree_id) {
    
    assert(dsm->is_register());
    print_verbose();

    root_ptr_ptr = get_root_ptr_ptr();

// try to init tree and install root pointer.
    auto page_buffer = (dsm->get_rbuf()).get_page_buffer();
    auto root_addr = dsm->alloc(kLeafPageSize);
    auto root_page = new (page_buffer) LeafPage;

    dsm->write_sync(page_buffer, root_addr, kLeafPageSize);
    
    auto cas_buffer = (dsm->get_rbuf()).get_cas_buffer();
    bool res = dsm->cas_sync(root_ptr_ptr, 0, root_addr.val, cas_buffer);
    if (res) {
        std::cout << "Tree root pointer value" << root_addr << std::endl;
    } else {
        std::cout << "fail\n";
    }

}

void  Tree::print_verbose() {

    constexpr int kLeafHdrOffset = STRUCT_OFFSET(LeafPage, hdr);
    constexpr int kInternalHdrOffset = STRUCT_OFFSET(InternalPage, hdr);
    static_assert(kLeafHdrOffset == kInternalHdrOffset, "XXX");

    if (dsm->getMyNodeID()) {
        std::cout << "Header size: " << sizeof(Header) << std::endl;
        std::cout << "Internal Page size: " << sizeof(InternalPage) << " [" << 
        kInternalPageSize << "]" << std::endl;
        std::cout << "Internal per Page: " << kInternalCardinality << std::endl;
        std::cout << "Leaf Page size: " << sizeof(LeafPage) << " [" << 
        kLeafPageSize << "]" << std::endl;
        std::cout << "Leaf per Page: " << kLeafCardinality << std::endl;
    }
}

GlobalAddress Tree::get_root_ptr_ptr() {
    GlobalAddress addr;
    addr.nodeID = 0;
    addr.offset = define::kRootPointerStoreOffest 
    + sizeof(GlobalAddress) * tree_id;

    return addr;
}

 GlobalAddress Tree::get_root_ptr() {
    auto page_buffer = (dsm->get_rbuf()).get_page_buffer();
    dsm->read_sync(page_buffer, root_ptr_ptr, sizeof(GlobalAddress));
    GlobalAddress root_ptr = *(GlobalAddress *)page_buffer;
    
    std::cout << "root ptr " << root_ptr << std::endl;

    return root_ptr;
 }

void Tree::insert(const Key &k, const Value &v) {
assert(dsm->is_register());

auto root = get_root_ptr();
SearchResult result;

GlobalAddress p = root;


next:
page_search(p, k, result);

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
page_search(p, k, result);
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
} else { // internal 
    p = result.slibing != GlobalAddress::Null() ? 
        result.slibing : result.next_level;
    goto next;
}


}

void Tree::del(const Key &k) {
assert(dsm->is_register());  
}

void Tree::page_search(GlobalAddress page_addr, const Key &k, SearchResult &result) {
    auto page_buffer = (dsm->get_rbuf()).get_page_buffer();

re_read:
    dsm->read_sync(page_buffer, page_addr, kLeafPageSize);

    auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));

    memset(&result, 0, sizeof(result));
    result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
    result.level = header->level;

    if (result.is_leaf) {
        auto page = (LeafPage *)page_buffer;
        if (page->front_version != page->rear_version) {
            goto re_read;
        }
        if (k >= page->hdr.highest) { // should turn right
            result.slibing = page->hdr.sibling_ptr;
            return;
        }
        if (k < page->hdr.lowest) {
            assert(false);
        }
        leaf_page_search(page, k, result);
    } else {
       auto page = (InternalPage *)page_buffer;
        if (page->front_version != page->rear_version) {
            goto re_read;
        }
        if (k >= page->hdr.highest) { // should turn right
            result.slibing = page->hdr.sibling_ptr;
            return;
        }
        if (k < page->hdr.lowest) {
            assert(false);
        }
        internal_page_search(page, k, result);
    }

}

void Tree::internal_page_search(InternalPage *page, const Key &k, SearchResult &result) {
       printf("Internal page\n");
       auto cnt = page->hdr.last_index + 1;
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

        assert(false);
}

void Tree::leaf_page_search(LeafPage *page, const Key &k, SearchResult &result) {
    printf("Hello\n");

    auto cnt = page->hdr.last_index + 1;
    printf("cnt %d\n", cnt);
    for (int i = 0; i < cnt; ++i) {
        if (page->records[i].key == k) {
            result.val = page->records[i].value;
        }
    }
}

void Tree::internal_page_store(GlobalAddress page_addr, const Key &k, const Value &v, GlobalAddress root, int level) {
  

}

void Tree::leaf_page_store(GlobalAddress page_addr, const Key &k, const Value &v, GlobalAddress root, int level) {
   uint64_t lock_index = CityHash64((char *)&page_addr, sizeof(page_addr)) % 
    define::kNumOfLock;

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
    assert(page->front_version == page->rear_version);
     std::cout << "ll " << page->hdr.leftmost_ptr << std::endl;
    if (k >= page->hdr.highest) {
        *cas_buffer = 0;
        dsm->write((char *)cas_buffer, lock_addr, sizeof(uint64_t), false); // unlock

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

    page->front_version++;
    page->rear_version = page->front_version;

    std::cout << "LLL " << page->hdr.leftmost_ptr << " " << page_addr << std::endl;
    dsm->write_sync(page_buffer, page_addr, kLeafPageSize);
    *cas_buffer = 0;
    dsm->write_sync((char *)cas_buffer, lock_addr, sizeof(uint64_t)); // unlock
    
}