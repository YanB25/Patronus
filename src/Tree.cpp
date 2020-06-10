#include "Tree.h"

#include <iostream>

#include "RdmaBuffer.h"


Tree::Tree(DSM *dsm, uint16_t tree_id): dsm(dsm), 
tree_id(tree_id) {
    assert(dsm->is_register());

    root_ptr_ptr = get_root_ptr_ptr();

    if (dsm->getMyNodeID()) {
        std::cout << "Header size: " << sizeof(Header) << std::endl;
        std::cout << "Internal Page size: " << sizeof(InternalPage) << " [" << 
        kInternalPageSize << "]" << std::endl;
        std::cout << "Internal per Page: " << kInternalCardinality << std::endl;
        std::cout << "Leaf Page size: " << sizeof(LeafPage) << " [" << 
        kLeafPageSize << "]" << std::endl;
        std::cout << "Leaf per Page: " << kLeafCardinality << std::endl;
    }

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

    auto header = (Header *)page_buffer;

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