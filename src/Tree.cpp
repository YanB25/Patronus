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

void Tree::search(const Key &k, Value &v) {
assert(dsm->is_register());



}

void Tree::del(const Key &k) {
assert(dsm->is_register());  
}