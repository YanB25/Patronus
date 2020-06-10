#include "Tree.h"

#include <iostream>

Tree::Tree(DSM *dsm, uint16_t tree_id): dsm(dsm), 
tree_id(tree_id) {
    assert(dsm->is_register());

    root_pointer = get_root_pointer();

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

GlobalAddress Tree::get_root_pointer() {
    GlobalAddress addr;
    addr.nodeID = 0;
    addr.offset = define::kRootPointerStoreOffest 
    + sizeof(uint64_t) * tree_id;

    return addr;
}

void Tree::put(const Key &k, const Value &v) {
assert(dsm->is_register());  
}

void Tree::get(const Key &k, Value &v) {
assert(dsm->is_register());  
}

void Tree::del(const Key &k) {
assert(dsm->is_register());  
}