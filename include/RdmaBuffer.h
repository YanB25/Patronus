#if !defined(_RDMA_BUFFER_H_)
#define _RDMA_BUFFER_H_

#include "Common.h"


// abstract rdma registered buffer
class RdmaBuffer {

    private:

    char *buffer;
    
    uint64_t *cas_buffer;
    uint64_t *unlock_buffer;
    char *page_buffer;
    char *sibling_buffer;
    char *entry_buffer;

    public:

    RdmaBuffer(char *buffer) {
        set_buffer(buffer);
    }

    RdmaBuffer() = default;

    void set_buffer(char *buffer) {
        this->buffer = buffer;
        cas_buffer = (uint64_t *)buffer;
        unlock_buffer = (uint64_t *)((char *)cas_buffer + sizeof(uint64_t));
        page_buffer = (char *)unlock_buffer + sizeof(uint64_t);
        sibling_buffer = (char *)page_buffer + std::max(kLeafPageSize, kInternalPageSize);
        entry_buffer = (char *)sibling_buffer + std::max(kLeafPageSize, kInternalPageSize);
    }

    uint64_t *get_cas_buffer() const {
        return cas_buffer;
    }

    uint64_t *get_unlock_buffer() const {
        return unlock_buffer;
    }

    char *get_page_buffer() const {
        return page_buffer;
    }

    char *get_sibling_buffer() const {
        return sibling_buffer;
    }

    char *get_entry_buffer() const {
        return entry_buffer;
    }
    
};

#endif // _RDMA_BUFFER_H_
