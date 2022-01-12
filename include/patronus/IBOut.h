#pragma once
#ifndef PATRONUS_IBOUT_H_
#define PATRONUS_IBOUT_H_

#include <infiniband/verbs.h>

#include <iostream>

#include "Common.h"

namespace patronus
{
const char *wc_opcode_str(ibv_wc_opcode opcode)
{
    switch (opcode)
    {
    case IBV_WC_SEND:
        return "IBV_WC_SEND";
    case IBV_WC_RDMA_WRITE:
        return "IBV_WC_RDMA_WRITE";
    case IBV_WC_RDMA_READ:
        return "IBV_WC_RDMA_READ";
    case IBV_WC_COMP_SWAP:
        return "IBV_WC_COMP_SWAP";
    case IBV_WC_FETCH_ADD:
        return "IBV_WC_FETCH_ADD";
    case IBV_WC_BIND_MW:
        return "IBV_WC_BIND_MW";
    case IBV_WC_LOCAL_INV:
        return "IBV_WC_LOCAL_INV";
    case IBV_WC_RECV:
        return "IBV_WC_RECV";
    case IBV_WC_RECV_RDMA_WITH_IMM:
        return "IBV_WC_RECV_RDMA_WITH_IMM";
    default:
    {
        return "Unknown ibv op code";
    }
    }
}

inline std::ostream &operator<<(std::ostream &os, const ibv_wc_status &status)
{
    os << ibv_wc_status_str(status);
    return os;
}
inline std::ostream &operator<<(std::ostream &os, const ibv_wc_opcode &opcode)
{
    os << wc_opcode_str(opcode);
    return os;
}
inline std::ostream &operator<<(std::ostream &os, const ibv_send_wr &wr)
{
    os << "{ibv_send_wr wr_id: " << WRID(wr.wr_id)
       << ", opcode: " << (int) wr.opcode << ", send_flags: " << wr.send_flags
       << ", imm_data: " << wr.imm_data << "}";
    return os;
}
inline std::ostream &operator<<(std::ostream &os, const ibv_wc &wc)
{
    os << "{ibv_wc wr_id: " << WRID(wc.wr_id) << ", status: " << wc.status
       << ", op: " << wc.opcode << ", vendor_err: " << wc.vendor_err
       << ", imm_data: " << wc.imm_data << ", qp_num: " << wc.qp_num << "}";
    return os;
}

}  // namespace patronus

#endif