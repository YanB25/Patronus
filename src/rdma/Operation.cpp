#include <glog/logging.h>
#include <inttypes.h>

#include <atomic>

#include "Common.h"
#include "Rdma.h"

int pollWithCQ(ibv_cq *cq,
               int pollNumber,
               struct ibv_wc *wc,
               const WcErrHandler &err_handler,
               const WcHandler &handler)
{
    int count = 0;
    DCHECK(pollNumber > 0) << "pollNumber should > 0, get " << pollNumber;

    do
    {
        int new_count = ibv_poll_cq(cq, 1, wc);
        if (new_count == 1)
        {
            if (wc->status == IBV_WC_SUCCESS)
            {
                handler(wc);
            }
            else
            {
                LOG(ERROR) << "[wc] Failed status "
                           << ibv_wc_status_str(wc->status) << " ("
                           << wc->status << ") for wr_id " << WRID(wc->wr_id)
                           << " at QP: " << wc->qp_num
                           << ". vendor err: " << wc->vendor_err;
                err_handler(wc);
                // it is a QP error, in addition to WC error.
                // should report to the outer world
                if (wc->status == IBV_WC_WR_FLUSH_ERR)
                {
                    return -1;
                }
            }
        }
        else if (new_count < 0)
        {
            LOG(ERROR) << "Poll Completion failed.";
            // sleep(5);
            return -1;
        }
        else if (new_count > 1)
        {
            LOG(FATAL) << "impossible return value from ibv_poll_cq";
        }
        count += new_count;

    } while (count < pollNumber);

    return count;
}

int pollOnce(ibv_cq *cq, int pollNumber, struct ibv_wc *wc)
{
    int count = ibv_poll_cq(cq, pollNumber, wc);
    if (count <= 0)
    {
        return 0;
    }
    if (wc->status != IBV_WC_SUCCESS)
    {
        LOG(ERROR) << "[qp] failed status " << ibv_wc_status_str(wc->status)
                   << " (" << wc->status << ") for wr_id " << wc->wr_id;
        return -1;
    }
    else
    {
        return count;
    }
}
/**
 * @brief fill SGE and WR and links the SGE to the WR
 */
static inline void fillSgeWr(
    ibv_sge &sg, ibv_send_wr &wr, uint64_t source, uint64_t size, uint32_t lkey)
{
    memset(&wr, 0, sizeof(wr));

    // optimized for zero-size op, e.g., zero-size SEND
    if (size == 0)
    {
        wr.num_sge = 0;
    }
    else
    {
        memset(&sg, 0, sizeof(sg));
        sg.addr = (uintptr_t) source;
        sg.length = size;
        sg.lkey = lkey;

        wr.wr_id = 0;
        wr.sg_list = &sg;
        wr.num_sge = 1;
    }
}

static inline void fillSgeWr(
    ibv_sge &sg, ibv_recv_wr &wr, uint64_t source, uint64_t size, uint32_t lkey)
{
    memset(&wr, 0, sizeof(wr));

    if (size == 0)
    {
        wr.num_sge = 0;
    }
    else
    {
        memset(&sg, 0, sizeof(sg));
        sg.addr = (uintptr_t) source;
        sg.length = size;
        sg.lkey = lkey;

        wr.wr_id = 0;
        wr.sg_list = &sg;
        wr.num_sge = 1;
    }
}

static inline void fillSgeWr(ibv_sge &sg,
                             ibv_exp_send_wr &wr,
                             uint64_t source,
                             uint64_t size,
                             uint32_t lkey)
{
    memset(&wr, 0, sizeof(wr));
    if (size == 0)
    {
        wr.num_sge = 0;
    }
    else
    {
        memset(&sg, 0, sizeof(sg));
        sg.addr = (uintptr_t) source;
        sg.length = size;
        sg.lkey = lkey;

        wr.wr_id = 0;
        wr.sg_list = &sg;
        wr.num_sge = 1;
    }
}

// for UD and DC
bool rdmaSend(ibv_qp *qp,
              uint64_t source,
              uint64_t size,
              uint32_t lkey,
              ibv_ah *ah,
              uint32_t remoteQPN /* remote dct_number */,
              bool isSignaled,
              bool isInlined,
              uint64_t wr_id)
{
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;

    fillSgeWr(sg, wr, source, size, lkey);

    wr.opcode = IBV_WR_SEND;

    wr.wr.ud.ah = ah;
    wr.wr.ud.remote_qpn = remoteQPN;
    wr.wr.ud.remote_qkey = UD_PKEY;
    wr.wr_id = wr_id;
    wr.send_flags = 0;

    if (isSignaled)
    {
        wr.send_flags |= IBV_SEND_SIGNALED;
    }
    if (isInlined)
    {
        wr.send_flags |= IBV_SEND_INLINE;
    }
    if (ibv_post_send(qp, &wr, &wrBad))
    {
        LOG(ERROR) << "Send with RDMA_SEND failed.";
        return false;
    }
    return true;
}

// for RC & UC
bool rdmaSend(ibv_qp *qp,
              uint64_t source,
              uint64_t size,
              uint32_t lkey,
              bool signal,
              bool inlined,
              uint64_t wr_id,
              int32_t imm)
{
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;

    fillSgeWr(sg, wr, source, size, lkey);
    wr.send_flags = 0;

    if (imm != -1)
    {
        wr.imm_data = imm;
        wr.opcode = IBV_WR_SEND_WITH_IMM;
    }
    else
    {
        wr.opcode = IBV_WR_SEND;
    }
    if (signal)
    {
        wr.send_flags |= IBV_SEND_SIGNALED;
    }
    if (inlined)
    {
        wr.send_flags |= IBV_SEND_INLINE;
    }

    wr.wr_id = wr_id;
    if (ibv_post_send(qp, &wr, &wrBad))
    {
        PLOG(ERROR) << "Send with RDMA_SEND failed. wrid: " << WRID(wr.wr_id)
                    << ", QP: " << (void *) qp;
        return false;
    }
    return true;
}

bool rdmaReceive(
    ibv_qp *qp, uint64_t source, uint64_t size, uint32_t lkey, uint64_t wr_id)
{
    struct ibv_sge sg;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *wrBad;

    fillSgeWr(sg, wr, source, size, lkey);

    wr.wr_id = wr_id;

    if (ibv_post_recv(qp, &wr, &wrBad))
    {
        LOG(ERROR) << "Receive with RDMA_RECV failed.";
        return false;
    }
    return true;
}

bool rdmaReceive(ibv_srq *srq, uint64_t source, uint64_t size, uint32_t lkey)
{
    struct ibv_sge sg;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *wrBad;

    fillSgeWr(sg, wr, source, size, lkey);

    if (ibv_post_srq_recv(srq, &wr, &wrBad))
    {
        LOG(ERROR) << "Receive with RDMA_RECV failed.";
        return false;
    }
    return true;
}

bool rdmaReceive(ibv_exp_dct *dct,
                 uint64_t source,
                 uint64_t size,
                 uint32_t lkey)
{
    return rdmaReceive(dct->srq, source, size, lkey);
}

// for RC & UC
bool rdmaRead(ibv_qp *qp,
              uint64_t source,
              uint64_t dest,
              uint64_t size,
              uint32_t lkey,
              uint32_t remoteRKey,
              bool signal,
              uint64_t wrID)
{
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;

    fillSgeWr(sg, wr, source, size, lkey);

    wr.opcode = IBV_WR_RDMA_READ;

    if (signal)
    {
        wr.send_flags = IBV_SEND_SIGNALED;
    }

    wr.wr.rdma.remote_addr = dest;
    wr.wr.rdma.rkey = remoteRKey;
    wr.wr_id = wrID;

    if (ibv_post_send(qp, &wr, &wrBad))
    {
        LOG(ERROR) << "Send with RDMA_READ failed.";
        return false;
    }
    return true;
}

// for DC
bool rdmaRead(ibv_qp *qp,
              uint64_t source,
              uint64_t dest,
              uint64_t size,
              uint32_t lkey,
              uint32_t remoteRKey,
              ibv_ah *ah,
              uint32_t remoteDctNumber)
{
    struct ibv_sge sg;
    struct ibv_exp_send_wr wr;
    struct ibv_exp_send_wr *wrBad;

    fillSgeWr(sg, wr, source, size, lkey);

    wr.exp_opcode = IBV_EXP_WR_RDMA_READ;
    wr.exp_send_flags = IBV_SEND_SIGNALED;

    wr.wr.rdma.remote_addr = dest;
    wr.wr.rdma.rkey = remoteRKey;

    wr.dc.ah = ah;
    wr.dc.dct_access_key = DCT_ACCESS_KEY;
    wr.dc.dct_number = remoteDctNumber;

    if (ibv_exp_post_send(qp, &wr, &wrBad))
    {
        LOG(ERROR) << "Send with RDMA_READ failed.";
        return false;
    }
    return true;
}

// for RC & UC
bool rdmaWrite(ibv_qp *qp,
               uint64_t source,
               uint64_t dest,
               uint64_t size,
               uint32_t lkey,
               uint32_t remoteRKey,
               int32_t imm,
               bool isSignaled,
               uint64_t wrID)
{
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;

    fillSgeWr(sg, wr, source, size, lkey);

    if (imm == -1)
    {
        wr.opcode = IBV_WR_RDMA_WRITE;
    }
    else
    {
        wr.imm_data = imm;
        wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    }

    if (isSignaled)
    {
        wr.send_flags = IBV_SEND_SIGNALED;
    }

    wr.wr.rdma.remote_addr = dest;
    wr.wr.rdma.rkey = remoteRKey;
    wr.wr_id = wrID;

    if (ibv_post_send(qp, &wr, &wrBad) != 0)
    {
        LOG(ERROR) << "Send with RDMA_WRITE(WITH_IMM) failed.";
        // sleep(10);
        return false;
    }
    return true;
}

// for DC
bool rdmaWrite(ibv_qp *qp,
               uint64_t source,
               uint64_t dest,
               uint64_t size,
               uint32_t lkey,
               uint32_t remoteRKey,
               ibv_ah *ah,
               uint32_t remoteDctNumber,
               int32_t imm)
{
    struct ibv_sge sg;
    struct ibv_exp_send_wr wr;
    struct ibv_exp_send_wr *wrBad;

    fillSgeWr(sg, wr, source, size, lkey);

    if (imm == -1)
    {
        wr.exp_opcode = IBV_EXP_WR_RDMA_WRITE;
    }
    else
    {
        wr.ex.imm_data = imm;
        wr.exp_opcode = IBV_EXP_WR_RDMA_WRITE_WITH_IMM;
    }
    wr.exp_send_flags = IBV_SEND_SIGNALED;

    wr.wr.rdma.remote_addr = dest;
    wr.wr.rdma.rkey = remoteRKey;

    wr.dc.ah = ah;
    wr.dc.dct_access_key = DCT_ACCESS_KEY;
    wr.dc.dct_number = remoteDctNumber;

    if (ibv_exp_post_send(qp, &wr, &wrBad) != 0)
    {
        LOG(ERROR) << "Send with RDMA_WRITE(WITH_IMM) failed.";
        // sleep(5);
        return false;
    }
    return true;
}

// RC & UC
bool rdmaFetchAndAdd(ibv_qp *qp,
                     uint64_t source,
                     uint64_t dest,
                     uint64_t add,
                     uint32_t lkey,
                     uint32_t remoteRKey)
{
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;

    fillSgeWr(sg, wr, source, 8, lkey);

    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;

    wr.wr.atomic.remote_addr = dest;
    wr.wr.atomic.rkey = remoteRKey;
    wr.wr.atomic.compare_add = add;

    if (ibv_post_send(qp, &wr, &wrBad))
    {
        LOG(ERROR) << "Send with ATOMIC_FETCH_AND_ADD failed.";
        return false;
    }
    return true;
}

bool rdmaFetchAndAddBoundary(ibv_qp *qp,
                             uint64_t source,
                             uint64_t dest,
                             uint64_t add,
                             uint32_t lkey,
                             uint32_t remoteRKey,
                             uint64_t boundary,
                             bool singal,
                             uint64_t wr_id)
{
    struct ibv_sge sg;
    struct ibv_exp_send_wr wr;
    struct ibv_exp_send_wr *wrBad;

    fillSgeWr(sg, wr, source, 8, lkey);

    wr.exp_opcode = IBV_EXP_WR_EXT_MASKED_ATOMIC_FETCH_AND_ADD;
    wr.exp_send_flags = IBV_EXP_SEND_EXT_ATOMIC_INLINE;
    wr.wr_id = wr_id;

    if (singal)
    {
        wr.exp_send_flags |= IBV_EXP_SEND_SIGNALED;
    }

    wr.ext_op.masked_atomics.log_arg_sz = 3;
    wr.ext_op.masked_atomics.remote_addr = dest;
    wr.ext_op.masked_atomics.rkey = remoteRKey;

    auto &op = wr.ext_op.masked_atomics.wr_data.inline_data.op.fetch_add;
    op.add_val = add;
    op.field_boundary = 1ull << boundary;

    if (ibv_exp_post_send(qp, &wr, &wrBad))
    {
        LOG(ERROR) << "Send with MASK FETCH_AND_ADD failed.";
        return false;
    }
    return true;
}

// DC
bool rdmaFetchAndAdd(ibv_qp *qp,
                     uint64_t source,
                     uint64_t dest,
                     uint64_t add,
                     uint32_t lkey,
                     uint32_t remoteRKey,
                     ibv_ah *ah,
                     uint32_t remoteDctNumber)
{
    struct ibv_sge sg;
    struct ibv_exp_send_wr wr;
    struct ibv_exp_send_wr *wrBad;

    fillSgeWr(sg, wr, source, 8, lkey);

    wr.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
    wr.exp_send_flags = IBV_EXP_SEND_SIGNALED;

    wr.wr.atomic.remote_addr = dest;
    wr.wr.atomic.rkey = remoteRKey;
    wr.wr.atomic.compare_add = add;

    wr.dc.ah = ah;
    wr.dc.dct_access_key = DCT_ACCESS_KEY;
    wr.dc.dct_number = remoteDctNumber;

    if (ibv_exp_post_send(qp, &wr, &wrBad))
    {
        LOG(ERROR) << "Send with ATOMIC_FETCH_AND_ADD failed.";
        return false;
    }
    return true;
}

static uint32_t magic = 0b1010101010;
static uint16_t mask = 0b1111111111;

uint32_t rdmaAsyncBindMemoryWindow(ibv_qp *qp,
                                   ibv_mw *mw,
                                   struct ibv_mr *mr,
                                   uint64_t mm,
                                   uint64_t mmSize,
                                   bool signal,
                                   uint64_t wrID,
                                   unsigned int mw_access_flag)
{
    static std::atomic<size_t> id_{0};

    DCHECK(qp->qp_type == IBV_QPT_RC || qp->qp_type == IBV_QPT_UC ||
           qp->qp_type == IBV_QPT_XRC_SEND);
    struct ibv_mw_bind mw_bind;
    memset(&mw_bind, 0, sizeof(mw_bind));

    if (signal)
    {
        mw_bind.send_flags |= IBV_SEND_SIGNALED;
    }
    mw_bind.wr_id = wrID;
    mw_bind.bind_info.mr = mr;
    mw_bind.bind_info.addr = mm;
    mw_bind.bind_info.length = mmSize;
    mw_bind.bind_info.mw_access_flags = mw_access_flag;

    // dinfo(
    //     "[MW] Binding memory window. qp: %p, mm: %p, size: %lu, mr: %p, "
    //     "mr.lkey: %u, mr.rkey: %u, mr.pd: %p, mr.addr: %p, mr.length: %lu",
    //     qp,
    //     (char *) mm,
    //     mmSize,
    //     mr,
    //     mr->lkey,
    //     mr->rkey,
    //     mr->pd,
    //     mr->addr,
    //     mr->length);
    int ret = ibv_bind_mw(qp, mw, &mw_bind);
    if (ret == EINVAL)
    {
        PLOG(ERROR)
            << "failed to bind mw: maybe library not support TYPE_2 memory "
               "window: errno "
            << ret;
        return 0;
    }
    if (ret == ENOMEM)
    {
        PLOG(FATAL) << "Failed to bind mw: Memory has been used up. errno: "
                    << ret;
    }
    if (ret == ENOTSUP)
    {
        PLOG(FATAL)
            << "Failed to bind mw: Operation not supported. Is it too large? "
               "errno: "
            << ret;
    }
    if (ret)
    {
        PLOG(ERROR) << "Failed to bind memory window. errno: " << ret
                    << "< qp: " << qp << ", mw: " << mw << ", mr: " << mr
                    << ", mr.lkey: " << mr->lkey << ", mm: " << (char *) mm
                    << ", size: " << mmSize;
        return 0;
    }
    // dinfo("Succeed in bind_mw. poll? send_cq: %p, recv_cq: %p, srq: %p",
    // qp->send_cq, qp->recv_cq, qp->srq);

    size_t id = id_.fetch_add(1, std::memory_order_relaxed);
    if ((id & mask) == magic)
    {
        LOG_FIRST_N(WARNING, 1)
            << "TODO: Strange bug: reallocate mw: " << mw << ", idx: %" << id;
        return rdmaAsyncBindMemoryWindow(
            qp, mw, mr, mm, mmSize, signal, wrID, mw_access_flag);
    }

    return mw->rkey;
}

// for RC & UC
bool rdmaCompareAndSwap(ibv_qp *qp,
                        uint64_t source,
                        uint64_t dest,
                        uint64_t compare,
                        uint64_t swap,
                        uint32_t lkey,
                        uint32_t remoteRKey,
                        bool signal,
                        uint64_t wrID)
{
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;

    fillSgeWr(sg, wr, source, 8, lkey);

    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;

    if (signal)
    {
        wr.send_flags = IBV_SEND_SIGNALED;
    }

    wr.wr.atomic.remote_addr = dest;
    wr.wr.atomic.rkey = remoteRKey;
    wr.wr.atomic.compare_add = compare;
    wr.wr.atomic.swap = swap;
    wr.wr_id = wrID;

    if (ibv_post_send(qp, &wr, &wrBad))
    {
        LOG(ERROR) << "Send with ATOMIC_CMP_AND_SWP failed.";
        // sleep(5);
        return false;
    }
    return true;
}

bool rdmaCompareAndSwapMask(ibv_qp *qp,
                            uint64_t source,
                            uint64_t dest,
                            uint64_t compare,
                            uint64_t swap,
                            uint32_t lkey,
                            uint32_t remoteRKey,
                            uint64_t mask,
                            bool singal)
{
    struct ibv_sge sg;
    struct ibv_exp_send_wr wr;
    struct ibv_exp_send_wr *wrBad;

    fillSgeWr(sg, wr, source, 8, lkey);

    wr.exp_opcode = IBV_EXP_WR_EXT_MASKED_ATOMIC_CMP_AND_SWP;
    wr.exp_send_flags = IBV_EXP_SEND_EXT_ATOMIC_INLINE;

    if (singal)
    {
        wr.exp_send_flags |= IBV_EXP_SEND_SIGNALED;
    }

    wr.ext_op.masked_atomics.log_arg_sz = 3;
    wr.ext_op.masked_atomics.remote_addr = dest;
    wr.ext_op.masked_atomics.rkey = remoteRKey;

    auto &op = wr.ext_op.masked_atomics.wr_data.inline_data.op.cmp_swap;
    op.compare_val = compare;
    op.swap_val = swap;

    op.compare_mask = mask;
    op.swap_mask = mask;

    if (ibv_exp_post_send(qp, &wr, &wrBad))
    {
        LOG(ERROR) << "Send with MASK ATOMIC_CMP_AND_SWP failed.";
        return false;
    }
    return true;
}

// DC
bool rdmaCompareAndSwap(ibv_qp *qp,
                        uint64_t source,
                        uint64_t dest,
                        uint64_t compare,
                        uint64_t swap,
                        uint32_t lkey,
                        uint32_t remoteRKey,
                        ibv_ah *ah,
                        uint32_t remoteDctNumber)
{
    struct ibv_sge sg;
    struct ibv_exp_send_wr wr;
    struct ibv_exp_send_wr *wrBad;

    fillSgeWr(sg, wr, source, 8, lkey);

    wr.exp_opcode = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
    wr.exp_send_flags = IBV_SEND_SIGNALED;

    wr.wr.atomic.remote_addr = dest;
    wr.wr.atomic.rkey = remoteRKey;
    wr.wr.atomic.compare_add = compare;
    wr.wr.atomic.swap = swap;

    wr.dc.ah = ah;
    wr.dc.dct_access_key = DCT_ACCESS_KEY;
    wr.dc.dct_number = remoteDctNumber;

    if (ibv_exp_post_send(qp, &wr, &wrBad))
    {
        LOG(ERROR) << "Send with ATOMIC_CMP_AND_SWP failed.";
        return false;
    }
    return true;
}

bool rdmaWriteBatch(
    ibv_qp *qp, RdmaOpRegion *ror, int k, bool isSignaled, uint64_t wrID)
{
    DCHECK(k < kOroMax) << "overflow detected at k = " << k;

    struct ibv_sge sg[kOroMax];
    struct ibv_send_wr wr[kOroMax];
    struct ibv_send_wr *wrBad;

    for (int i = 0; i < k; ++i)
    {
        fillSgeWr(sg[i], wr[i], ror[i].source, ror[i].size, ror[i].lkey);

        wr[i].next = (i == k - 1) ? NULL : &wr[i + 1];

        wr[i].opcode = IBV_WR_RDMA_WRITE;

        if (i == k - 1 && isSignaled)
        {
            wr[i].send_flags = IBV_SEND_SIGNALED;
        }

        wr[i].wr.rdma.remote_addr = ror[i].dest;
        wr[i].wr.rdma.rkey = ror[i].remoteRKey;
        wr[i].wr_id = wrID;
    }

    if (ibv_post_send(qp, &wr[0], &wrBad) != 0)
    {
        LOG(ERROR) << "Send with RDMA_WRITE(WITH_IMM) failed.";
        // sleep(10);
        return false;
    }
    return true;
}

bool rdmaCasRead(ibv_qp *qp,
                 const RdmaOpRegion &cas_ror,
                 const RdmaOpRegion &read_ror,
                 uint64_t compare,
                 uint64_t swap,
                 bool isSignaled,
                 uint64_t wrID)
{
    struct ibv_sge sg[2];
    struct ibv_send_wr wr[2];
    struct ibv_send_wr *wrBad;

    fillSgeWr(sg[0], wr[0], cas_ror.source, 8, cas_ror.lkey);
    wr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr[0].wr.atomic.remote_addr = cas_ror.dest;
    wr[0].wr.atomic.rkey = cas_ror.remoteRKey;
    wr[0].wr.atomic.compare_add = compare;
    wr[0].wr.atomic.swap = swap;
    wr[0].next = &wr[1];

    fillSgeWr(sg[1], wr[1], read_ror.source, read_ror.size, read_ror.lkey);
    wr[1].opcode = IBV_WR_RDMA_READ;
    wr[1].wr.rdma.remote_addr = read_ror.dest;
    wr[1].wr.rdma.rkey = read_ror.remoteRKey;
    wr[1].wr_id = wrID;
    wr[1].send_flags |= IBV_SEND_FENCE;
    if (isSignaled)
    {
        wr[1].send_flags |= IBV_SEND_SIGNALED;
    }

    if (ibv_post_send(qp, &wr[0], &wrBad))
    {
        LOG(ERROR) << "Send with CAS_READs failed.";
        // sleep(10);
        return false;
    }
    return true;
}

/**
 * @brief rdma Write and Fetch and Add
 */
bool rdmaWriteFaa(ibv_qp *qp,
                  const RdmaOpRegion &write_ror,
                  const RdmaOpRegion &faa_ror,
                  uint64_t add_val,
                  bool isSignaled,
                  uint64_t wrID)
{
    struct ibv_sge sg[2];
    struct ibv_send_wr wr[2];
    struct ibv_send_wr *wrBad;

    fillSgeWr(sg[0], wr[0], write_ror.source, write_ror.size, write_ror.lkey);
    wr[0].opcode = IBV_WR_RDMA_WRITE;
    wr[0].wr.rdma.remote_addr = write_ror.dest;
    wr[0].wr.rdma.rkey = write_ror.remoteRKey;
    wr[0].next = &wr[1];

    fillSgeWr(sg[1], wr[1], faa_ror.source, 8, faa_ror.lkey);
    wr[1].opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr[1].wr.atomic.remote_addr = faa_ror.dest;
    wr[1].wr.atomic.rkey = faa_ror.remoteRKey;
    wr[1].wr.atomic.compare_add = add_val;
    wr[1].wr_id = wrID;

    if (isSignaled)
    {
        wr[1].send_flags |= IBV_SEND_SIGNALED;
    }

    if (ibv_post_send(qp, &wr[0], &wrBad))
    {
        LOG(ERROR) << "Send with Write Faa failed.";
        // sleep(10);
        return false;
    }
    return true;
}

bool rdmaWriteCas(ibv_qp *qp,
                  const RdmaOpRegion &write_ror,
                  const RdmaOpRegion &cas_ror,
                  uint64_t compare,
                  uint64_t swap,
                  bool isSignaled,
                  uint64_t wrID)
{
    struct ibv_sge sg[2];
    struct ibv_send_wr wr[2];
    struct ibv_send_wr *wrBad;

    fillSgeWr(sg[0], wr[0], write_ror.source, write_ror.size, write_ror.lkey);
    wr[0].opcode = IBV_WR_RDMA_WRITE;
    wr[0].wr.rdma.remote_addr = write_ror.dest;
    wr[0].wr.rdma.rkey = write_ror.remoteRKey;
    wr[0].next = &wr[1];

    fillSgeWr(sg[1], wr[1], cas_ror.source, 8, cas_ror.lkey);
    wr[1].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr[1].wr.atomic.remote_addr = cas_ror.dest;
    wr[1].wr.atomic.rkey = cas_ror.remoteRKey;
    wr[1].wr.atomic.compare_add = compare;
    wr[1].wr.atomic.swap = swap;
    wr[1].wr_id = wrID;

    if (isSignaled)
    {
        wr[1].send_flags |= IBV_SEND_SIGNALED;
    }

    if (ibv_post_send(qp, &wr[0], &wrBad))
    {
        LOG(ERROR) << "Send with Write Cas failed.";
        // sleep(10);
        return false;
    }
    return true;
}

void rdmaQueryDevice()
{
    ibv_device *dev = NULL;
    ibv_context *ctx = NULL;
    uint8_t devIndex = 0;

    // get device names in the system
    int devicesNum;
    struct ibv_device **deviceList = ibv_get_device_list(&devicesNum);
    if (!deviceList)
    {
        LOG(ERROR) << "failed to get IB devices list";
        return;
    }

    // if there isn't any IB device in host
    if (!devicesNum)
    {
        LOG(INFO) << "found " << devicesNum << " device(s)";
        return;
    }
    // dinfo("Open IB Device");

    for (int i = 0; i < devicesNum; ++i)
    {
        // printf("Device %d: %s\n", i, ibv_get_device_name(deviceList[i]));
        if (ibv_get_device_name(deviceList[i])[3] == '5')
        {
            devIndex = i;
            break;
        }
    }

    if (devIndex >= devicesNum)
    {
        LOG(ERROR) << "ib device wasn't found";
        return;
    }

    dev = deviceList[devIndex];
    ctx = ibv_open_device(dev);
    if (!ctx)
    {
        LOG(ERROR) << "failed to open device";
        return;
    }

    struct ibv_device_attr attr;
    memset(&attr, 0, sizeof(struct ibv_device_attr));
    if (ibv_query_device(ctx, &attr))
    {
        PLOG(ERROR) << "failed to query device attr";
    }
    printf("======= device attr ========\n");
    printf("max_mr_size: %" PRIu64 "\n", attr.max_mr_size);
    printf("max_qp: %d\n", attr.max_qp);
    printf("max_qp_wr: %d\n", attr.max_qp_wr);
    printf("max_sge: %d\n", attr.max_sge);
    printf("max_cq: %d\n", attr.max_cq);
    printf("max_cqe: %d\n", attr.max_cqe);
    printf("max_mr: %d\n", attr.max_mr);
    printf("max_mr_pd: %d\n", attr.max_pd);
    printf("max_mw: %d\n", attr.max_mw);
    printf("max_srq: %d\n", attr.max_srq);
    printf("max_srq_wr: %d\n", attr.max_srq_wr);
    printf("max_srq_sge: %d\n", attr.max_srq_sge);
    printf("max_pkeys: %d\n", attr.max_pkeys);
    printf("max_srq_wr: %d\n", attr.max_srq_wr);
    printf("max_qp_rd_atom: %d\n", attr.max_qp_rd_atom);
    printf("max_res_rd_atom: %d\n", attr.max_res_rd_atom);
    printf("atomic_cap: %d\n", attr.atomic_cap);
    printf("max_fmr: %d\n", attr.max_fmr);
    printf("IBV_DEVICE_MEM_WINDOW: %d\n",
           attr.device_cap_flags & IBV_DEVICE_MEM_WINDOW ? 1 : 0);
    printf("IBV_DEVICE_MEM_WINDOW_TYPE_2A: %d\n",
           attr.device_cap_flags & IBV_DEVICE_MEM_WINDOW_TYPE_2A ? 1 : 0);
    printf("IBV_DEVICE_MEM_WINDOW_TYPE_2B: %d\n",
           attr.device_cap_flags & IBV_DEVICE_MEM_WINDOW_TYPE_2B ? 1 : 0);
    printf("IBV_DEVICE_MEM_MGT_EXTENTIONS: %d\n",
           attr.device_cap_flags & IBV_DEVICE_MEM_MGT_EXTENSIONS ? 1 : 0);

    struct ibv_exp_device_attr exp_attr;
    memset(&exp_attr, 0, sizeof(exp_attr));
    exp_attr.comp_mask = IBV_EXP_DEVICE_ATTR_ODP |
                         IBV_EXP_DEVICE_ATTR_EXP_CAP_FLAGS |
                         IBV_EXP_DEVICE_ATTR_INLINE_RECV_SZ |
                         IBV_EXP_DEVICE_ATTR_TUNNELED_ATOMIC |
                         IBV_EXP_DEVICE_ATTR_TUNNEL_OFFLOADS_CAPS;
    PLOG_IF(ERROR, ibv_exp_query_device(ctx, &exp_attr))
        << "failed to query device attr";

    printf("IBV_EXP_DEVICE_ODP: %d\n",
           exp_attr.exp_device_cap_flags & IBV_EXP_DEVICE_ODP ? 1 : 0);
    printf("inline_recv_sz: %d\n", exp_attr.inline_recv_sz);
    printf("IBV_EXP_TUNNELED_ATOMIC_SUPPORTED: %d\n",
           exp_attr.tunneled_atomic_caps & IBV_EXP_TUNNELED_ATOMIC_SUPPORTED);
    printf("======= device attr end ====\n");

    PLOG_IF(ERROR, ibv_close_device(ctx)) << "failed to close device";

    ibv_free_device_list(deviceList);
}