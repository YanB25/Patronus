#include "Rdma.h"

// RC & UC
/**
 * @brief issue a batch of send
 * @param qp
 * @param regions a list of @see Region
 * @param lkey
 * @param signalBatch
 * @param counter the counter of every passed requests
 * @param isInline
 * @param imm
 */
bool rdmaBatchSend(ibv_qp *qp,
                   const std::list<Region> &regions,
                   uint32_t lkey,
                   uint32_t signalBatch,
                   uint64_t &counter,
                   bool isInline,
                   int32_t imm)
{
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_send_wr send_wr[MAX_POST_LIST];
    struct ibv_send_wr *wrBad;
    struct ibv_wc wc;

    auto iter = regions.begin();
    uint32_t batchSize = regions.size();
    for (size_t w_i = 0; w_i < batchSize; ++w_i, ++iter)
    {
        if ((counter & signalBatch) == 0 && counter > 0)
        {
            pollWithCQ(qp->send_cq, 1, &wc);
        }

        sgl[w_i].addr = iter->source;
        sgl[w_i].length = iter->size;
        sgl[w_i].lkey = lkey;
        send_wr[w_i].sg_list = &sgl[w_i];
        send_wr[w_i].num_sge = 1;
        send_wr[w_i].next = (w_i == batchSize - 1) ? NULL : &send_wr[w_i + 1];
        send_wr[w_i].wr_id = 0;

        if (imm == -1)
        {
            send_wr[w_i].opcode = IBV_WR_SEND;
        }
        else
        {
            send_wr[w_i].imm_data = imm;
            send_wr[w_i].opcode = IBV_WR_SEND_WITH_IMM;
        }

        send_wr[w_i].send_flags =
            (counter & signalBatch) == 0 ? IBV_SEND_SIGNALED : 0;

        if (isInline)
        {
            send_wr[w_i].send_flags |= IBV_SEND_INLINE;
        }

        counter += 1;
    }
    if (ibv_post_send(qp, &send_wr[0], &wrBad))
    {
        error("Send with RDMA_SEND failed");
        return false;
    }
    return true;
}

// UC & DC
bool rdmaBatchSend(ibv_qp *qp,
                   const std::list<Region> &regions,
                   uint32_t lkey,
                   uint32_t signalBatch,
                   uint64_t &counter,
                   ibv_ah *ah,
                   uint32_t remoteQPN /* remote dct_number */,
                   bool isInline,
                   int32_t imm)
{
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_exp_send_wr send_wr[MAX_POST_LIST];
    struct ibv_exp_send_wr *wrBad;
    struct ibv_wc wc;

    auto iter = regions.begin();
    uint32_t batchSize = regions.size();
    for (size_t w_i = 0; w_i < batchSize; ++w_i, ++iter)
    {
        if ((counter & signalBatch) == 0 && counter > 0)
        {
            pollWithCQ(qp->send_cq, 1, &wc);
        }

        sgl[w_i].addr = iter->source;
        sgl[w_i].length = iter->size;
        sgl[w_i].lkey = lkey;
        send_wr[w_i].sg_list = &sgl[w_i];
        send_wr[w_i].num_sge = 1;
        send_wr[w_i].next = (w_i == batchSize - 1) ? NULL : &send_wr[w_i + 1];
        send_wr[w_i].wr_id = 0;

        if (qp->qp_type == IBV_QPT_UD)
        {
            send_wr[w_i].wr.ud.ah = ah;
            send_wr[w_i].wr.ud.remote_qpn = remoteQPN;
            send_wr[w_i].wr.ud.remote_qkey = UD_PKEY;
        }
        else
        {
            send_wr[w_i].dc.ah = ah;
            send_wr[w_i].dc.dct_access_key = DCT_ACCESS_KEY;
            send_wr[w_i].dc.dct_number = remoteQPN;
        }

        if (imm == -1)
        {
            send_wr[w_i].exp_opcode = IBV_EXP_WR_SEND;
        }
        else
        {
            send_wr[w_i].ex.imm_data = imm;
            send_wr[w_i].exp_opcode = IBV_EXP_WR_SEND_WITH_IMM;
        }

        send_wr[w_i].exp_send_flags =
            (counter & signalBatch) == 0 ? IBV_SEND_SIGNALED : 0;

        if (isInline)
        {
            send_wr[w_i].exp_send_flags |= IBV_SEND_INLINE;
        }

        counter += 1;
    }
    if (ibv_exp_post_send(qp, &send_wr[0], &wrBad))
    {
        error("Send with RDMA_SEND failed");
        return false;
    }
    return true;
}

bool rdmaBatchReceive(ibv_qp *qp,
                      const std::list<Region> &regions,
                      uint32_t lkey)
{
    struct ibv_recv_wr recv_wr[MAX_POST_LIST], *bad_recv_wr;
    struct ibv_sge sgl[MAX_POST_LIST];

    auto iter = regions.begin();
    uint32_t batchSize = regions.size();
    for (size_t w_i = 0; w_i < batchSize; ++w_i, ++iter)
    {
        sgl[w_i].addr = iter->source;
        sgl[w_i].length = iter->size;
        sgl[w_i].lkey = lkey;

        recv_wr[w_i].sg_list = &sgl[w_i];
        recv_wr[w_i].num_sge = 1;
        recv_wr[w_i].next = (w_i == batchSize - 1) ? NULL : &recv_wr[w_i + 1];
    }
    if (ibv_post_recv(qp, &recv_wr[0], &bad_recv_wr))
    {
        error("Receive failed.");
        return false;
    }
    return true;
}

bool rdmaBatchReceive(ibv_srq *srq,
                      const std::list<Region> &regions,
                      uint32_t lkey)
{
    struct ibv_recv_wr recv_wr[MAX_POST_LIST], *bad_recv_wr;
    struct ibv_sge sgl[MAX_POST_LIST];

    auto iter = regions.begin();
    uint32_t batchSize = regions.size();
    for (size_t w_i = 0; w_i < batchSize; ++w_i, ++iter)
    {
        sgl[w_i].addr = iter->source;
        sgl[w_i].length = iter->size;
        sgl[w_i].lkey = lkey;

        recv_wr[w_i].sg_list = &sgl[w_i];
        recv_wr[w_i].num_sge = 1;
        recv_wr[w_i].next = (w_i == batchSize - 1) ? NULL : &recv_wr[w_i + 1];
    }

    if (ibv_post_srq_recv(srq, &recv_wr[0], &bad_recv_wr))
    {
        error("Receive failed.");
        return false;
    }
    return true;
}

bool rdmaBatchReceive(ibv_exp_dct *dct,
                      const std::list<Region> &regions,
                      uint32_t lkey)
{
    return rdmaBatchReceive(dct->srq, regions, lkey);
}

// RC & UC
bool rdmaBatchRead(ibv_qp *qp,
                   const std::list<Region> &regions,
                   uint32_t lkey,
                   uint32_t signalBatch,
                   uint64_t &counter,
                   uint32_t remoteRKey,
                   bool isInline)
{
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_send_wr send_wr[MAX_POST_LIST];
    struct ibv_send_wr *wrBad;
    struct ibv_wc wc;

    auto iter = regions.begin();
    uint32_t batchSize = regions.size();
    for (size_t w_i = 0; w_i < batchSize; ++w_i, ++iter)
    {
        if ((counter & signalBatch) == 0 && counter > 0)
        {
            pollWithCQ(qp->send_cq, 1, &wc);
        }

        sgl[w_i].addr = iter->source;
        sgl[w_i].length = iter->size;
        sgl[w_i].lkey = lkey;
        send_wr[w_i].sg_list = &sgl[w_i];
        send_wr[w_i].num_sge = 1;
        send_wr[w_i].next = (w_i == batchSize - 1) ? NULL : &send_wr[w_i + 1];
        send_wr[w_i].wr_id = 0;

        send_wr[w_i].opcode = IBV_WR_RDMA_READ;
        send_wr[w_i].send_flags =
            (counter & signalBatch) == 0 ? IBV_SEND_SIGNALED : 0;

        send_wr[w_i].wr.rdma.remote_addr = iter->dest;
        send_wr[w_i].wr.rdma.rkey = remoteRKey;

        if (isInline)
        {
            send_wr[w_i].send_flags |= IBV_SEND_INLINE;
        }

        counter += 1;
    }

    if (ibv_post_send(qp, &send_wr[0], &wrBad))
    {
        error("Send with RDMA_READ failed.");
        return false;
    }
    return true;
}

// DC
bool rdmaBatchRead(ibv_qp *qp,
                   const std::list<Region> &regions,
                   uint32_t lkey,
                   uint32_t signalBatch,
                   uint64_t &counter,
                   uint32_t remoteRKey,
                   ibv_ah *ah,
                   uint32_t remoteDctNumber,
                   bool isInline)
{
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_exp_send_wr send_wr[MAX_POST_LIST];
    struct ibv_exp_send_wr *wrBad;
    struct ibv_wc wc;

    auto iter = regions.begin();
    uint32_t batchSize = regions.size();
    for (size_t w_i = 0; w_i < batchSize; ++w_i, ++iter)
    {
        // TODO:
        // why we need to pollwithcq and why the condition is counter &
        // signalBatch
        if ((counter & signalBatch) == 0 && counter > 0)
        {
            pollWithCQ(qp->send_cq, 1, &wc);
        }

        sgl[w_i].addr = iter->source;
        sgl[w_i].length = iter->size;
        sgl[w_i].lkey = lkey;
        send_wr[w_i].sg_list = &sgl[w_i];
        send_wr[w_i].num_sge = 1;
        send_wr[w_i].next = (w_i == batchSize - 1) ? NULL : &send_wr[w_i + 1];
        send_wr[w_i].wr_id = 0;

        send_wr[w_i].exp_opcode = IBV_EXP_WR_RDMA_READ;
        send_wr[w_i].exp_send_flags =
            (counter & signalBatch) == 0 ? IBV_SEND_SIGNALED : 0;

        send_wr[w_i].wr.rdma.remote_addr = iter->dest;
        send_wr[w_i].wr.rdma.rkey = remoteRKey;

        send_wr[w_i].dc.ah = ah;
        send_wr[w_i].dc.dct_access_key = DCT_ACCESS_KEY;
        send_wr[w_i].dc.dct_number = remoteDctNumber;

        if (isInline)
        {
            send_wr[w_i].exp_send_flags |= IBV_SEND_INLINE;
        }

        counter += 1;
    }

    if (ibv_exp_post_send(qp, &send_wr[0], &wrBad))
    {
        error("Send with RDMA_READ failed.");
        return false;
    }
    return true;
}

// RC & UC
bool rdmaBatchWrite(ibv_qp *qp,
                    const std::list<Region> &regions,
                    uint32_t lkey,
                    uint32_t signalBatch,
                    uint64_t &counter,
                    uint32_t remoteRKey,
                    bool isInline,
                    int32_t imm)
{
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_send_wr send_wr[MAX_POST_LIST];
    struct ibv_send_wr *wrBad;
    struct ibv_wc wc;

    auto iter = regions.begin();
    uint32_t batchSize = regions.size();
    for (size_t w_i = 0; w_i < batchSize; ++w_i, ++iter)
    {
        if ((counter & signalBatch) == 0 && counter > 0)
        {
            pollWithCQ(qp->send_cq, 1, &wc);
        }

        sgl[w_i].addr = iter->source;
        sgl[w_i].length = iter->size;
        sgl[w_i].lkey = lkey;
        send_wr[w_i].sg_list = &sgl[w_i];
        send_wr[w_i].num_sge = 1;
        send_wr[w_i].next = (w_i == batchSize - 1) ? NULL : &send_wr[w_i + 1];
        send_wr[w_i].wr_id = 0;

        if (imm == -1)
        {
            send_wr[w_i].opcode = IBV_WR_RDMA_WRITE;
        }
        else
        {
            send_wr[w_i].opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            send_wr[w_i].imm_data = imm;
        }

        send_wr[w_i].send_flags =
            (counter & signalBatch) == 0 ? IBV_SEND_SIGNALED : 0;
        if (isInline)
        {
            send_wr[w_i].send_flags |= IBV_SEND_INLINE;
        }

        send_wr[w_i].wr.rdma.remote_addr = iter->dest;
        send_wr[w_i].wr.rdma.rkey = remoteRKey;

        counter += 1;
    }

    if (ibv_post_send(qp, &send_wr[0], &wrBad))
    {
        error("Send with RDMA_WRITE(WITH_IMM) failed.");
        return false;
    }
    return true;
}

bool rdmaBatchWrite(ibv_qp *qp,
                    const std::list<Region> &regions,
                    uint32_t lkey,
                    uint32_t signalBatch,
                    uint64_t &counter,
                    uint32_t remoteRKey,
                    ibv_ah *ah,
                    uint32_t remoteDctNumber,
                    bool isInline,
                    int32_t imm)
{
    struct ibv_sge sgl[MAX_POST_LIST];
    struct ibv_exp_send_wr send_wr[MAX_POST_LIST];
    struct ibv_exp_send_wr *wrBad;
    struct ibv_wc wc;

    auto iter = regions.begin();
    uint32_t batchSize = regions.size();
    for (size_t w_i = 0; w_i < batchSize; ++w_i, ++iter)
    {
        if ((counter & signalBatch) == 0 && counter > 0)
        {
            pollWithCQ(qp->send_cq, 1, &wc);
        }

        sgl[w_i].addr = iter->source;
        sgl[w_i].length = iter->size;
        sgl[w_i].lkey = lkey;
        send_wr[w_i].sg_list = &sgl[w_i];
        send_wr[w_i].num_sge = 1;
        send_wr[w_i].next = (w_i == batchSize - 1) ? NULL : &send_wr[w_i + 1];
        send_wr[w_i].wr_id = 0;

        if (imm == -1)
        {
            send_wr[w_i].exp_opcode = IBV_EXP_WR_RDMA_WRITE;
        }
        else
        {
            send_wr[w_i].exp_opcode = IBV_EXP_WR_RDMA_WRITE_WITH_IMM;
            send_wr[w_i].ex.imm_data = imm;
        }

        send_wr[w_i].exp_send_flags =
            (counter & signalBatch) == 0 ? IBV_SEND_SIGNALED : 0;
        if (isInline)
        {
            send_wr[w_i].exp_send_flags |= IBV_SEND_INLINE;
        }

        send_wr[w_i].wr.rdma.remote_addr = iter->dest;
        send_wr[w_i].wr.rdma.rkey = remoteRKey;

        send_wr[w_i].dc.ah = ah;
        send_wr[w_i].dc.dct_access_key = DCT_ACCESS_KEY;
        send_wr[w_i].dc.dct_number = remoteDctNumber;

        counter += 1;
    }

    if (ibv_exp_post_send(qp, &send_wr[0], &wrBad))
    {
        error("Send with RDMA_WRITE(WITH_IMM) failed.");
        return false;
    }
    return true;
}
