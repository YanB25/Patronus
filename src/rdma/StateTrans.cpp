#include <glog/logging.h>

#include "Rdma.h"
bool modifyErrQPtoNormal(struct ibv_qp *qp,
                         uint32_t remoteQPN,
                         uint16_t remoteLid,
                         const uint8_t *remoteGid,
                         RdmaContext *context)
{
    if (!modifyQPtoReset(qp))
    {
        return false;
    }
    if (!modifyQPtoInit(qp, context))
    {
        return false;
    }
    if (!modifyQPtoRTR(qp, remoteQPN, remoteLid, remoteGid, context))
    {
        return false;
    }
    if (!modifyQPtoRTS(qp))
    {
        return false;
    }
    DLOG(INFO) << "Succeed in switch QP: " << qp << " to normal state.";
    return true;
}
bool modifyQPtoReset(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;

    switch (qp->qp_type)
    {
    case IBV_QPT_RC:
        attr.qp_access_flags = IBV_ACCESS_REMOTE_READ |
                               IBV_ACCESS_REMOTE_WRITE |
                               IBV_ACCESS_REMOTE_ATOMIC;
        break;

    case IBV_QPT_UC:
        attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
        break;

    case IBV_EXP_QPT_DC_INI:
        LOG(ERROR) << "implement me:)";
        break;

    default:
        LOG(ERROR) << "implement me:)";
    }
    int ret;
    if ((ret = ibv_modify_qp(qp, &attr, IBV_QP_STATE)))
    {
        PLOG(ERROR) << "Failed to modify QP state to RESET. ret: %d" << ret;
        return false;
    }
    return true;
}
bool modifyQPtoInit(struct ibv_qp *qp, RdmaContext *context)
{
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = context->port;
    attr.pkey_index = 0;

    switch (qp->qp_type)
    {
    case IBV_QPT_RC:
        attr.qp_access_flags = IBV_ACCESS_REMOTE_READ |
                               IBV_ACCESS_REMOTE_WRITE |
                               IBV_ACCESS_REMOTE_ATOMIC;
        break;

    case IBV_QPT_UC:
        attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
        break;

    case IBV_EXP_QPT_DC_INI:
        LOG(ERROR) << "implement me:)";
        break;

    default:
        LOG(ERROR) << "implement me:)";
    }

    if (ibv_modify_qp(qp,
                      &attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                          IBV_QP_ACCESS_FLAGS))
    {
        PLOG(ERROR) << "Failed to modify QP state to INIT";
        return false;
    }
    return true;
}

bool modifyQPtoRTR(struct ibv_qp *qp,
                   uint32_t remoteQPN,
                   uint16_t remoteLid,
                   const uint8_t *remoteGid,
                   RdmaContext *context)
{
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;

    attr.path_mtu = IBV_MTU_4096;
    attr.dest_qp_num = remoteQPN;
    attr.rq_psn = PSN;

    fillAhAttr(&attr.ah_attr, remoteLid, remoteGid, context);

    int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN;

    if (qp->qp_type == IBV_QPT_RC)
    {
        attr.max_dest_rd_atomic = 16;
        attr.min_rnr_timer = 12;
        flags |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    }
    int ret = ibv_modify_qp(qp, &attr, flags);
    if (ret)
    {
        PLOG(ERROR) << "failed to modify QP state to RTR. ";
        return false;
    }
    return true;
}

bool modifyQPtoRTS(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    int flags;
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = PSN;
    flags = IBV_QP_STATE | IBV_QP_SQ_PSN;

    if (qp->qp_type == IBV_QPT_RC)
    {
        attr.timeout = 14;
        attr.retry_cnt = 7;
        attr.rnr_retry = 7;
        attr.max_rd_atomic = 16;
        flags |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                 IBV_QP_MAX_QP_RD_ATOMIC;
    }
    int ret = ibv_modify_qp(qp, &attr, flags);
    if (ret)
    {
        PLOG(ERROR) << "failed to modify QP state to RTS. ret";
        return false;
    }
    return true;
}

bool modifyUDtoRTS(struct ibv_qp *qp, RdmaContext *context)
{
    // assert(qp->qp_type == IBV_QPT_UD);

    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = context->port;
    attr.qkey = UD_PKEY;

    if (qp->qp_type == IBV_QPT_UD)
    {
        if (ibv_modify_qp(
                qp,
                &attr,
                IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY))
        {
            PLOG(ERROR) << "Failed to modify QP state to INIT";
            return false;
        }
    }
    else
    {
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PORT))
        {
            PLOG(ERROR) << "Failed to modify QP state to INIT";
            return false;
        }
    }

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    if (ibv_modify_qp(qp, &attr, IBV_QP_STATE))
    {
        PLOG(ERROR) << "failed to modify QP state to RTR";
        return false;
    }

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = PSN;

    if (qp->qp_type == IBV_QPT_UD)
    {
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN))
        {
            PLOG(ERROR) << "failed to modify QP state to RTS";
            return false;
        }
    }
    else
    {
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE))
        {
            PLOG(ERROR) << "failed to modify QP state to RTS";
            return false;
        }
    }
    return true;
}

bool modifyDCtoRTS(struct ibv_qp *qp,
                   uint16_t remoteLid,
                   uint8_t *remoteGid,
                   RdmaContext *context)
{
    // assert(qp->qp_type == IBV_EXP_QPT_DC_INI);

    struct ibv_exp_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = context->port;
    attr.qp_access_flags = 0;
    attr.dct_key = DCT_ACCESS_KEY;

    if (ibv_exp_modify_qp(qp,
                          &attr,
                          IBV_EXP_QP_STATE | IBV_EXP_QP_PKEY_INDEX |
                              IBV_EXP_QP_PORT | IBV_EXP_QP_DC_KEY))
    {
        PLOG(ERROR) << "failed to modify QP state to INI";
        return false;
    }

    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_4096;

    fillAhAttr(&attr.ah_attr, remoteLid, remoteGid, context);
    if (ibv_exp_modify_qp(
            qp, &attr, IBV_EXP_QP_STATE | IBV_EXP_QP_PATH_MTU | IBV_EXP_QP_AV))
    {
        PLOG(ERROR) << "failed to modify QP state to RTR";
        return false;
    }

    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.max_rd_atomic = 16;
    if (ibv_exp_modify_qp(qp,
                          &attr,
                          IBV_EXP_QP_STATE | IBV_EXP_QP_TIMEOUT |
                              IBV_EXP_QP_RETRY_CNT | IBV_EXP_QP_RNR_RETRY |
                              IBV_EXP_QP_MAX_QP_RD_ATOMIC))
    {
        PLOG(ERROR) << "failed to modify QP state to RTS";
        return false;
    }

    return true;
}
