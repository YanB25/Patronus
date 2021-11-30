#include "Debug.h"
#include "Rdma.h"
#include "Util.h"

int kMaxDeviceMemorySize = 0;

void rdmaReportQueuePair(ibv_qp *qp)
{
    auto state = rdmaQueryQueuePair(qp);
    switch (state)
    {
    case IBV_QPS_RESET:
        printf("QP state: IBV_QPS_RESET. qp: %p\n", qp);
        break;
    case IBV_QPS_INIT:
        printf("QP state: IBV_QPS_INIT. qp: %p\n", qp);
        break;
    case IBV_QPS_RTR:
        printf("QP state: IBV_QPS_RTR. qp: %p\n", qp);
        break;
    case IBV_QPS_RTS:
        printf("QP state: IBV_QPS_RTS. qp: %p\n", qp);
        break;
    case IBV_QPS_SQD:
        printf("QP state: IBV_QPS_SQD. qp: %p\n", qp);
        break;
    case IBV_QPS_SQE:
        printf("QP state: IBV_QPS_SQE. qp: %p\n", qp);
        break;
    case IBV_QPS_ERR:
        printf("QP state: IBV_QPS_ERR. qp: %p\n", qp);
        break;
    case IBV_QPS_UNKNOWN:
        printf("QP state: IBV_QPS_UNKNOWN. qp: %p\n", qp);
        break;
    }
}

ibv_qp_state rdmaQueryQueuePair(ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr;
    ibv_query_qp(qp, &attr, IBV_QP_STATE, &init_attr);
    return attr.qp_state;
}

void checkDctSupported(struct ibv_context *ctx)
{
    // printf("Checking if DCT is supported.. ");
    struct ibv_exp_device_attr attrs;

    attrs.comp_mask = IBV_EXP_DEVICE_ATTR_UMR;
    attrs.comp_mask |= IBV_EXP_DEVICE_ATTR_MAX_DM_SIZE;

    if (ibv_exp_query_device(ctx, &attrs))
    {
        printf("Couldn't query device attributes\n");
    }

    if (!(attrs.comp_mask & IBV_EXP_DEVICE_ATTR_MAX_DM_SIZE))
    {
        fprintf(stderr, "Can not support Device Memory!\n");
        exit(-1);
    }
    else if (!(attrs.max_dm_size))
    {
    }
    else
    {
        kMaxDeviceMemorySize = attrs.max_dm_size;
        info("NIC Device Memory is %s",
             smart::smartSize(kMaxDeviceMemorySize).c_str());
    }
}
