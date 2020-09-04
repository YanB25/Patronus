#include "Rdma.h"

int kMaxDeviceMemorySize = 0;

void rdmaQueryQueuePair(ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr;
    ibv_query_qp(qp, &attr, IBV_QP_STATE, &init_attr);
    switch (attr.qp_state)
    {
    case IBV_QPS_RESET:
        printf("QP state: IBV_QPS_RESET\n");
        break;
    case IBV_QPS_INIT:
        printf("QP state: IBV_QPS_INIT\n");
        break;
    case IBV_QPS_RTR:
        printf("QP state: IBV_QPS_RTR\n");
        break;
    case IBV_QPS_RTS:
        printf("QP state: IBV_QPS_RTS\n");
        break;
    case IBV_QPS_SQD:
        printf("QP state: IBV_QPS_SQD\n");
        break;
    case IBV_QPS_SQE:
        printf("QP state: IBV_QPS_SQE\n");
        break;
    case IBV_QPS_ERR:
        printf("QP state: IBV_QPS_ERR\n");
        break;
    case IBV_QPS_UNKNOWN:
        printf("QP state: IBV_QPS_UNKNOWN\n");
        break;
    }
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
        printf("NIC Device Memory is %dKB\n", kMaxDeviceMemorySize / 1024);
        // printf("DM %d\n", maxMemic);
    }

    return;

    struct ibv_exp_device_attr dattr;
    dattr.comp_mask = IBV_EXP_DEVICE_ATTR_EXP_CAP_FLAGS |
                      IBV_EXP_DEVICE_DC_RD_REQ | IBV_EXP_DEVICE_DC_RD_RES |
                      IBV_EXP_DEVICE_ATTR_EXT_ATOMIC_ARGS;
    int err = ibv_exp_query_device(ctx, &dattr);
    if (err)
    {
        printf("couldn't query device extended attributes\n");
        assert(false);
    }
    else
    {
        if (!(dattr.comp_mask & IBV_EXP_DEVICE_ATTR_EXP_CAP_FLAGS))
        {
            printf("no extended capability flgas\n");
            assert(false);
        }
        if (!(dattr.exp_device_cap_flags & IBV_EXP_DEVICE_DC_TRANSPORT))
        {
            printf("DC transport not enabled\n");
            assert(false);
        }

        if (!(dattr.comp_mask & IBV_EXP_DEVICE_DC_RD_REQ))
        {
            printf("no report on max requestor rdma/atomic resources\n");
            assert(false);
        }

        if (!(dattr.comp_mask & IBV_EXP_DEVICE_DC_RD_RES))
        {
            printf("no report on max responder rdma/atomic resources\n");
            assert(false);
        }

        //  if (!(dattr.comp_mask & IBV_EXP_DEVICE_ATTR_MASKED_ATOMICS)) {
        //     printf("no report on extended atomic\n");
        //     assert(false);
        // }
    }
    printf("Success\n");
}
