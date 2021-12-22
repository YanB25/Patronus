#include <glog/logging.h>

#include "Rdma.h"

bool createContext(RdmaContext *context,
                   uint8_t port,
                   int gidIndex,
                   uint8_t devIndex)
{
    ibv_device *dev = NULL;
    ibv_context *ctx = NULL;
    ibv_pd *pd = NULL;
    ibv_port_attr portAttr;

    // get device names in the system
    int devicesNum;
    struct ibv_device **deviceList = ibv_get_device_list(&devicesNum);
    if (!deviceList)
    {
        LOG(ERROR) << "failed to get IB devices list";
        goto CreateResourcesExit;
    }

    // if there isn't any IB device in host
    if (!devicesNum)
    {
        LOG(INFO) << "found " << devicesNum << " device(s)";
        goto CreateResourcesExit;
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
        goto CreateResourcesExit;
    }

    dev = deviceList[devIndex];
    // printf("I open %s :)\n", ibv_get_device_name(dev));

    // get device handle
    ctx = ibv_open_device(dev);
    if (!ctx)
    {
        LOG(ERROR) << "failed to open device";
        goto CreateResourcesExit;
    }

    /* We are now done with device list, free it */
    ibv_free_device_list(deviceList);
    deviceList = NULL;

    // query port properties
    if (ibv_query_port(ctx, port, &portAttr))
    {
        LOG(ERROR) << "ibv_query_port failed";
        goto CreateResourcesExit;
    }

    struct ibv_device_attr dev_attr;
    memset(&dev_attr, 0, sizeof(dev_attr));
    if (ibv_query_device(ctx, &dev_attr))
    {
        PLOG(ERROR) << "failed to query device.";
    }
    else
    {
        auto flag = dev_attr.device_cap_flags;
        if ((flag & IBV_DEVICE_MEM_WINDOW_TYPE_2A) ||
            (flag & IBV_DEVICE_MEM_WINDOW_TYPE_2B))
        {
            LOG_FIRST_N(WARNING, 1) << "TODO: Although device seems to support "
                                       "memory window TYPE_2, "
                                       "we fall back to TYPE_1";
            context->mw_type = IBV_MW_TYPE_1;
            // context->mw_type = IBV_MW_TYPE_2;
        }
    }

    // allocate Protection Domain
    // info("Allocate Protection Domain");
    pd = ibv_alloc_pd(ctx);
    if (!pd)
    {
        LOG(ERROR) << "ibv_alloc_pd failed";
        goto CreateResourcesExit;
    }

    CHECK(portAttr.state == IBV_PORT_ARMED ||
          portAttr.state == IBV_PORT_ACTIVE);
    CHECK_LT(gidIndex, portAttr.gid_tbl_len) << "required from doc";
    CHECK_GE(port, 1);
    CHECK_LE(port, dev_attr.phys_port_cnt);

    if (ibv_query_gid(ctx, port, gidIndex, &context->gid))
    {
        PLOG(ERROR) << "could not get gid for port: " << (int) port
                    << " gidIndex: " << gidIndex;
        goto CreateResourcesExit;
    }

    // Success :)
    // dinfo("setup succeed.");
    context->devIndex = devIndex;
    context->gidIndex = gidIndex;
    context->port = port;
    context->ctx = ctx;
    context->pd = pd;
    context->lid = portAttr.lid;

    // CHECK device memory support
    if (kMaxDeviceMemorySize == 0)
    {
        checkDctSupported(ctx);
    }

    return true;

/* Error encountered, cleanup */
CreateResourcesExit:
    LOG(ERROR) << "Error Encountered, Cleanup ...";

    if (pd)
    {
        ibv_dealloc_pd(pd);
        pd = NULL;
    }
    if (ctx)
    {
        ibv_close_device(ctx);
        ctx = NULL;
    }
    if (deviceList)
    {
        ibv_free_device_list(deviceList);
        deviceList = NULL;
    }

    return false;
}

bool destroyContext(RdmaContext *context)
{
    bool rc = true;
    if (context->pd)
    {
        if (ibv_dealloc_pd(context->pd))
        {
            PLOG(ERROR) << "Failed to deallocate PD";
            rc = false;
        }
    }
    if (context->ctx)
    {
        if (ibv_close_device(context->ctx))
        {
            PLOG(ERROR) << "failed to close device context";
            rc = false;
        }
    }

    return rc;
}

ibv_mr *createMemoryRegion(uint64_t mm, uint64_t mmSize, const RdmaContext *ctx)
{
    ibv_mr *mr = NULL;
    mr = ibv_reg_mr(ctx->pd,
                    (void *) mm,
                    mmSize,
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC |
                        IBV_ACCESS_MW_BIND);

    if (!mr)
    {
        LOG(ERROR) << "Memory registration failed";
    }

    return mr;
}

bool destroyMemoryRegion(ibv_mr *mr)
{
    if (mr)
    {
        if (ibv_dereg_mr(mr))
        {
            PLOG(ERROR) << "failed to destroy mr";
            return false;
        }
    }
    return true;
}

ibv_mr *createMemoryRegionOnChip(uint64_t mm, uint64_t mmSize, RdmaContext *ctx)
{
    /* Device memory allocation request */
    struct ibv_exp_alloc_dm_attr dm_attr;
    memset(&dm_attr, 0, sizeof(dm_attr));
    dm_attr.length = mmSize;
    struct ibv_exp_dm *dm = ibv_exp_alloc_dm(ctx->ctx, &dm_attr);
    if (!dm)
    {
        LOG(ERROR) << "Allocate on-chip memory failed";
        return nullptr;
    }
    ctx->dm = dm;

    /* Device memory registration as memory region */
    struct ibv_exp_reg_mr_in mr_in;
    memset(&mr_in, 0, sizeof(mr_in));
    mr_in.pd = ctx->pd, mr_in.addr = (void *) mm, mr_in.length = mmSize,
    mr_in.exp_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                       IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC,
    mr_in.create_flags = 0;
    mr_in.dm = dm;
    mr_in.comp_mask = IBV_EXP_REG_MR_DM;
    struct ibv_mr *mr = ibv_exp_reg_mr(&mr_in);
    if (!mr)
    {
        LOG(ERROR) << "Memory registration failed";
        return nullptr;
    }

    // init zero
    char *buffer = (char *) malloc(mmSize);
    memset(buffer, 0, mmSize);

    struct ibv_exp_memcpy_dm_attr cpy_attr;
    memset(&cpy_attr, 0, sizeof(cpy_attr));
    cpy_attr.memcpy_dir = IBV_EXP_DM_CPY_TO_DEVICE;
    cpy_attr.host_addr = (void *) buffer;
    cpy_attr.length = mmSize;
    cpy_attr.dm_offset = 0;
    ibv_exp_memcpy_dm(dm, &cpy_attr);

    free(buffer);

    return mr;
}

bool destroyMemoryRegionOnChip(ibv_mr *mr, ibv_exp_dm *dm)
{
    if (mr)
    {
        if (!destroyMemoryRegion(mr))
        {
            return false;
        }
        CHECK(dm != nullptr)
            << "If dm is nullptr, please use regular destroyMemoryRegion.";
        if (ibv_exp_free_dm(dm))
        {
            PLOG(ERROR) << "failed to free dm";
            return false;
        }
    }
    return true;
}

bool createQueuePair(ibv_qp **qp,
                     ibv_qp_type mode,
                     ibv_cq *send_cq,
                     ibv_cq *recv_cq,
                     RdmaContext *context,
                     size_t max_send_wr,
                     size_t max_recv_wr,
                     uint32_t maxInlineData)
{
    struct ibv_exp_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_type = mode;
    attr.sq_sig_all = 0;
    attr.send_cq = send_cq;
    attr.recv_cq = recv_cq;
    attr.pd = context->pd;

    if (mode == IBV_QPT_RC)
    {
        attr.comp_mask = IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS |
                         IBV_EXP_QP_INIT_ATTR_PD |
                         IBV_EXP_QP_INIT_ATTR_ATOMICS_ARG;
        attr.max_atomic_arg = 32;
    }
    else
    {
        attr.comp_mask = IBV_EXP_QP_INIT_ATTR_PD;
    }

    attr.cap.max_send_wr = max_send_wr;
    attr.cap.max_recv_wr = max_recv_wr;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = maxInlineData;

    *qp = ibv_exp_create_qp(context->ctx, &attr);
    if (!(*qp))
    {
        LOG(ERROR) << "Failed to create QP";
        return false;
    }

    // info("Create Queue Pair with Num = %d", (*qp)->qp_num);

    return true;
}

bool createQueuePair(ibv_qp **qp,
                     ibv_qp_type mode,
                     ibv_cq *send_cq,
                     ibv_cq *recv_cq,
                     RdmaContext *context,
                     uint32_t qpsMaxDepth,
                     uint32_t maxInlineData)
{
    return createQueuePair(qp,
                           mode,
                           send_cq,
                           recv_cq,
                           context,
                           qpsMaxDepth,
                           qpsMaxDepth,
                           maxInlineData);
}

bool createQueuePair(ibv_qp **qp,
                     ibv_qp_type mode,
                     ibv_cq *cq,
                     RdmaContext *context,
                     uint32_t qpsMaxDepth,
                     uint32_t maxInlineData)
{
    return createQueuePair(
        qp, mode, cq, cq, context, qpsMaxDepth, maxInlineData);
}
bool destroyQueuePair(ibv_qp *qp)
{
    if (qp)
    {
        if (ibv_destroy_qp(qp))
        {
            PLOG(ERROR) << "failed to destroy QP";
            return false;
        }
    }
    return true;
}

bool createDCTarget(ibv_exp_dct **dct,
                    ibv_cq *cq,
                    RdmaContext *context,
                    uint32_t qpsMaxDepth,
                    uint32_t maxInlineData)
{
    // construct SRQ fot DC Target :)
    struct ibv_srq_init_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.attr.max_wr = qpsMaxDepth;
    attr.attr.max_sge = 1;
    ibv_srq *srq = ibv_create_srq(context->pd, &attr);

    ibv_exp_dct_init_attr dAttr;
    memset(&dAttr, 0, sizeof(dAttr));
    dAttr.pd = context->pd;
    dAttr.cq = cq;
    dAttr.srq = srq;
    dAttr.dc_key = DCT_ACCESS_KEY;
    dAttr.port = context->port;
    dAttr.access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_READ |
                         IBV_ACCESS_REMOTE_ATOMIC;
    dAttr.min_rnr_timer = 2;
    dAttr.tclass = 0;
    dAttr.flow_label = 0;
    dAttr.mtu = IBV_MTU_4096;
    dAttr.pkey_index = 0;
    dAttr.hop_limit = 1;
    dAttr.create_flags = 0;
    dAttr.inline_size = maxInlineData;

    *dct = ibv_exp_create_dct(context->ctx, &dAttr);
    if (dct == NULL)
    {
        LOG(ERROR) << "failed to create dc target";
        return false;
    }

    return true;
}

void fillAhAttr(ibv_ah_attr *attr,
                uint32_t remoteLid,
                const uint8_t *remoteGid,
                RdmaContext *context)
{
    (void) remoteGid;

    memset(attr, 0, sizeof(ibv_ah_attr));
    attr->dlid = remoteLid;
    attr->sl = 0;
    attr->src_path_bits = 0;
    attr->port_num = context->port;

    // attr->is_global = 0;

    // fill ah_attr with GRH
    attr->is_global = 1;
    memcpy(&attr->grh.dgid, remoteGid, 16);
    attr->grh.flow_label = 0;
    attr->grh.hop_limit = 1;
    attr->grh.sgid_index = context->gidIndex;
    attr->grh.traffic_class = 0;
}

bool destroyCompleteQueue(ibv_cq *cq)
{
    if (cq)
    {
        if (ibv_destroy_cq(cq))
        {
            PLOG(ERROR) << "failed to destroy cq";
            return false;
        }
    }
    return true;
}
