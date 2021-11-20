/**
 * @file rdma
 */
#ifndef _RDMA_H__
#define _RDMA_H__

#define forceinline inline __attribute__((always_inline))

#include <assert.h>
#include <infiniband/verbs.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#include <cstring>
#include <list>
#include <string>

#include "Debug.h"

#define MAX_POST_LIST 32
#define DCT_ACCESS_KEY 3185
#define UD_PKEY 0x11111111
#define PSN 3185

constexpr int kOroMax = 3;
struct RdmaOpRegion
{
    uint64_t source;
    uint64_t dest;
    uint64_t size;

    uint32_t lkey;
    union
    {
        uint32_t remoteRKey;
        bool is_on_chip;
    };
};

extern int kMaxDeviceMemorySize;

struct RdmaContext
{
    uint8_t devIndex;
    uint8_t port;
    int gidIndex;

    ibv_context *ctx;
    ibv_pd *pd;

    uint16_t lid;
    union ibv_gid gid;
    ibv_mw_type mw_type;

    // default to use type 1, because it is always supported
    // will query and change to TYPE_2 if supported.
    RdmaContext() : ctx(NULL), pd(NULL), mw_type(IBV_MW_TYPE_1)
    {
    }
};

struct Region
{
    uint64_t source;
    uint32_t size;

    uint64_t dest;
};

//// Resource.cpp
bool createContext(RdmaContext *context,
                   uint8_t port = 1,
                   int gidIndex = 1,
                   uint8_t devIndex = 0);
bool destoryContext(RdmaContext *context);

ibv_mr *createMemoryRegion(uint64_t mm, uint64_t mmSize, RdmaContext *ctx);
ibv_mr *createMemoryRegionOnChip(uint64_t mm,
                                 uint64_t mmSize,
                                 RdmaContext *ctx);

bool createQueuePair(ibv_qp **qp,
                     ibv_qp_type mode,
                     ibv_cq *cq,
                     RdmaContext *context,
                     uint32_t qpsMaxDepth = 128,
                     uint32_t maxInlineData = 0);

bool createQueuePair(ibv_qp **qp,
                     ibv_qp_type mode,
                     ibv_cq *send_cq,
                     ibv_cq *recv_cq,
                     RdmaContext *context,
                     uint32_t qpsMaxDepth = 128,
                     uint32_t maxInlineData = 0);

bool createDCTarget(ibv_exp_dct **dct,
                    ibv_cq *cq,
                    RdmaContext *context,
                    uint32_t qpsMaxDepth = 128,
                    uint32_t maxInlineData = 0);
void fillAhAttr(ibv_ah_attr *attr,
                uint32_t remoteLid,
                const uint8_t *remoteGid,
                RdmaContext *context);

//// StateTrans.cpp
bool modifyQPtoInit(struct ibv_qp *qp, RdmaContext *context);
bool modifyQPtoRTR(struct ibv_qp *qp,
                   uint32_t remoteQPN,
                   uint16_t remoteLid,
                   const uint8_t *gid,
                   RdmaContext *context);
bool modifyQPtoRTS(struct ibv_qp *qp);

bool modifyUDtoRTS(struct ibv_qp *qp, RdmaContext *context);
bool modifyDCtoRTS(struct ibv_qp *qp,
                   uint16_t remoteLid,
                   uint8_t *remoteGid,
                   RdmaContext *context);

//// Operation.cpp
/**
 * @brief block and poll the CQ until the specified number
 * @param cq the CQ to polled with
 * @param pollNumber the number to poll until returns
 * @param wc the polling results
 * @return the number of wc actually polled.
 */
int pollWithCQ(ibv_cq *cq, int pollNumber, struct ibv_wc *wc);
/**
 * @brief non-blocking and try to poll the CQ and return immediately if get
 * nothing.
 * @param cq
 * @param pollNumber to number used to poll with
 * @param wc the polling results
 * @return the number of wc actually polled.
 */
int pollOnce(ibv_cq *cq, int pollNumber, struct ibv_wc *wc);

/**
 * @brief rdma send for UD and DC
 * @param qp
 * @param source sge.addr
 * @param size sge.length
 * @param lkey sge.lkey
 * @param ah wr.ud.ah
 * @param remoeQPN wr.ud.remote_qpn
 * @param isSignaled
 * @return whether or not operation succeeds
 */
bool rdmaSend(ibv_qp *qp,
              uint64_t source,
              uint64_t size,
              uint32_t lkey,
              ibv_ah *ah,
              uint32_t remoteQPN,
              bool isSignaled = false);

/**
 * @brief rdma send for RC and UC
 * @param imm the immediate number, -1 for null.
 * @return whether or not operation succeeds.
 */
bool rdmaSend(ibv_qp *qp,
              uint64_t source,
              uint64_t size,
              uint32_t lkey,
              int32_t imm = -1);
/**
 * @brief rdma receive
 */
bool rdmaReceive(ibv_qp *qp,
                 uint64_t source,
                 uint64_t size,
                 uint32_t lkey,
                 uint64_t wr_id = 0);
/**
 * @brief rdma receive for shared receive queue
 */
bool rdmaReceive(ibv_srq *srq, uint64_t source, uint64_t size, uint32_t lkey);
/**
 * @brief rdma receive for dct
 */
bool rdmaReceive(ibv_exp_dct *dct,
                 uint64_t source,
                 uint64_t size,
                 uint32_t lkey);

bool rdmaRead(ibv_qp *qp,
              uint64_t source,
              uint64_t dest,
              uint64_t size,
              uint32_t lkey,
              uint32_t remoteRKey,
              bool signal = true,
              uint64_t wrID = 0);
bool rdmaRead(ibv_qp *qp,
              uint64_t source,
              uint64_t dest,
              uint64_t size,
              uint32_t lkey,
              uint32_t remoteRKey,
              ibv_ah *ah,
              uint32_t remoteDctNumber);
/**
 * @brief rdma write to RC and UC
 * @param remoteRKey wr.rdma.rkey
 */
bool rdmaWrite(ibv_qp *qp,
               uint64_t source,
               uint64_t dest,
               uint64_t size,
               uint32_t lkey,
               uint32_t remoteRKey,
               int32_t imm = -1,
               bool isSignaled = true,
               uint64_t wrID = 0);
/**
 * @brief rdma write for dct
 */
bool rdmaWrite(ibv_qp *qp,
               uint64_t source,
               uint64_t dest,
               uint64_t size,
               uint32_t lkey,
               uint32_t remoteRKey,
               ibv_ah *ah,
               uint32_t remoteDctNumber,
               int32_t imm);

bool rdmaFetchAndAdd(ibv_qp *qp,
                     uint64_t source,
                     uint64_t dest,
                     uint64_t add,
                     uint32_t lkey,
                     uint32_t remoteRKey);
bool rdmaFetchAndAdd(ibv_qp *qp,
                     uint64_t source,
                     uint64_t dest,
                     uint64_t add,
                     uint32_t lkey,
                     uint32_t remoteRKey,
                     ibv_ah *ah,
                     uint32_t remoteDctNumber);
bool rdmaFetchAndAddBoundary(ibv_qp *qp,
                             uint64_t source,
                             uint64_t dest,
                             uint64_t add,
                             uint32_t lkey,
                             uint32_t remoteRKey,
                             uint64_t boundary = 63,
                             bool singal = true,
                             uint64_t wr_id = 0);

bool rdmaCompareAndSwap(ibv_qp *qp,
                        uint64_t source,
                        uint64_t dest,
                        uint64_t compare,
                        uint64_t swap,
                        uint32_t lkey,
                        uint32_t remoteRKey,
                        bool signal = true,
                        uint64_t wrID = 0);
bool rdmaCompareAndSwap(ibv_qp *qp,
                        uint64_t source,
                        uint64_t dest,
                        uint64_t compare,
                        uint64_t swap,
                        uint32_t lkey,
                        uint32_t remoteRKey,
                        ibv_ah *ah,
                        uint32_t remoteDctNumber);
bool rdmaCompareAndSwapMask(ibv_qp *qp,
                            uint64_t source,
                            uint64_t dest,
                            uint64_t compare,
                            uint64_t swap,
                            uint32_t lkey,
                            uint32_t remoteRKey,
                            uint64_t mask = ~(0ull),
                            bool signal = true);

//// Batch.cpp
bool rdmaBatchSend(ibv_qp *qp,
                   const std::list<Region> &regions,
                   uint32_t lkey,
                   uint32_t signalBatch,
                   uint64_t &counter,
                   bool isInline = false,
                   int32_t imm = -1);
bool rdmaBatchSend(ibv_qp *qp,
                   const std::list<Region> &regions,
                   uint32_t lkey,
                   uint32_t signalBatch,
                   uint64_t &counter,
                   ibv_ah *ah,
                   uint32_t remoteQPN,
                   bool isInline = false,
                   int32_t imm = -1);

bool rdmaBatchReceive(ibv_qp *qp,
                      const std::list<Region> &regions,
                      uint32_t lkey);
bool rdmaBatchReceive(ibv_srq *srq,
                      const std::list<Region> &regions,
                      uint32_t lkey);
bool rdmaBatchReceive(ibv_exp_dct *dct,
                      const std::list<Region> &regions,
                      uint32_t lkey);

bool rdmaBatchRead(ibv_qp *qp,
                   const std::list<Region> &regions,
                   uint32_t lkey,
                   uint32_t signalBatch,
                   uint64_t &counter,
                   uint32_t remoteRKey,
                   bool isInline = false);
bool rdmaBatchRead(ibv_qp *qp,
                   const std::list<Region> &regions,
                   uint32_t lkey,
                   uint32_t signalBatch,
                   uint64_t &counter,
                   uint32_t remoteRKey,
                   ibv_ah *ah,
                   uint32_t remoteDctNumber,
                   bool isInline = false);

bool rdmaBatchWrite(ibv_qp *qp,
                    const std::list<Region> &regions,
                    uint32_t lkey,
                    uint32_t signalBatch,
                    uint64_t &counter,
                    uint32_t remoteRKey,
                    bool isInline = false,
                    int32_t imm = -1);
bool rdmaBatchWrite(ibv_qp *qp,
                    const std::list<Region> &regions,
                    uint32_t lkey,
                    uint32_t signalBatch,
                    uint64_t &counter,
                    uint32_t remoteRKey,
                    ibv_ah *ah,
                    uint32_t remoteDctNumber,
                    bool isInline = false,
                    int32_t imm = -1);

//// Utility.cpp
void rdmaQueryQueuePair(ibv_qp *qp);
void checkDctSupported(struct ibv_context *ctx);
/**
 * print out the information of devices
 */
void rdmaQueryDevice();

//// specified
bool rdmaWriteBatch(
    ibv_qp *qp, RdmaOpRegion *ror, int k, bool isSignaled, uint64_t wrID = 0);
bool rdmaCasRead(ibv_qp *qp,
                 const RdmaOpRegion &cas_ror,
                 const RdmaOpRegion &read_ror,
                 uint64_t compare,
                 uint64_t swap,
                 bool isSignaled,
                 uint64_t wrID = 0);
bool rdmaWriteFaa(ibv_qp *qp,
                  const RdmaOpRegion &write_ror,
                  const RdmaOpRegion &faa_ror,
                  uint64_t add_val,
                  bool isSignaled,
                  uint64_t wrID = 0);
bool rdmaWriteCas(ibv_qp *qp,
                  const RdmaOpRegion &write_ror,
                  const RdmaOpRegion &cas_ror,
                  uint64_t compare,
                  uint64_t swap,
                  bool isSignaled,
                  uint64_t wrID = 0);

constexpr int IBV_ACCESS_CUSTOM_REMOTE_ALL =
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
    IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_MW_BIND;

/**
 * bind a memory window to a part of memory region.
 * @param mw from createMemoryWindow
 * @param mm
 * @param mmSize the part of memory region to bind
 * @param mw_access_flag the access permission like memory region
 * @return the RKey to the memory window. return 0 on failure
 */
uint32_t rdmaAsyncBindMemoryWindow(
    ibv_qp *qp,
    ibv_mw *mw,
    struct ibv_mr *mr,
    uint64_t mm,
    uint64_t mmSize,
    bool signal,
    uint64_t wrID = 0,
    unsigned int mw_access_flag = IBV_ACCESS_CUSTOM_REMOTE_ALL);
#endif
