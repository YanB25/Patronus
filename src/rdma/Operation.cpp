#include "Rdma.h"

int pollWithCQ(ibv_cq *cq, int pollNumber, struct ibv_wc *wc) {
  int count = 0;

  do {

    int new_count = ibv_poll_cq(cq, 1, wc);
    count += new_count;

  } while (count < pollNumber);

  if (count < 0) {
    Debug::notifyError("Poll Completion failed.");
    return -1;
  }

  if (wc->status != IBV_WC_SUCCESS) {
    Debug::notifyError("Failed status %s (%d) for wr_id %d",
                       ibv_wc_status_str(wc->status), wc->status,
                       (int)wc->wr_id);
    return -1;
  }

  return count;
}

int pollOnce(ibv_cq *cq, int pollNumber, struct ibv_wc *wc) {
  int count = ibv_poll_cq(cq, pollNumber, wc);
  if (count <= 0) {
    return 0;
  }
  if (wc->status != IBV_WC_SUCCESS) {
    Debug::notifyError("Failed status %s (%d) for wr_id %d",
                       ibv_wc_status_str(wc->status), wc->status,
                       (int)wc->wr_id);
    return -1;
  } else {
    return count;
  }
}

static inline void fillSgeWr(ibv_sge &sg, ibv_send_wr &wr, uint64_t source,
                             uint64_t size, uint32_t lkey) {
  memset(&sg, 0, sizeof(sg));
  sg.addr = (uintptr_t)source;
  sg.length = size;
  sg.lkey = lkey;

  memset(&wr, 0, sizeof(wr));
  wr.wr_id = 0;
  wr.sg_list = &sg;
  wr.num_sge = 1;
}

static inline void fillSgeWr(ibv_sge &sg, ibv_recv_wr &wr, uint64_t source,
                             uint64_t size, uint32_t lkey) {
  memset(&sg, 0, sizeof(sg));
  sg.addr = (uintptr_t)source;
  sg.length = size;
  sg.lkey = lkey;

  memset(&wr, 0, sizeof(wr));
  wr.wr_id = 0;
  wr.sg_list = &sg;
  wr.num_sge = 1;
}

static inline void fillSgeWr(ibv_sge &sg, ibv_exp_send_wr &wr, uint64_t source,
                             uint64_t size, uint32_t lkey) {
  memset(&sg, 0, sizeof(sg));
  sg.addr = (uintptr_t)source;
  sg.length = size;
  sg.lkey = lkey;

  memset(&wr, 0, sizeof(wr));
  wr.wr_id = 0;
  wr.sg_list = &sg;
  wr.num_sge = 1;
}

// for UD and DC
bool rdmaSend(ibv_qp *qp, uint64_t source, uint64_t size, uint32_t lkey,
              ibv_ah *ah, uint32_t remoteQPN /* remote dct_number */,
              bool isSignaled) {

  struct ibv_sge sg;
  struct ibv_send_wr wr;
  struct ibv_send_wr *wrBad;

  fillSgeWr(sg, wr, source, size, lkey);

  wr.opcode = IBV_WR_SEND;

  wr.wr.ud.ah = ah;
  wr.wr.ud.remote_qpn = remoteQPN;
  wr.wr.ud.remote_qkey = UD_PKEY;

  if (isSignaled)
    wr.send_flags = IBV_SEND_SIGNALED;
  if (ibv_post_send(qp, &wr, &wrBad)) {
    Debug::notifyError("Send with RDMA_SEND failed.");
    return false;
  }
  return true;
}

// for RC & UC
bool rdmaSend(ibv_qp *qp, uint64_t source, uint64_t size, uint32_t lkey,
              int32_t imm) {

  struct ibv_sge sg;
  struct ibv_send_wr wr;
  struct ibv_send_wr *wrBad;

  fillSgeWr(sg, wr, source, size, lkey);

  if (imm != -1) {
    wr.imm_data = imm;
    wr.opcode = IBV_WR_SEND_WITH_IMM;
  } else {
    wr.opcode = IBV_WR_SEND;
  }

  wr.send_flags = IBV_SEND_SIGNALED;
  if (ibv_post_send(qp, &wr, &wrBad)) {
    Debug::notifyError("Send with RDMA_SEND failed.");
    return false;
  }
  return true;
}

bool rdmaReceive(ibv_qp *qp, uint64_t source, uint64_t size, uint32_t lkey,
                 uint64_t wr_id) {
  struct ibv_sge sg;
  struct ibv_recv_wr wr;
  struct ibv_recv_wr *wrBad;

  fillSgeWr(sg, wr, source, size, lkey);

  wr.wr_id = wr_id;

  if (ibv_post_recv(qp, &wr, &wrBad)) {
    Debug::notifyError("Receive with RDMA_RECV failed.");
    return false;
  }
  return true;
}

bool rdmaReceive(ibv_srq *srq, uint64_t source, uint64_t size, uint32_t lkey) {

  struct ibv_sge sg;
  struct ibv_recv_wr wr;
  struct ibv_recv_wr *wrBad;

  fillSgeWr(sg, wr, source, size, lkey);

  if (ibv_post_srq_recv(srq, &wr, &wrBad)) {
    Debug::notifyError("Receive with RDMA_RECV failed.");
    return false;
  }
  return true;
}

bool rdmaReceive(ibv_exp_dct *dct, uint64_t source, uint64_t size,
                 uint32_t lkey) {
  return rdmaReceive(dct->srq, source, size, lkey);
}

// for RC & UC
bool rdmaRead(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t size,
              uint32_t lkey, uint32_t remoteRKey, uint64_t wrID) {
  struct ibv_sge sg;
  struct ibv_send_wr wr;
  struct ibv_send_wr *wrBad;

  fillSgeWr(sg, wr, source, size, lkey);

  wr.opcode = IBV_WR_RDMA_READ;

  wr.send_flags = IBV_SEND_SIGNALED;

  wr.wr.rdma.remote_addr = dest;
  wr.wr.rdma.rkey = remoteRKey;
  wr.wr_id = wrID;

  if (ibv_post_send(qp, &wr, &wrBad)) {
    Debug::notifyError("Send with RDMA_READ failed.");
    return false;
  }
  return true;
}

// for DC
bool rdmaRead(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t size,
              uint32_t lkey, uint32_t remoteRKey, ibv_ah *ah,
              uint32_t remoteDctNumber) {
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

  if (ibv_exp_post_send(qp, &wr, &wrBad)) {
    Debug::notifyError("Send with RDMA_READ failed.");
    return false;
  }
  return true;
}

// for RC & UC
bool rdmaWrite(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t size,
               uint32_t lkey, uint32_t remoteRKey, int32_t imm, bool isSignaled,
               uint64_t wrID) {

  struct ibv_sge sg;
  struct ibv_send_wr wr;
  struct ibv_send_wr *wrBad;

  fillSgeWr(sg, wr, source, size, lkey);

  if (imm == -1) {
    wr.opcode = IBV_WR_RDMA_WRITE;
  } else {
    wr.imm_data = imm;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
  }

  if (isSignaled) {
    wr.send_flags = IBV_SEND_SIGNALED;
  }

  wr.wr.rdma.remote_addr = dest;
  wr.wr.rdma.rkey = remoteRKey;
  wr.wr_id = wrID;

  if (ibv_post_send(qp, &wr, &wrBad) != 0) {
    Debug::notifyError("Send with RDMA_WRITE(WITH_IMM) failed.");
    return false;
  }
  return true;
}

// for DC
bool rdmaWrite(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t size,
               uint32_t lkey, uint32_t remoteRKey, ibv_ah *ah,
               uint32_t remoteDctNumber, int32_t imm) {

  struct ibv_sge sg;
  struct ibv_exp_send_wr wr;
  struct ibv_exp_send_wr *wrBad;

  fillSgeWr(sg, wr, source, size, lkey);

  if (imm == -1) {
    wr.exp_opcode = IBV_EXP_WR_RDMA_WRITE;
  } else {
    wr.ex.imm_data = imm;
    wr.exp_opcode = IBV_EXP_WR_RDMA_WRITE_WITH_IMM;
  }
  wr.exp_send_flags = IBV_SEND_SIGNALED;

  wr.wr.rdma.remote_addr = dest;
  wr.wr.rdma.rkey = remoteRKey;

  wr.dc.ah = ah;
  wr.dc.dct_access_key = DCT_ACCESS_KEY;
  wr.dc.dct_number = remoteDctNumber;

  if (ibv_exp_post_send(qp, &wr, &wrBad) != 0) {
    Debug::notifyError("Send with RDMA_WRITE(WITH_IMM) failed.");
    return false;
  }
  return true;
}

// RC & UC
bool rdmaFetchAndAdd(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t add,
                     uint32_t lkey, uint32_t remoteRKey) {
  struct ibv_sge sg;
  struct ibv_send_wr wr;
  struct ibv_send_wr *wrBad;

  fillSgeWr(sg, wr, source, 8, lkey);

  wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
  wr.send_flags = IBV_SEND_SIGNALED;

  wr.wr.atomic.remote_addr = dest;
  wr.wr.atomic.rkey = remoteRKey;
  wr.wr.atomic.compare_add = add;

  if (ibv_post_send(qp, &wr, &wrBad)) {
    Debug::notifyError("Send with ATOMIC_FETCH_AND_ADD failed.");
    return false;
  }
  return true;
}

// DC
bool rdmaFetchAndAdd(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t add,
                     uint32_t lkey, uint32_t remoteRKey, ibv_ah *ah,
                     uint32_t remoteDctNumber) {
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

  if (ibv_exp_post_send(qp, &wr, &wrBad)) {
    Debug::notifyError("Send with ATOMIC_FETCH_AND_ADD failed.");
    return false;
  }
  return true;
}

// for RC & UC
bool rdmaCompareAndSwap(ibv_qp *qp, uint64_t source, uint64_t dest,
                        uint64_t compare, uint64_t swap, uint32_t lkey,
                        uint32_t remoteRKey) {
  struct ibv_sge sg;
  struct ibv_send_wr wr;
  struct ibv_send_wr *wrBad;

  fillSgeWr(sg, wr, source, 8, lkey);

  wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  wr.send_flags = IBV_SEND_SIGNALED;

  wr.wr.atomic.remote_addr = dest;
  wr.wr.atomic.rkey = remoteRKey;
  wr.wr.atomic.compare_add = compare;
  wr.wr.atomic.swap = swap;

  if (ibv_post_send(qp, &wr, &wrBad)) {
    Debug::notifyError("Send with ATOMIC_CMP_AND_SWP failed.");
    return false;
  }
  return true;
}

bool rdmaCompareAndSwapMask(ibv_qp *qp, uint64_t source, uint64_t dest,
                        uint64_t compare, uint64_t swap, uint32_t lkey,
                        uint32_t remoteRKey, uint64_t mask) {
  struct ibv_sge sg;
  struct ibv_exp_send_wr wr;
  struct ibv_exp_send_wr *wrBad;

  fillSgeWr(sg, wr, source, 8, lkey);

  wr.exp_opcode = IBV_EXP_WR_EXT_MASKED_ATOMIC_CMP_AND_SWP;
  wr.exp_send_flags = IBV_EXP_SEND_SIGNALED | IBV_EXP_SEND_EXT_ATOMIC_INLINE;
 
  wr.ext_op.masked_atomics.log_arg_sz = 3;
  wr.ext_op.masked_atomics.remote_addr = dest;
  wr.ext_op.masked_atomics.rkey = remoteRKey;

  auto &op = wr.ext_op.masked_atomics.wr_data.inline_data.op.cmp_swap;
  op.compare_val = compare;
  op.swap_val = swap;

  op.compare_mask = mask;
  op.swap_mask = mask;

  if (ibv_exp_post_send(qp, &wr, &wrBad)) {
    Debug::notifyError("Send with ATOMIC_CMP_AND_SWP failed.");
    return false;
  }
  return true;
}

// DC
bool rdmaCompareAndSwap(ibv_qp *qp, uint64_t source, uint64_t dest,
                        uint64_t compare, uint64_t swap, uint32_t lkey,
                        uint32_t remoteRKey, ibv_ah *ah,
                        uint32_t remoteDctNumber) {
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

  if (ibv_exp_post_send(qp, &wr, &wrBad)) {
    Debug::notifyError("Send with ATOMIC_CMP_AND_SWP failed.");
    return false;
  }
  return true;
}
