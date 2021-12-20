#pragma once
#ifndef RELIABLE_MESSAGE_CONNECTION_H_
#define RELIABLE_MESSAGE_CONNECTION_H_

#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <vector>

#include "AbstractMessageConnection.h"
#include "Common.h"
#include "Pool.h"
// #include "RawMessageConnection.h"

class ReliableRecvMessageConnection
{
    constexpr static size_t kMessageSize = 32;

    const static int kRecvBuffer = 128;
    // post 2 batch in advance.
    const static int kPostRecvBufferAdvanceBatch = 2;
    const static int kPostRecvBufferBatch = 64;

public:
    ReliableRecvMessageConnection(size_t machine_nr);
    ~ReliableRecvMessageConnection();
    void recv(char *ibuf);
    bool try_recv(char *ibuf);
    void init();
    void fills(ibv_sge &sge, ibv_recv_wr &wr, size_t node_id, size_t batch_id);
    const std::vector<ibv_qp *> &QPs() const
    {
        return QPs_;
    }
    RdmaContext &context()
    {
        return ctx_;
    }
    uint32_t rkey() const
    {
        return mr_->rkey;
    }

private:
    bool inited_{false};
    RdmaContext ctx_;
    void poll_cq();

    /**
     * @brief maintain QPs[machineNR]
     */
    std::vector<ibv_qp *> QPs_;
    void *msg_pool_{nullptr};
    ibv_mr *mr_{nullptr};
    uint32_t lkey_{0};

    size_t get_msg_pool_idx(size_t node_id, size_t batch_id)
    {
        return node_id * kRecvBuffer + batch_id;
    }

    ibv_recv_wr recvs[MAX_MACHINE][kRecvBuffer];
    ibv_sge recv_sgl[MAX_MACHINE][kRecvBuffer];

    std::array<std::atomic<size_t>, MAX_MACHINE> msg_recv_index_{};

    ibv_cq *recv_cq_{nullptr};
};

class ReliableSendMessageConnection
{
    constexpr static size_t kMessageSize = 32;
    const static int kSendBatch = 16;

public:
    ReliableSendMessageConnection(uint64_t mm,
                                  size_t mm_size,
                                  size_t machine_nr);
    ~ReliableSendMessageConnection();
    void send(size_t node_id, const char *buf, size_t size);

    const std::vector<ibv_qp *> &QPs() const
    {
        return QPs_;
    }
    RdmaContext &context()
    {
        return ctx_;
    }
    uint32_t rkey() const
    {
        return mr_->rkey;
    }

private:
    void poll_cq();

    /**
     * @brief maintain QPs[machineNR]
     */
    std::vector<ibv_qp *> QPs_;
    RdmaContext ctx_;
    ibv_cq *send_cq_{nullptr};

    std::atomic<size_t> msg_send_index_{0};
    std::atomic<bool> second_{false};

    uint32_t lkey_{0};
    ibv_mr *mr_{nullptr};

    std::atomic<size_t> msg_sent_nr_{0};
};

class ReliableConnection
{
public:
    /**
     * @brief Construct a new Reliable Connection object
     *
     * @param mm  the RDMA buffer for local access
     * @param mm_size  the size of mm
     * @param machine_nr
     */
    ReliableConnection(uint64_t mm, size_t mm_size, size_t machine_nr)
        : recv_(machine_nr), send_(mm, mm_size, machine_nr)
    {
    }
    void send(const char *buf, size_t size, uint16_t node_id)
    {
        return send_.send(node_id, buf, size);
    }
    void recv(char *ibuf)
    {
        return recv_.recv(ibuf);
    }
    bool try_recv(char *ibuf)
    {
        return recv_.try_recv(ibuf);
    }

private:
    friend class DSMKeeper;
    ReliableRecvMessageConnection recv_;
    ReliableSendMessageConnection send_;
};

#endif