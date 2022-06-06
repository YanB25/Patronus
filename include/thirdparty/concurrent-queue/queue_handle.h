#pragma once
#ifndef PATRONUS_CONCURRENT_QUEUE_HANDLE_H_
#define PATRONUS_CONCURRENT_QUEUE_HANDLE_H_

#include "util/RetCode.h"

namespace patronus::cqueue
{
struct HandleConfig
{
    std::string name;
    bool bypass_prot{false};

    std::string conf_name() const
    {
        return name;
    }
};

inline std::ostream &operator<<(std::ostream &os, const HandleConfig &config)
{
    os << "{HandleConfig name: " << config.name
       << ", bypass_prot: " << config.bypass_prot << "}";
    return os;
}

template <typename T>
class QueueHandle
{
public:
    using pointer = std::shared_ptr<QueueHandle<T>>;
    QueueHandle(uint16_t node_id,
                uint64_t client_id,
                GlobalAddress meta,
                IRdmaAdaptor::pointer rdma_adpt,
                const HandleConfig &config)
        : node_id_(node_id),
          client_id_(client_id),
          meta_gaddr_(meta),
          rdma_adpt_(rdma_adpt),
          config_(config)
    {
    }
    static pointer new_instance(uint16_t node_id,
                                uint64_t client_id,
                                GlobalAddress meta,
                                IRdmaAdaptor::pointer rdma_adpt,
                                const HandleConfig &conf)
    {
        return std::make_shared<QueueHandle<T>>(
            node_id, client_id, meta, rdma_adpt, conf);
    }

    RetCode push(const T &t)
    {
        return kOk;
    }
    RetCode pop(T &t)
    {
        return kOk;
    }

    void read_meta()
    {
        if (unlikely(!meta_handle_.valid()))
        {
            update_meta_handle();
        }
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(meta_size());
        auto rc = rdma_adpt_->rdma_read(
            rdma_buf.buffer, meta_gaddr_, meta_size(), meta_handle_);
        CHECK_EQ(rc, kOk);
        rc = rdma_adpt_->commit();
        CHECK_EQ(rc, kOk);
        memcpy(&cached_meta_, rdma_buf.buffer, meta_size());

        rc = rdma_adpt_->put_all_rdma_buffer();
        CHECK_EQ(rc, kOk);
    }
    Meta cached_meta() const
    {
        return cached_meta_;
    }
    size_t meta_size() const
    {
        return Meta::size();
    }

    ~QueueHandle()
    {
        if (meta_handle_.valid())
        {
            auto rel_flag = (flag_t) 0;
            rdma_adpt_->relinquish_perm(meta_handle_, 0 /* hint */, rel_flag);
        }
    }

private:
    void update_meta_handle()
    {
        CHECK(!meta_handle_.valid());
        auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                       (flag_t) AcquireRequestFlag::kNoBindPR;
        meta_handle_ = rdma_adpt_->acquire_perm(
            meta_gaddr_, 0 /* alloc_hint */, meta_size(), 0ns, ac_flag);
        CHECK(meta_handle_.valid());
    }

    uint16_t node_id_;
    uint64_t client_id_;
    GlobalAddress meta_gaddr_;
    IRdmaAdaptor::pointer rdma_adpt_;
    HandleConfig config_;

    RemoteMemHandle meta_handle_;

    Meta cached_meta_;
};
}  // namespace patronus::cqueue

#endif