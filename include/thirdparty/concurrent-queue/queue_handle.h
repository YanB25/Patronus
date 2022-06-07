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
    size_t max_entry_nr;

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

template <typename T,
          std::enable_if_t<std::is_trivially_copyable_v<T>, bool> = true>
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
        CHECK(false) << "TODO: " << sizeof(t);
        return kOk;
    }
    RetCode pop(T &t)
    {
        CHECK(false) << "TODO: " << sizeof(t);
        return kOk;
    }

    RetCode debug()
    {
        {
            auto rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(uint64_t));
            auto gaddr = self_finished_gaddr();
            memcpy(rdma_buf.buffer, &client_id_, sizeof(uint64_t));
            auto &handle = get_self_finished_handle();
            auto rc = rdma_adpt_->rdma_write(
                gaddr, rdma_buf.buffer, sizeof(uint64_t), handle);
            CHECK_EQ(rc, kOk);
            rc = rdma_adpt_->commit();
            CHECK_EQ(rc, kOk);
        }

        {
            auto rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(uint64_t));
            auto gaddr = self_finished_gaddr();
            memset(rdma_buf.buffer, 0, sizeof(uint64_t));
            auto &handle = get_self_finished_handle();
            auto rc = rdma_adpt_->rdma_read(
                rdma_buf.buffer, gaddr, sizeof(uint64_t), handle);
            CHECK_EQ(rc, kOk);
            rc = rdma_adpt_->commit();
            CHECK_EQ(rc, kOk);

            uint64_t got = *(uint64_t *) rdma_buf.buffer;
            CHECK_EQ(got, client_id_);
        }

        rdma_adpt_->put_all_rdma_buffer();
        return kOk;
    }

    void read_meta()
    {
        auto &meta_handle = get_meta_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(meta_size());
        auto rc = rdma_adpt_->rdma_read(
            rdma_buf.buffer, meta_gaddr_, meta_size(), meta_handle);
        CHECK_EQ(rc, kOk);
        rc = rdma_adpt_->commit();
        CHECK_EQ(rc, kOk);
        memcpy(&cached_meta_, rdma_buf.buffer, meta_size());

        rdma_adpt_->put_all_rdma_buffer();
        cached_inited_ = true;
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
        if (entry_handle_.valid())
        {
            auto rel_flag = (flag_t) 0;
            rdma_adpt_->relinquish_perm(entry_handle_, 0 /* hint */, rel_flag);
        }
        if (producer.witness_handle_.valid())
        {
            auto rel_flag = (flag_t) 0;
            rdma_adpt_->relinquish_perm(
                producer.witness_handle_, 0 /* hint */, rel_flag);
        }
        if (producer.finished_handle_.valid())
        {
            auto rel_flag = (flag_t) 0;
            rdma_adpt_->relinquish_perm(
                producer.finished_handle_, 0 /* hint */, rel_flag);
        }
        if (consumer.witness_handle_.valid())
        {
            auto rel_flag = (flag_t) 0;
            rdma_adpt_->relinquish_perm(
                consumer.witness_handle_, 0 /* hint */, rel_flag);
        }
        if (consumer.finished_handle_.valid())
        {
            auto rel_flag = (flag_t) 0;
            rdma_adpt_->relinquish_perm(
                consumer.finished_handle_, 0 /* hint */, rel_flag);
        }
    }

private:
    GlobalAddress front_gaddr() const
    {
        return meta_gaddr_ + offsetof(Meta, front);
    }
    GlobalAddress rear_gaddr() const
    {
        return meta_gaddr_ + offsetof(Meta, rear);
    }
    GlobalAddress self_witness_gaddr() const
    {
        DCHECK(cached_inited_);
        auto client_witness = cached_meta_.client_witness;
        return client_witness + sizeof(uint64_t) * client_id_;
    }
    GlobalAddress self_finished_gaddr() const
    {
        DCHECK(cached_inited_);
        auto client_finished = cached_meta_.client_finished;
        return client_finished + sizeof(uint64_t) * client_id_;
    }
    GlobalAddress entry_gaddr(uint64_t entry_idx) const
    {
        DCHECK(cached_inited_);
        auto entry_gaddr = cached_meta_.entries_gaddr;
        auto round_entry_idx = entry_idx % config_.max_entry_nr;
        return entry_gaddr + sizeof(T) * round_entry_idx;
    }
    RemoteMemHandle &get_meta_handle()
    {
        if (unlikely(!meta_handle_.valid()))
        {
            update_meta_handle();
        }
        return meta_handle_;
    }
    RemoteMemHandle &get_self_finished_handle()
    {
        if (unlikely(!producer.finished_handle_.valid()))
        {
            auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                           (flag_t) AcquireRequestFlag::kNoBindPR;
            producer.finished_handle_ =
                rdma_adpt_->acquire_perm(self_finished_gaddr(),
                                         0 /* alloc_hint */,
                                         sizeof(uint64_t),
                                         0ns,
                                         ac_flag);
        }
        return producer.finished_handle_;
    }
    RemoteMemHandle &get_self_witnessed_handle()
    {
        if (unlikely(!producer.witness_handle_.valid()))
        {
            auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                           (flag_t) AcquireRequestFlag::kNoBindPR;
            producer.witness_handle_ =
                rdma_adpt_->acquire_perm(self_witness_gaddr(),
                                         0 /* alloc_hint */,
                                         sizeof(uint64_t),
                                         0ns,
                                         ac_flag);
        }
        return producer.witness_handle_;
    }
    RemoteMemHandle &get_finished_handle()
    {
        if (unlikely(!consumer.finished_handle_.valid()))
        {
            auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                           (flag_t) AcquireRequestFlag::kNoBindPR;
            consumer.finished_handle_ =
                rdma_adpt_->acquire_perm(cached_meta_.client_finished,
                                         0 /* alloc_hint */,
                                         cached_meta_.finished_buf_size(),
                                         0ns,
                                         ac_flag);
        }
        return consumer.finished_handle_;
    }
    RemoteMemHandle &get_witness_handle()
    {
        if (unlikely(!consumer.witness_handle_.valid()))
        {
            auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                           (flag_t) AcquireRequestFlag::kNoBindPR;
            consumer.witness_handle_ =
                rdma_adpt_->acquire_perm(cached_meta_.client_witness,
                                         0 /* alloc_hint */,
                                         cached_meta_.witness_buf_size(),
                                         0ns,
                                         ac_flag);
        }
        return consumer.witness_handle_;
    }
    size_t entry_buf_size() const
    {
        return config_.max_entry_nr * sizeof(T);
    }
    RemoteMemHandle &get_entry_handle(size_t entry_id)
    {
        // TODO: later, should use more complicated alg to handle the entry
        // handle.
        std::ignore = entry_id;
        if (unlikely(!meta_handle_.valid()))
        {
            auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                           (flag_t) AcquireRequestFlag::kNoBindPR;
            meta_handle_ = rdma_adpt_->acquire_perm(entry_gaddr(0),
                                                    0 /* alloc_hint */,
                                                    entry_buf_size(),
                                                    0ns,
                                                    ac_flag);
        }
        DCHECK(meta_handle_.valid());
        return meta_handle_;
    }
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
    RemoteMemHandle entry_handle_;

    struct
    {
        RemoteMemHandle witness_handle_;
        RemoteMemHandle finished_handle_;
    } producer;
    struct
    {
        RemoteMemHandle witness_handle_;
        RemoteMemHandle finished_handle_;
    } consumer;

    bool cached_inited_{false};
    Meta cached_meta_;
};
}  // namespace patronus::cqueue

#endif