#pragma once
#ifndef PATRONUS_LINKED_LIST_HANDLE_H_
#define PATRONUS_LINKED_LIST_HANDLE_H_

#include "util/RetCode.h"

namespace patronus::list
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
class ListHandle
{
public:
    using pointer = std::shared_ptr<ListHandle<T>>;
    ListHandle(uint16_t node_id,
               GlobalAddress meta,
               IRdmaAdaptor::pointer rdma_adpt,
               const HandleConfig &config)
        : node_id_(node_id),
          meta_gaddr_(meta),
          rdma_adpt_(rdma_adpt),
          config_(config)
    {
    }
    static pointer new_instance(uint16_t node_id,
                                GlobalAddress meta,
                                IRdmaAdaptor::pointer rdma_adpt,
                                const HandleConfig &conf)
    {
        return std::make_shared<ListHandle<T>>(node_id, meta, rdma_adpt, conf);
    }
    RetCode func_debug()
    {
        read_meta();
        return kOk;
    }

    RetCode lf_push_back(const T &t);
    RetCode lf_pop_front(const T &t);
    RetCode lk_push_back(const T &t)
    {
        std::ignore = t;
        auto rc = lock_push();
        if (rc != kOk)
        {
            DCHECK_EQ(rc, kRetry);
            return rc;
        }
        unlock_push();
        return kOk;
    }
    RetCode lk_pop_front(const T &t)
    {
        std::ignore = t;
        auto rc = lock_pop();
        if (rc != kOk)
        {
            DCHECK_EQ(rc, kRetry);
            return rc;
        }
        unlock_pop();
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

        rc = rdma_adpt_->put_all_rdma_buffer();
        CHECK_EQ(rc, kOk);
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

    ~ListHandle()
    {
        if (meta_handle_.valid())
        {
            auto rel_flag = (flag_t) 0;
            rdma_adpt_->relinquish_perm(meta_handle_, 0 /* hint */, rel_flag);
        }
    }

private:
    RemoteMemHandle &get_meta_handle()
    {
        if (unlikely(!meta_handle_.valid()))
        {
            update_meta_handle();
        }
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
    GlobalAddress meta_gaddr() const
    {
        return meta_gaddr_;
    }
    RetCode lock_push()
    {
        auto &push_lock_handle = get_push_lock_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(uint64_t));
        auto rc = rdma_adpt_->rdma_cas(
            push_lock_gaddr(), 0, 1, rdma_buf.buffer, push_lock_handle);
        CHECK_EQ(rc, kOk);
        uint64_t got = *(uint64_t *) rdma_buf.buffer;
        DCHECK(got == 0 || got == 1)
            << "got invalid lock value: " << got << ". Not 0 or 1.";
        rc = rdma_adpt_->commit();
        CHECK_EQ(rc, kOk);
        rc = rdma_adpt_->put_all_rdma_buffer();
        CHECK_EQ(rc, kOk);

        if (got == 0)
        {
            return kOk;
        }
        else
        {
            return kRetry;
        }
    }
    void unlock_push()
    {
        auto &push_lock_handle = get_push_lock_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(uint64_t));
        *((uint64_t *) rdma_buf.buffer) = 0;

        auto rc = rdma_adpt_->rdma_write(push_lock_gaddr(),
                                         rdma_buf.buffer,
                                         sizeof(uint64_t),
                                         push_lock_handle);
        CHECK_EQ(rc, kOk);
        rc = rdma_adpt_->commit();
        CHECK_EQ(rc, kOk);
        rc = rdma_adpt_->put_all_rdma_buffer();
        CHECK_EQ(rc, kOk);
    }
    RetCode lock_pop()
    {
        auto &pop_lock_handle = get_pop_lock_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(uint64_t));
        auto rc = rdma_adpt_->rdma_cas(
            pop_lock_gaddr(), 0, 1, rdma_buf.buffer, pop_lock_handle);
        CHECK_EQ(rc, kOk);
        uint64_t got = *(uint64_t *) rdma_buf.buffer;
        DCHECK(got == 0 || got == 1)
            << "got invalid lock value: " << got << ". Not 0 or 1.";
        rc = rdma_adpt_->commit();
        CHECK_EQ(rc, kOk);
        rc = rdma_adpt_->put_all_rdma_buffer();
        CHECK_EQ(rc, kOk);

        if (got == 0)
        {
            return kOk;
        }
        else
        {
            return kRetry;
        }
    }
    void unlock_pop()
    {
        auto &pop_lock_handle = get_pop_lock_handle();
        auto rdma_buf = rdma_adpt_->get_rdma_buffer(sizeof(uint64_t));
        *((uint64_t *) rdma_buf.buffer) = 0;

        if constexpr (debug())
        {
            auto rc = rdma_adpt_->rdma_read(rdma_buf.buffer,
                                            pop_lock_gaddr(),
                                            sizeof(uint64_t),
                                            pop_lock_handle);
            // CHECK_EQ(rc, kOk);
            // auto rc = rdma_adpt_->rdma_read(pop_lock_gaddr(),
            //                                 rdma_buf.buffer,
            //                                 sizeof(uint64_t),
            //                                 pop_lock_handle);
            // CHECK_EQ(rc, kOk);
            // rc = rdma_adpt_->commit();
            // TODO: start here
        }

        auto rc = rdma_adpt_->rdma_write(pop_lock_gaddr(),
                                         rdma_buf.buffer,
                                         sizeof(uint64_t),
                                         pop_lock_handle);
        CHECK_EQ(rc, kOk);
        rc = rdma_adpt_->commit();
        CHECK_EQ(rc, kOk);
        rc = rdma_adpt_->put_all_rdma_buffer();
        CHECK_EQ(rc, kOk);
    }

    GlobalAddress push_lock_gaddr() const
    {
        return meta_gaddr() + offsetof(Meta, push_lock);
    }
    GlobalAddress pop_lock_gaddr() const
    {
        return meta_gaddr() + offsetof(Meta, pop_lock);
    }
    RemoteMemHandle &get_push_lock_handle()
    {
        return get_handle_impl(0, push_lock_gaddr(), sizeof(uint64_t));
    }
    RemoteMemHandle &get_pop_lock_handle()
    {
        return get_handle_impl(0, pop_lock_gaddr(), sizeof(uint64_t));
    }
    RemoteMemHandle &get_handle_impl(uint64_t id,
                                     GlobalAddress gaddr,
                                     size_t size)
    {
        auto &ret = handles_[id];
        if (unlikely(!ret.valid()))
        {
            auto ac_flag = (flag_t) AcquireRequestFlag::kNoGc |
                           (flag_t) AcquireRequestFlag::kNoBindPR;
            ret = rdma_adpt_->acquire_perm(gaddr, 0, size, 0ns, ac_flag);
        }
        return ret;
    }

    uint16_t node_id_;
    GlobalAddress meta_gaddr_;
    IRdmaAdaptor::pointer rdma_adpt_;
    HandleConfig config_;

    bool cached_inited_{false};
    Meta cached_meta_;

    std::unordered_map<uint64_t, RemoteMemHandle> handles_;

    RemoteMemHandle meta_handle_;
};
}  // namespace patronus::list

#endif