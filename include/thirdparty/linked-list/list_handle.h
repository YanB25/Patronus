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
    RetCode debug()
    {
        read_meta();
        return kOk;
    }

    RetCode push_back(const T &t)
    {
        std::ignore = t;
        return kInvalid;
    }
    RetCode pop_front(T &t)
    {
        std::ignore = t;
        return kInvalid;
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

    uint16_t node_id_;
    GlobalAddress meta_gaddr_;
    IRdmaAdaptor::pointer rdma_adpt_;
    HandleConfig config_;

    bool cached_inited_{false};
    Meta cached_meta_;

    RemoteMemHandle meta_handle_;
};
}  // namespace patronus::list

#endif