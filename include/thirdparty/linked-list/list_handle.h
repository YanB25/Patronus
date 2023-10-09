#pragma once
#ifndef PATRONUS_LINKED_LIST_HANDLE_H_
#define PATRONUS_LINKED_LIST_HANDLE_H_
#include "./list_handle_impl.h"

namespace patronus::list
{
template <typename T>
class ListHandle
{
public:
    using pointer = std::shared_ptr<ListHandle<T>>;
    ListHandle(uint16_t node_id,
               GlobalAddress meta_gaddr,
               IRdmaAdaptor::pointer rdma_adpt,
               const ListHandleConfig &config)
        : config_(config),
          impl_(ListHandleImpl<T>::new_instance(
              node_id, meta_gaddr, rdma_adpt, config.list_impl_config))
    {
    }
    static pointer new_instance(uint16_t node_id,
                                GlobalAddress meta_gaddr,
                                IRdmaAdaptor::pointer rdma_adpt,
                                const ListHandleConfig &config)
    {
        return std::make_shared<ListHandle<T>>(
            node_id, meta_gaddr, rdma_adpt, config);
    }
    RetCode push_back(const T &t, util::TraceView trace = util::nulltrace)
    {
        return do_push_back(t, config_.retry_nr, config_.lock_free, trace);
    }
    RetCode lk_push_back(const T &t, util::TraceView trace = util::nulltrace)
    {
        return do_push_back(t, config_.retry_nr, false /* lock free */, trace);
    }
    [[nodiscard]] RetCode lf_push_back(const T &t,
                                       util::TraceView trace = util::nulltrace)
    {
        return do_push_back(t, config_.retry_nr, true /* lock free */, trace);
    }

    [[nodiscard]] RetCode do_push_back(const T &t,
                                       size_t retry_nr,
                                       bool lock_free,
                                       util::TraceView trace = util::nulltrace)
    {
        fill_to_insert_node_if_needed(t);
        DCHECK(to_insert_node_gaddr_.has_value());
        for (size_t i = 0; i < retry_nr; ++i)
        {
            RetCode rc;
            if (lock_free)
            {
                rc = impl_->lf_push_back(to_insert_node_gaddr_.value(), trace);
            }
            else
            {
                rc = impl_->lk_push_back(to_insert_node_gaddr_.value(), trace);
            }
            if (rc == RC::kOk)
            {
                consume_to_insert_node();
                return RC::kOk;
            }
            CHECK_EQ(rc, RC::kRetry);
        }
        return RC::kRetry;
    }

    [[nodiscard]] RetCode pop_front(T *t,
                                    util::TraceView trace = util::nulltrace)
    {
        if (config_.lock_free)
        {
            return lf_pop_front(t, trace);
        }
        else
        {
            return lk_pop_front(t, trace);
        }
    }
    RetCode lk_pop_front(T *t, util::TraceView trace = util::nulltrace)
    {
        return impl_->lk_pop_front(t, trace);
    }
    RetCode lf_pop_front(T *t, util::TraceView trace = util::nulltrace)
    {
        return impl_->lf_pop_front(t, trace);
    }
    ~ListHandle()
    {
        if (to_insert_node_handle_.valid())
        {
            default_relinquish_handle(to_insert_node_handle_);
        }
    }
    void read_meta()
    {
        return impl_->read_meta();
    }
    const Meta &cached_meta() const
    {
        return impl_->cached_meta();
    }
    bool cached_empty() const
    {
        return impl_->cached_empty();
    }
    using Visitor = std::function<bool(const T &)>;
    void lf_visit(const Visitor &visit)
    {
        return impl_->lf_visit(visit);
    }
    auto debug_iterator(TraceView trace = util::nulltrace)
    {
        return impl_->debug_iterator(trace);
    }
    auto debug_meta()
    {
        return impl_->debug_meta();
    }

private:
    void consume_to_insert_node()
    {
        to_insert_node_gaddr_ = std::nullopt;
    }
    void fill_to_insert_node_if_needed(const T &t)
    {
        if (likely(to_insert_node_gaddr_.has_value()))
        {
            return;
        }
        // need to allocate
        // if old handle exists, should relinquish it. Otherwise, resource leaks
        if (unlikely(to_insert_node_handle_.valid()))
        {
            default_relinquish_handle(to_insert_node_handle_);
        }
        to_insert_node_handle_ = impl_->allocate_to_insert_node();
        DCHECK(to_insert_node_handle_.valid());
        to_insert_node_gaddr_ = to_insert_node_handle_.gaddr();

        // NOTE: just prepare it but not commit it.
        impl_
            ->prepare_write_to_node(t,
                                    to_insert_node_gaddr_.value(),
                                    nullgaddr,
                                    to_insert_node_handle_)
            .expect(RC::kOk);
    }
    void default_relinquish_handle(RemoteMemHandle &handle)
    {
        impl_->default_relinquish_handle(handle);
    }
    const ListHandleConfig &config_;
    typename ListHandleImpl<T>::pointer impl_;

    std::optional<GlobalAddress> to_insert_node_gaddr_;
    RemoteMemHandle to_insert_node_handle_;
};

}  // namespace patronus::list

#endif