#include "util/Tracer.h"

#include "Common.h"

namespace util
{
uint64_t TraceView::pin(std::string_view name)
{
    if (unlikely(impl_ != nullptr))
    {
        if constexpr (::config::kReportTraceViewRoute)
        {
            LOG(INFO) << "[traceview] " << impl_->name() << " -> " << name;
        }
        return impl_->pin(std::string(name));
    }
    return 0;
}
std::string TraceView::name() const
{
    if (unlikely(impl_ != nullptr))
    {
        return impl_->name();
    }
    return "";
}
uint64_t TraceView::sum_ns() const
{
    if (unlikely(impl_ != nullptr))
    {
        return impl_->sum_ns();
    }
    return 0;
}
TraceView TraceView::child(std::string_view name)
{
    if (unlikely(impl_ != nullptr))
    {
        return TraceView{impl_->child(std::string(name))};
    }
    return TraceView(nullptr);
}
const std::vector<RetrieveTimerRecord> &TraceView::retrieve_vec() const
{
    return DCHECK_NOTNULL(impl_)->retrieve_vec();
}
const std::map<std::string, uint64_t> &TraceView::retrieve_map() const
{
    return DCHECK_NOTNULL(impl_)->retrieve_map();
}
bool TraceView::enabled() const
{
    return impl_ != nullptr;
}
// std::vector<TraceRecord> TraceView::get_flat_records(size_t depth_limit)
// const
// {
//     if (unlikely(impl_ != nullptr))
//     {
//         return impl_->get_flat_records(depth_limit);
//     }
//     return {};
// }

void TraceView::set(const std::string &key, const std::string &value)
{
    if (unlikely(impl_ != nullptr))
    {
        return impl_->set(key, value);
    }
}

std::string TraceView::get(const std::string &key) const
{
    if (unlikely(impl_ != nullptr))
    {
        return impl_->get(key);
    }
    return {};
}
std::map<std::string, std::string> TraceView::kv() const
{
    if (unlikely(impl_ != nullptr))
    {
        return impl_->kv();
    }
    return {};
}

}  // namespace util