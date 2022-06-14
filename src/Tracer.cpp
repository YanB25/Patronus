#include "util/Tracer.h"

#include "Common.h"

namespace util
{
uint64_t TraceView::pin(const std::string &name)
{
    if (unlikely(impl_ != nullptr))
    {
        if constexpr (::config::kReportTraceViewRoute)
        {
            LOG(INFO) << "[traceview] " << impl_->name() << " -> " << name;
        }
        return impl_->pin(name);
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
TraceView TraceView::child(const std::string &name)
{
    pin("REACH " + name);
    if (unlikely(impl_ != nullptr))
    {
        return TraceView{impl_->child_context(name)};
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
std::vector<TraceRecord> TraceView::get_flat_records(size_t depth_limit) const
{
    if (unlikely(impl_ != nullptr))
    {
        return impl_->get_flat_records(depth_limit);
    }
    return {};
}

}  // namespace util