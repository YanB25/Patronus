#include "util/Tracer.h"

namespace util
{
uint64_t TraceView::pin(const std::string &name)
{
    if (unlikely(impl_ != nullptr))
    {
        return impl_->pin(name);
    }
    return 0;
}
std::vector<RetrieveTimerRecord> TraceView::retrieve_vec() const
{
    return DCHECK_NOTNULL(impl_)->retrieve_vec();
}
std::map<std::string, uint64_t> TraceView::retrieve_map() const
{
    return DCHECK_NOTNULL(impl_)->retrieve_map();
}
bool TraceView::enabled() const
{
    return impl_ != nullptr;
}

}  // namespace util