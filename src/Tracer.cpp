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

}  // namespace util