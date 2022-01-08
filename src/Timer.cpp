#include "Timer.h"

#include <glog/logging.h>

#include "boost/format.hpp"

ContTimer<true>::ContTimer(const std::string &name, const std::string &step)
{
    init(name, step);
}
ContTimer<true>::ContTimer()
{
}

void ContTimer<true>::init(const std::string &name,
                           const std::string &first_step)
{
    clear();
    
    name_ = name;
    step_ = first_step;
    start_ = pin_ = std::chrono::steady_clock::now();
    inited_ = true;
}

void ContTimer<true>::pin(const std::string this_step)
{
    DCHECK(inited_);
    auto now = std::chrono::steady_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - pin_)
                  .count();
    std::string event = step_ + " => " + this_step;
    event_ns_[event] = ns;

    pin_ = now;
    step_ = this_step;
}
void ContTimer<true>::report(std::ostream &os) const
{
    DCHECK(inited_);
    auto now = std::chrono::steady_clock::now();
    auto total_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_)
            .count();
    auto fmt = boost::format("[%1%]: *summary* takes %2% ns ( %3% ms)\n") %
               name_ % total_ns % (total_ns / 1e6);
    auto str = boost::str(fmt);
    for (const auto &[event, ns] : event_ns_)
    {
        fmt = boost::format("%1% %% [%2%] takes %3% ns (%4% ms)\n") %
              (100.0f * ns / total_ns) % event % ns % (ns / 1e6);
        str += boost::str(fmt);
    }
    os << str << std::endl;
}

ContTimer<true>::~ContTimer()
{
    // pin("~Dtor()");
    // report();
}